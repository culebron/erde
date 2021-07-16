"""
Base driver for parallel, streaming io. Does not implement actual reading/writing to files/databases.
"""

from erde import ESYNC, dprint
from multiprocessing import Process, Value, Queue
from shapely.geometry.base import BaseGeometry
from time import sleep
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
import re
import types
import ctypes
import sys


class BaseReader:
	"""Base class for chunk-streaming format drivers.

	These drivers allow to process large geospatial data files in chunks, but still as dataframes.

	BaseReader implements dispatching. Subclasses need to define the format specific actions (read columns, lines, or statistics).

	To run in another process, readers must be used as context managers:

		with GpkgReader(path_to_file) as reader:
			for gdf in reader:
				print(gdf)

	Otherwise they're run in sync mode:

		for gdf in GpkgReader(path_to_file):
			print(gdf)

	"""

	def __init__(self, source, geometry_filter=None, chunk_size: int = 10_000, sync: bool = False, pbar: bool = True, queue_size=10, **kwargs):
		self.source = source
		assert chunk_size is None or chunk_size > 0, "chunk_size must be positive int or None"
		self.chunk_size = chunk_size
		self.queue_size = queue_size

		self._sync = sync or ESYNC
		self.pbar = pbar
		self.index_start = 0  # dataframes should have different indice, otherwise they'll be merged incorrectly
		self.crs = None
		self.total = None
		self.kwargs = kwargs

		self._reader = None
		self.err_q = None
		self.out_q = None
		self.background_process = None
		self._entered_context = False
		self.emergency_stop = Value(ctypes.c_bool, False)

		geo_filter_total = None
		if geometry_filter is None or isinstance(geometry_filter, BaseGeometry):
			g_ = [geometry_filter]
		elif isinstance(geometry_filter, str):
			from erde.io import read_stream
			geo_filter_stream = read_stream(geometry_filter, chunk_size=1, pbar=False, sync=True)
			geo_filter_total = len(geo_filter_stream)
			g_ = (df['geometry'].values[0] for df in geo_filter_stream)
		elif isinstance(geometry_filter, BaseReader):
			geo_filter_total = len(geometry_filter)
			g_ = (g for df in geometry_filter for g in df['geometry'].values)
		elif isinstance(geometry_filter, types.GeneratorType):
			g_ = (g for df in geometry_filter for g in df['geometry'].values)
		elif isinstance(geometry_filter, gpd.GeoSeries):
			g_ = geometry_filter.tolist()
			geo_filter_total = len(g_)
		elif isinstance(geometry_filter, gpd.GeoDataFrame):
			g_ = geometry_filter['geometry'].tolist()
			geo_filter_total = len(g_)
		elif isinstance(geometry_filter, list):
			g_ = geometry_filter
			geo_filter_total = len(g_)
		else:
			raise TypeError('geometry filter can be: None, shapely.geometry.BaseGeometry, generator, DfReader')

		self.geometry_filter = g_
		self.geometry_filter_total = geo_filter_total

	# main process
	@property
	def geometry_filter_pbar(self):
		disable = ((not self.pbar) or self.geometry_filter_total is None or self.geometry_filter_total < 2)
		return self._pbar(self.geometry_filter, total=self.geometry_filter_total, desc=f'{self.source} filters', disable=disable)

	def _pbar(self, iterable=None, disable=None, **kwargs):
		if disable is None: disable = not self.pbar
		kwargs.update({'disable': disable, 'unit_scale': 1})
		return tqdm(iterable, **kwargs)

	# main process
	def __enter__(self):
		# multiprocessing features can be used only if the object is used as contex manager. Otherwise there's no safe shutdown mechanism.
		self.err_q = Queue(maxsize=2)
		self.out_q = Queue(maxsize=self.queue_size)
		self.background_process = Process(target=self._worker, name=f'reader of {self.source}')
		self._entered_context = True
		return self

	def __exit__(self, exc_type, exc_value, exc_trace):
		dprint('base reader __EXIT__')
		if self._sync:
			self._close_handler()
		else:
			dprint('base reader: exiting async writer')
			if exc_type is not None:
				dprint('base reader exit: sending emergency stop')
				sleep(0)
				self.emergency_stop.value = True  # both sync/async
				while not self.out_q.empty():
					sleep(0)
					self.out_q.get()
					sleep(0)
			else:
				dprint('base reader: normal exit')

			self.background_process.join()
			sleep(0)
			dprint('base reader: exit background process joined')
		dprint('base reader: exit done')

	# main process
	def __iter__(self):
		return self

	def __len__(self):
		return self.total_rows

	# main process
	def __next__(self):
		if self._reader is None:
			if self._sync or not self._entered_context:  # if this object is not used as context manager, run in sync mode
				self._reader = self._read_sync()
			else:
				self.background_process.start()  # start the process only here, in case nothing is read from the stream
				self._reader = self._read_parallel()

		return next(self._reader)

	# background process
	def _worker(self):
		try:
			for i, gdf in enumerate(self._read_sync()):
				dprint('reader worker: reading done')
				if self.emergency_stop.value:
					dprint('reader worker: emergency')
					break
				dprint('reader worker: putting to q')
				sleep(0)
				self.out_q.put(gdf)
				sleep(0)  # required to yield to queue's thread!!!
				dprint('reader worker: q done')
				if self.emergency_stop.value:
					dprint('reader worker: emergency')
					break
			else:
				self.out_q.put(None)

		except Exception as e:
			dprint('reader worker: exception')
			self.emergency_stop.value = True
			import traceback as tb
			self.err_q.put((e.__class__, e.args, ''.join(tb.format_tb(sys.exc_info()[2]))))

		dprint('reader worker: ending')
		sleep(0)

	# main process
	def _read_parallel(self):
		while True:
			try:
				if self.emergency_stop.value: break
				sleep(0)
				item = self.out_q.get()
				sleep(0)
				if self.emergency_stop.value or item is None: break
				yield item
				if self.emergency_stop.value: break
			except Exception:
				self.emergency_stop.value = True

		if self.emergency_stop.value:
			t, e, tb = self.err_q.get()
			print('exception in reading process, printing its traceback', file=sys.stderr)
			raise t(*e)

	# background process
	def _read_sync(self):
		raise NotImplementedError

	# background process
	def _range_index(self, rows):
		old_start = self.index_start
		self.index_start += len(rows)
		return pd.RangeIndex(old_start, self.index_start)

	def stats(self, column):
		raise NotImplementedError


class BaseWriter:
	def __init__(self, target, sync: bool = False, **kwargs):
		self.target = target
		self._sync = sync
		self._handler = None
		self.kwargs = kwargs
		self.emergency_stop = Value(ctypes.c_bool, False)  # used both in sync/async

	# main process
	def __enter__(self):
		dprint('enter writer')

		if self._sync:
			return self

		self.in_q = Queue(maxsize=3)
		self.err_q = Queue(maxsize=2)
		self.background_process = Process(target=self._worker, name=f'writer to {self.target}')
		self.background_process.start()
		dprint('enter: done')
		return self

	# main process
	def __exit__(self, exc_type, exc_value, exc_trace):
		dprint('base writer __exit__')
		# check emergency status first
		if self._sync:
			if exc_type is None:
				self._close_handler()
			else:
				self._cancel()

		else:
			dprint('base writer: exiting async writer')
			if isinstance(exc_value, KeyboardInterrupt):
				pass
			elif exc_type is not None:
				dprint('base writer exit: exception')
				self.emergency_stop.value = True  # both sync/async
				dprint('base writer exit: sending None to q')
				if self.in_q.empty():
					self.in_q.put(None)
				dprint('base writer exit: None to q sent, joining background process')
			else:
				dprint('base writer exit: normal exit')
				self.in_q.put(None)
				dprint('base writer exit: joining background process')


			self.background_process.join()
			self.in_q.close()
			self.err_q.close()
			sleep(0)
		dprint('base writer: exit done')

	# main process
	def __call__(self, df):
		if self.emergency_stop.value and not self._sync:
			dprint('main __call__: emergency stop in background process')
			t, v, tb = self.err_q.get()
			print('exception in writing process', file=sys.stderr)
			self.background_process.join()
			raise t(*v)

		if self._sync:
			self._write_sync(df)
		else:
			self.in_q.put(df)
			sleep(0)  # unlock the queue thread

	# background process
	def _worker(self):
		try:
			while True:
				if self.emergency_stop.value:
					dprint('base writer worker: emergency')
					break
				dprint('base writer worker: reading from q')
				df = self.in_q.get()
				dprint('base writer worker: q read')
				if self.emergency_stop.value:
					dprint('base writer worker: emergency')
					break
				if df is None: break
				dprint('base writer worker: item from q not empty')
				self._write_sync(df)
				dprint('base writer worker: item written')
				if self.emergency_stop.value:
					dprint('base writer worker: emergency')
					break

			if self.emergency_stop.value:
				self._cancel()
			else:
				self._close_handler()
			dprint('base writer worker: handler closed')

		except KeyboardInterrupt:
			pass
		except Exception as e:  # something crashed
			self.emergency_stop.value = True
			import traceback as tb
			self.err_q.put((e.__class__, e.args, ''.join(tb.format_tb(sys.exc_info()[2]))))
			self._cancel()
		dprint('base writer worker: exiting (or trying)')
		sleep(0)  # unlock some resource

	# background process
	def _write_sync(self):
		raise NotImplementedError

	# background process
	def _open_handler(self):
		raise NotImplementedError

	# background process
	def _cancel(self):
		raise NotImplementedError

	# background process
	def _close_handler(self):
		raise NotImplementedError


class BaseDriver:
	reader = BaseReader
	writer = BaseWriter

	data_type = (pd.DataFrame, gpd.GeoDataFrame)
	source_extension = None
	source_regexp = None

	@classmethod
	def can_open(cls, source):
		if cls.source_extension is not None:
			return source.endswith('.' + cls.source_extension)

		if cls.source_regexp is not None:
			return re.match(cls.source_regexp, source)

		return False

	@classmethod
	def open_read(cls, *args, **kwargs):
		return cls.reader(*args, **kwargs)

	@classmethod
	def open_write(cls, *args, **kwargs):
		return cls.writer(*args, **kwargs)
