from contextlib import contextmanager
from erde.io.base import BaseReader, BaseWriter
from shapely.geometry import Point, LineString, Polygon
from time import sleep
from unittest import mock
from unittest.mock import patch, Mock
import geopandas as gpd
import pytest
import sys
import traceback as tb


d = 'tests/io/data/'
polygons = gpd.read_file(d + 'polygons.gpkg', driver='GPKG')

def test_base_reader():

	# (self, source, geometry_filter=None, chunk_size: int = 10_000, sync: bool = False, pbar: bool = True, queue_size=10, **kwargs)

	df = polygons.copy()
	s = 10
	dfs = [df[i:i + s] for i in range(0, len(df), s)]
	gen_obj = (i for i in df.geometry.values)


	# calling BaseReader, with path to file as geometry_filter.
	with patch('erde.io.read_stream', return_value=dfs) as mock:
		BaseReader('mock source', 'path_to_geofile.gpkg')

	mock.assert_called_with('path_to_geofile.gpkg', chunk_size=1, pbar=False, sync=True)

	# cover the case where geometry_filter is None
	with patch('erde.io.read_stream', return_value=dfs) as mock:
		br = BaseReader('another mock')

	mock.assert_not_called()

	# now, br is used as a filter for other readers
	br.total_rows = 10

	# trying other objects as geometry_filter:
	# single geometries; another BaseReader; generator; geoseries; geodataframe; list of geometries
	for obj in [Point(1, 2), LineString([[1, 2], [3, 4]]), Polygon([[0, 1], [1, 1], [1, 0], [0, 0], [0, 1]], []), br, gen_obj, df.geometry, df, df.geometry.to_list()]:
		BaseReader('mock source', geometry_filter=obj)

	# geometry_filter can't be anything other than those above
	with pytest.raises(TypeError):
		BaseReader('mock source', Mock)

def test_raise_notimplemented():
	# basereader must raise NotImplementedError when we try reading chunks of data
	with BaseReader(d + 'polygons.gpkg', chunk_size=3, sync=False) as rd:
		itr = iter(rd)
		with pytest.raises(NotImplementedError):
			next(itr)

		with pytest.raises(NotImplementedError):
			itr._read_sync()

		with pytest.raises(NotImplementedError):
			itr.stats('test')

	with BaseWriter('/tmp/not-implemented-writer.gpkg', sync=False) as wr:
		for method in ('_write_sync', '_open_handler', '_cancel', '_close_handler'):
			with pytest.raises(NotImplementedError):
				getattr(wr, method)()


def test_parallel_coverage():
	# just coverage
	for s in (False, True):
		with BaseReader(d + 'polygons.gpkg', chunk_size=3, sync=s) as rd:
			pass

	with pytest.raises(RuntimeError):
		with BaseReader(d + 'polygons.gpkg', chunk_size=3) as rd:
			# call __exit__ with runtime error
			rd.out_q.put('a data object to keep the queue busy')
			rd.out_q.put('another data object')
			rd.background_process.start()
			sleep(1)
			raise RuntimeError

	assert rd.out_q.empty()
	assert not rd.background_process.is_alive()


def test_read_parallel():
	# 1. should yield what was in the q and exit normally
	# 2. when we raise emergency stop, it should break and raise the error from err_q

	@contextmanager
	def _setup():
		with BaseReader(d + 'polygons.gpkg', chunk_size=3) as br:
			# entered context but did not start process yet
			assert not br.background_process.is_alive()

			br.out_q.put(df)
			br.out_q.put(df)
			br.out_q.put(None)

			yield br



	df = polygons.copy()

	with _setup() as br:
		ret_data = list(br._read_parallel())
		assert len(ret_data) == 2

	with _setup() as br:
		# generator will reach yield statement before we get any value, and will yield exactly one df
		gen = br._read_parallel()

		# pretending we had an exception
		e = RuntimeError('arbitrary exception')
		br.emergency_stop.value = True
		br.err_q.put((e.__class__, e.args, None))

		# the generator will run till the next yield, then our code is supposed to get the dataframe
		with pytest.raises(RuntimeError):
			df = next(gen)

def make_chunks():
	df = polygons.copy()
	return [df[i:i+2] for i in range(0, 6, 2)]

def test_read_worker():
	# _worker should put all dfs yielded by self._read_sync() into que
	from unittest import mock

	@contextmanager
	def _setup():
		from queue import Queue
		qq = Queue()
		with BaseReader(d + 'polygons.gpkg', chunk_size=3) as br:
			with mock.patch.object(br, '_read_sync', return_value=dfgen()) as rs, mock.patch.object(br, 'out_q', qq):
				yield br, rs

	orig_data = make_chunks()
	def dfgen():
		for i in orig_data:
			yield i

	# just a normal read, make sure it's called once
	with _setup() as (br, rs):
		br._worker()
		rs.assert_called_once()
		out_data = []
		sleep(0)
		while not br.out_q.empty():
			out_data.append(br.out_q.get())
			sleep(0) # otherwise que won't work

	assert len(out_data) == len(orig_data) + 1

	for a, b in zip(out_data, orig_data):
		assert a.equals(b)

	assert out_data[-1] is None

	with _setup() as (br, rs):
		e = RuntimeError('arbitrary exception')
		br.emergency_stop.value = True
		br.err_q.put((e.__class__, e.args, None))
		br._worker()
		sleep(0)
		assert br.out_q.empty()


out_data = []
def _pretend_to_write(self, df):
	# this function is called from worker and does nothing
	out_data.append(df)


runtime_error_arg = "let's fail"
def _pretend_to_crash(self, df):
	# _worker should process the exception
	raise RuntimeError(runtime_error_arg)


def _pretend_keyboard_interrupt(self, df):
	raise KeyboardInterrupt()


from queue import Queue
def _setup_writer_q():
	q = Queue(maxsize=100)
	in_data = make_chunks()

	for df in in_data:
		q.put(df)

	q.put(None)
	return in_data, q


@contextmanager
def patch_base_writer(**kwargs):
	with mock.patch.multiple(BaseWriter, _close_handler=mock.MagicMock(return_value=None), _cancel=mock.MagicMock(return_value=None), **kwargs):
		with BaseWriter('/tmp/test.gpkg', sync=True) as bw:
			in_data, in_q = _setup_writer_q()
			bw.in_q = in_q
			bw.err_q = Queue()
			bw._worker()

		yield bw, in_data


def test_write_worker_ok():
	with patch_base_writer(_write_sync=_pretend_to_write) as (bw, in_data):
		# here we can't test that _close_handler is called only once -- it's been called twice, by _worker and __exit__
		# because we assume sync mode (_worker not launched and __exit__ does cleanup), but then call _worker anyway
		BaseWriter._close_handler.assert_called()
		BaseWriter._cancel.assert_not_called()

	for i, j in zip(in_data, out_data):
		assert i.equals(j)


def test_write_worker_crash():
	with patch_base_writer(_write_sync=_pretend_to_crash) as (bw, in_data):
		# _cancel is called twice, by _worker and __exit__, because we pretended to run in sync mode, but then called _worker anyway, which calls _cancel.
		# for the same reason, _close_handler is called by __exit__, not by _worker (who caught the exception)
		BaseWriter._close_handler.assert_called_once()
		BaseWriter._cancel.assert_called()


def test_write_worker_keyboard_interrupt():
	with patch_base_writer(_write_sync=_pretend_keyboard_interrupt) as (bw, in_data):
		BaseWriter._close_handler.assert_called_once()
		BaseWriter._cancel.assert_called()

def test_write_worker_stops():
	with patch_base_writer(_write_sync=_pretend_to_write) as (bw, in_data):
		bw(polygons)
		bw.emergency_stop.value = True
		bw.background_process = Mock(join=Mock())
		msg = '__call__ should raise this'
		e = RuntimeError(msg)

		bw.err_q.put((e.__class__, e.args, ''.join(tb.format_tb(sys.exc_info()[2]))))
		bw._sync = False # pretending we're in async mode
		with pytest.raises(RuntimeError):
			bw(polygons)

		bw.background_process.join.assert_called_once()

