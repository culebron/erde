#!/usr/bin/python3.6

from .base import BaseDriver, BaseReader, BaseWriter
from . import FORMATS
from csv import field_size_limit
from shapely.wkt import loads
import shapely.errors
import pandas as pd
import geopandas as gpd
import io
import os


class CsvReader(BaseReader):
	def __init__(self, source, geometry_filter=None, chunk_size:int=10_000, sync:bool=False, skip=0, sep=',', pbar=True):
		if not os.path.exists(source):  # immediately raise error to avoid crashing much later
			raise FileNotFoundError(f'file {source} does not exist')

		self.sep = sep  # needed in _read_schema
		super().__init__(source, geometry_filter, chunk_size or 10_000, sync=sync, pbar=pbar)

		# reading schema, should be like fiona schema
		df = pd.read_csv(self.source, nrows=1, sep=self.sep, engine='c')

		self.fieldnames = list(df)
		properties = {k: df[k].dtype for k in self.fieldnames}
		self.schema = {'properties': properties}

		geom_col = None
		if 'geometry' in properties:
			geom_col = 'geometry'
		elif 'WKT' in properties:
			geom_col = 'WKT'

		if geom_col:
			properties.pop(geom_col)
			self.schema['geometry'] = loads(df[geom_col][0]).geom_type

		with open(self.source) as f:
			self.total_rows = sum(1 for i in f)

	def _read_sync(self):
		for geometry in self.geometry_filter_pbar:
			with open(self.source) as f:
				self.reader = pd.read_csv(f, chunksize=self.chunk_size, sep=self.sep, engine='c')
				self._stopped_iteration = False

				with self._pbar(desc=f'rows in {self.source}', total=self.total_rows) as reader_bar:
					try:
						while True:
							data = self.reader.get_chunk()
							data.index = self._range_index(data)

							if self.fieldnames is None: # field names not available before read # и пофиг пока
								self.fieldnames = list(data) # field names as in file, not in df :(

							if 'geometry' not in data and 'WKT' not in data:
								yield data
								reader_bar.update(len(data))
								continue

							text_geometry = 'geometry' if 'geometry' in data else 'WKT'
							try:
								geom = data[text_geometry].apply(loads)
							except shapely.errors.WKTReadingError:
								# ignore bad WKT (might be not wkt at all)
								yield data
								reader_bar.update(len(data))
								continue

							data.pop(text_geometry)
							data['geometry'] = geom

							yield gpd.GeoDataFrame(data, crs=self.crs)
							reader_bar.update(len(data))
					except StopIteration:
						pass


class CsvWriter(BaseWriter):
	def __init__(self, target, sync:bool=False, **kwargs):
		super().__init__(target, sync, **kwargs)
		field_size_limit(10000000)
		self._file_handler = None
		self.header = kwargs.get('header', True)

	def _open_handler(self, df=None):
		if self._handler is not None:
			return

		if df is None:
			df = pd.DataFrame()
		from csv import DictWriter
		self.fieldnames = list(df)
		if isinstance(self.target, str):
			self._file_handler = open(self.target, 'w')
		elif isinstance(self.target, io.TextIOWrapper):
			self._file_handler = self.target

		self._handler = DictWriter(self._file_handler, fieldnames=self.fieldnames, extrasaction='ignore')
		if self.header:
			self._handler.writeheader()

	def _write_sync(self, df):
		self._open_handler(df)
		if 'geometry' in df:
			df['geometry'] = df['geometry'].apply(shapely.wkt.dumps)

		self._handler.writerows(df.to_dict(orient='records'))
		# df.to_csv(self._handler, header=False, index=False)
		self._file_handler.flush()

	def _close_handler(self):
		self._open_handler()
		self._file_handler.close()

	def _cancel(self):
		if self._handler is not None:
			self._close_handler()
			os.unlink(self.target)


class CsvDriver(BaseDriver):
	source_extension = None
	source_regexp = FORMATS['csv']
	reader = CsvReader
	writer = CsvWriter


driver = CsvDriver