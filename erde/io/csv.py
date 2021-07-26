from . import check_path_exists
from .base import BaseDriver, BaseReader, BaseWriter
from csv import field_size_limit
from shapely.wkt import loads
import geopandas as gpd
import io
import os
import pandas as pd
import shapely.errors


PATH_REGEXP = r'^.*\.(csv|txt)$'

class CsvReader(BaseReader):
	source_regexp = PATH_REGEXP

	def __init__(self, source, geometry_filter=None, chunk_size:int=10_000, sync:bool=False, skip=0, sep=',', pbar=True):
		check_path_exists(source)
		self.sep = sep  # needed in _read_schema
		super().__init__(source, geometry_filter, chunk_size or 10_000, sync=sync, pbar=pbar)

		# reading schema, should be like fiona schema
		df = pd.read_csv(self.source, nrows=1, sep=self.sep, engine='c')

		self.fieldnames = list(df)
		properties = {k: df[k].dtype for k in self.fieldnames}
		self.schema = {'properties': properties}

		self.geom_col = None
		if 'geometry' in properties:
			self.geom_col = 'geometry'
		elif 'WKT' in properties:
			self.geom_col = 'WKT'

		if self.geom_col:
			properties.pop(self.geom_col)
			self.schema['geometry'] = loads(df[self.geom_col][0]).geom_type

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

							if self.geom_col is None:
								yield data
								reader_bar.update(len(data))
								continue

							try:
								geom = data[self.geom_col].apply(loads)
							except shapely.errors.WKTReadingError:
								# ignore bad WKT (might be not wkt at all)
								yield data
								reader_bar.update(len(data))
								continue

							data.pop(self.geom_col)
							data['geometry'] = geom

							yield gpd.GeoDataFrame(data, crs=self.crs)
							reader_bar.update(len(data))
					except StopIteration:
						pass


class CsvWriter(BaseWriter):
	target_regexp = PATH_REGEXP

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
	path_regexp = PATH_REGEXP
	reader = CsvReader
	writer = CsvWriter

	@classmethod
	def read_df(cls, path, path_match, crs=None, geometry_columns=('geometry', 'WKT'), *args, **kwargs):
		from erde.io import _try_gdf
		source_df = pd.read_csv(path, **kwargs)
		return _try_gdf(source_df, geometry_columns, crs)

	@classmethod
	def write_df(cls, df, path, path_match, *args, **kwargs):
		if os.path.exists(path):
			os.unlink(path)

		df.to_csv(path, index=False)

driver = CsvDriver
