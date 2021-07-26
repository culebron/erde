from .base import BaseDriver, BaseReader, BaseWriter
from collections import OrderedDict
from erde import dprint
from time import sleep
import geopandas as gpd
import os
import re
import fiona

FIONA_DRIVER = 'GPKG'
PATH_REGEXP = r'^(?P<file_path>(?:.*/)?(?P<file_name>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>gpkg)))(?:\:(?P<layer_name>[a-z0-9_-]+))?$'

class GpkgReader(BaseReader):
	fiona_driver = FIONA_DRIVER
	source_regexp = PATH_REGEXP

	def __init__(self, source, geometry_filter=None, chunk_size: int = 10_000, sync: bool = False, pbar: bool = True, **kwargs):
		super().__init__(source, geometry_filter, chunk_size, sync, pbar, **kwargs)

		g = self.source_match.groupdict()
		self.source = g['file_path']
		self.layername = g['layer_name']

		import fiona
		if self.layername is None:
			try:
				layers = fiona.listlayers(self.source)
			except ValueError:
				raise RuntimeError(f'Fiona driver can\'t read layers from file {self.source}')

			if len(layers) == 1:
				self.layername = layers[0]

			else:
				layername = g['file_own_name']
				if layername not in layers:
					raise RuntimeError(f"Can\'t detect default layer in {self.source}. Layers available are: {', '.join(layers)}")

				self.layername = layername

		self._read_schema()
		self._handler = None

	def _read_schema(self):
		import fiona
		import pyproj

		# read schema
		tmp_handler = fiona.open(self.source, layer=self.layername, driver=self.fiona_driver, **self.kwargs)
		self.schema = tmp_handler.schema
		self.crs = tmp_handler.crs_wkt
		if self.crs is not None and self.crs != '':
			self.crs = pyproj.crs.CRS(self.crs)
		self.fieldnames = list(self.schema['properties']) + ['geometry']
		self.total_rows = len(tmp_handler)
		self.bounds = tmp_handler.bounds

	def _read_sync(self):
		# works in background process. memory not shared with the main
		import fiona
		from shapely.geometry import shape

		# чтение файла через fiona быстрее, чем gpd.read_file с отступом
		# потому что на больших файлах на каждый кусок приходится пропускать
		# много строк каждый раз
		with fiona.open(self.source, layer=self.layername, driver=self.fiona_driver, **self.kwargs) as self._handler:
			for geometry_filter in self.geometry_filter_pbar:
				self._stopped_iteration = False
				iterator = self._handler.filter(mask=geometry_filter.__geo_interface__ if geometry_filter is not None else None)
				with self._pbar(desc=f'rows in {self.source}', total=self.total_rows) as reader_bar:
					while not self._stopped_iteration:
						if self.emergency_stop.value: return

						rows = []
						try:
							while self.chunk_size is None or len(rows) < self.chunk_size:
								row = next(iterator)
								data = row['properties']
								# fiona makes empty geometries None :(
								if row['geometry'] is None:
									continue
								data['geometry'] = shape(row['geometry'])
								rows.append(data)
						except StopIteration:
							self._stopped_iteration = True
						if len(rows) == 0:
							self._stopped_iteration = True

						if self.emergency_stop.value: return

						if len(rows) == 0: continue

						gdf = gpd.GeoDataFrame(rows, crs=self.crs, index=self._range_index(rows))
						if self.emergency_stop.value: return
						yield gdf
						reader_bar.update(len(gdf))

	def stats(self):
		# make sqlite connection and get min, max, avg.
		import sqlite3
		conn = sqlite3.connect(self.source)
		curs = conn.execute(f'''pragma table_info('{self.layername}')''')
		stats = []
		for col in curs.fetchall():
			num, name, dtype, *rest = col
			stat = {'name': name, 'type': dtype}
			if dtype in ('INTEGER', 'REAL'):
				curs3 = conn.execute(f'''select min({name}), avg({name}), max({name}),
					sum(({name} - (select avg({name}) from '{self.layername}')) * ({name} - (select avg({name}) from '{self.layername}'))) / count({name}), count({name}) from '{self.layername}' ''')
				stat['min'], stat['mean'], stat['max'], stat['variance'], stat['count'] = curs3.fetchall()[0]
			stats.append(stat)

		return stats


class GpkgWriter(BaseWriter):
	fiona_driver = FIONA_DRIVER
	target_regexp = PATH_REGEXP

	def __init__(self, target, sync: bool = False, **kwargs):
		gpkg_match = re.match(self.target_regexp, target)
		if not gpkg_match:
			raise ValueError(f'filename {target} is not GeoPackage path')

		super().__init__(target, sync, **kwargs)
		g = gpkg_match.groupdict()
		self.target = g['file_path']
		self.layername = g['layer_name'] or g['file_own_name']

	def _write_sync(self, df):
		dprint('gpkg write sync')
		if df is None or len(df) == 0:
			return

		#dicts_to_json(df, inplace=True)
		dprint('gpkg write sync made dicts')
		self._open_handler(df)
		dprint(f'checked handler, writing {len(df)} records')
		self._handler.writerecords(df.iterfeatures())
		dprint('gpkg write sync records done')
		# replace by df.to_file(mode='a') later

	def _open_handler(self, df=None):
		if self._handler is not None:
			dprint('gpkg open handler not opening')
			return

		dprint('gpkg opening handler')
		import fiona
		from geopandas.io.file import infer_schema
		if df is None:  # or len(df) == 0:
			# write empty dataframe to file anyway
			# TODO: if empty df will be written initially, the schema may have incorrect geometry type
			schema = {'geometry': 'Point', 'properties': OrderedDict()}
		else:
			schema = infer_schema(df)

		# instead of self._cleanup_target(), delete fiona layer
		# df is None when we try to write zero data
		# (this happens when you filter a dataset and have nothing out, but still have to write that into a file,
		# otherwise there's no way to tell zero data from a crash)
		crs_ = None
		if df is not None and df.crs is not None:
			crs_ = df.crs.to_string()
		dprint(f'opening fiona handler, target {self.target}')
		self._handler = fiona.open(self.target, 'w', layer=self.layername, crs=crs_, driver=self.fiona_driver, schema=schema)

	def _close_handler(self):
		dprint('gpkg close handler need to open handler')
		self._open_handler()
		dprint('gpkg close handler closing')
		self._handler.close()
		self._handler = None

	def _cancel(self):
		sleep(0)
		self._close_handler()
		dprint('gpkg cancel closed')
		import fiona
		layers = fiona.listlayers(self.target)
		dprint('gpkg cancel removing the layers')
		fiona.remove(self.target, layer=self.layername, driver=self.fiona_driver)  # delete layer there
		# delete file if GPKG had only 1 layer, that was just deleted
		if len(layers) == 1:
			dprint('gpkg cancel removing file')
			os.unlink(self.target)


class GpkgDriver(BaseDriver):
	reader = GpkgReader
	writer = GpkgWriter
	data_type = gpd.GeoDataFrame
	path_regexp = PATH_REGEXP

	@classmethod
	def read_df(cls, path, path_match, crs=None, *args, **kwargs):
		match = re.match(PATH_REGEXP, path)
		filename = match['file_name']
		file_path = match['file_path']
		layer_name = match['layer_name']
		own_name = match['file_own_name']

		if layer_name in ('', None):
			try:
				layers = fiona.listlayers(file_path)
			except ValueError:
				raise ValueError('Fiona driver can\'t read layers from file %s' % filename)

			if len(layers) == 1 and layer_name in ('', None):
				layer_name = layers[0]
			elif own_name in layers:
				layer_name = own_name
			else:
				raise ValueError('Can\'t detect default layer in %s. Layers available are: %s' % (filename, ', '.join(layers)))

		return GpkgDriver.gpd_read(file_path, driver=FIONA_DRIVER, crs=crs, layer=layer_name, **kwargs)

	@classmethod
	def write_df(cls, df, path, path_match, *args, **kwargs):
		filepath = path_match['file_path']
		layer_name = path_match['layer_name'] or path_match['file_own_name']

		if os.path.exists(filepath):
			if layer_name in fiona.listlayers(filepath):
				fiona.remove(filepath, FIONA_DRIVER, layer_name)

		df.to_file(filepath, driver=FIONA_DRIVER, encoding='utf-8', layer=layer_name)


driver = GpkgDriver
