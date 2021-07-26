from .geojson import GeoJsonDriver, GeoJsonReader, GeoJsonWriter
import fiona
import os
import re

FIONA_DRIVER = 'ESRI Shapefile'
PATH_REGEXP = r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>shp))$'

class ShpReader(GeoJsonReader):
	fiona_driver = FIONA_DRIVER
	source_regexp = PATH_REGEXP
	layername = None

	def __init__(self, source, geometry_filter=None, chunk_size:int=10_000, sync:bool=False, pbar:bool=True, **kwargs):
		super(GeoJsonReader, self).__init__(source, geometry_filter, chunk_size, sync, pbar, **kwargs)

		try:
			self._read_schema()
		except fiona.errors.DriverError as e:
			raise RuntimeError(*e.args)


class ShpWriter(GeoJsonWriter):
	fiona_driver = FIONA_DRIVER
	target_regexp = PATH_REGEXP
	layername = None

	def __init__(self, target, sync:bool=False, **kwargs):
		super(GeoJsonWriter, self).__init__(target, sync, **kwargs)
		name_match = re.match(self.target_regexp, self.target)
		assert name_match, f'filename {target} is not GeoJSON path'

	def _cancel(self):
		if self._handler is not None:
			self._close_handler()
			os.unlink(self.target)


class ShpDriver(GeoJsonDriver):
	reader = ShpReader
	writer = ShpWriter
	path_regexp = r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>shp))$'

	@classmethod
	def read_df(cls, path, path_match, crs=None, *args, **kwargs):
		return ShpDriver.gpd_read(path, crs, driver=FIONA_DRIVER, *args, **kwargs)

driver = ShpDriver
