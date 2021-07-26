from . import check_path_exists
from .geojson import GeoJsonDriver, GeoJsonReader, GeoJsonWriter
from .gpkg import GpkgWriter, GpkgReader
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
		check_path_exists(source)
		super(GpkgReader, self).__init__(source, geometry_filter, chunk_size, sync, pbar, **kwargs)

		try:
			self._read_schema()
		except fiona.errors.DriverError as e:
			raise RuntimeError(*e.args)


class ShpWriter(GeoJsonWriter):
	fiona_driver = FIONA_DRIVER
	target_regexp = PATH_REGEXP
	layername = None

	def __init__(self, target, sync:bool=False, **kwargs):
		super(GpkgWriter, self).__init__(target, sync, **kwargs)
		name_match = re.match(self.target_regexp, self.target)
		assert name_match, f'filename {target} is not GeoJSON path'


class ShpDriver(GeoJsonDriver):
	reader = ShpReader
	writer = ShpWriter
	fiona_driver = FIONA_DRIVER
	path_regexp = r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>shp))$'

driver = ShpDriver
