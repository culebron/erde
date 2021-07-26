from .gpkg import GpkgDriver, GpkgReader, GpkgWriter
import fiona
import os
import re

FIONA_DRIVER = 'GeoJSON'
PATH_REGEXP = r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>geojson))$'

class GeoJsonReader(GpkgReader):
	fiona_driver = FIONA_DRIVER
	source_regexp = PATH_REGEXP
	layername = None

	def __init__(self, source, geometry_filter=None, chunk_size:int=10_000, sync:bool=False, pbar:bool=True, **kwargs):
		# this __init__ repeats part of GPKG driver
		super(GpkgReader, self).__init__(source, geometry_filter, chunk_size, sync, pbar, **kwargs)

		try:
			self._read_schema()
		except fiona.errors.DriverError as e:
			raise RuntimeError(*e.args)


class GeoJsonWriter(GpkgWriter):
	fiona_driver = FIONA_DRIVER
	target_regexp = PATH_REGEXP
	layername = None

	def __init__(self, target, sync:bool=False, **kwargs):
		super(GpkgWriter, self).__init__(target, sync, **kwargs)
		name_match = re.match(self.target_regexp, self.target)
		assert name_match, f'filename {target} is not GeoJSON path'

	def _cancel(self):
		if self._handler is not None:
			self._close_handler()
			os.unlink(self.target)


class GeoJsonDriver(GpkgDriver):
	reader = GeoJsonReader
	writer = GeoJsonWriter
	path_regexp = PATH_REGEXP
	fiona_driver = FIONA_DRIVER

	@classmethod
	def read_df(cls, path, path_match, crs=None, *args, **kwargs):
		return cls.gpd_read(path, crs, driver=cls.fiona_driver, *args, **kwargs)

	@classmethod
	def write_df(cls, df, path, path_match, *args, driver=None, **kwargs):
		if driver is None:
			driver = cls.fiona_driver

		if os.path.exists(path):
			os.unlink(path)

		df.to_file(path, driver=driver, encoding='utf-8')


driver = GeoJsonDriver
