"""Line-delimited GeoJSON"""
from .geojson import GeoJsonReader, GeoJsonWriter, GeoJsonDriver

FIONA_DRIVER = 'GeoJSONSeq'
PATH_REGEXP = r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>geojsonl\.json|geojsonl))$'

class GeoJsonSeqReader(GeoJsonReader):
	fiona_driver = FIONA_DRIVER
	source_regexp = PATH_REGEXP

class GeoJsonSeqWriter(GeoJsonWriter):
	fiona_driver = FIONA_DRIVER
	target_regexp = PATH_REGEXP


class GeoJsonSeqDriver(GeoJsonDriver):
	reader = GeoJsonSeqReader
	writer = GeoJsonSeqWriter
	path_regexp = PATH_REGEXP

	@staticmethod
	def read_df(path, path_match, crs=None, *args, **kwargs):
		return GeoJsonSeqDriver.gpd_read(path, crs, driver=FIONA_DRIVER, *args, **kwargs)

	@staticmethod
	def write_df(df, path, path_match, *args, driver=FIONA_DRIVER, **kwargs):
		GeoJsonDriver.write_df(df, path, path_match, *args, driver=driver, **kwargs)

driver = GeoJsonSeqDriver
