#!/usr/bin/python3.6

from .gpkg import GpkgDriver, GpkgReader, GpkgWriter
from . import FORMATS
import fiona
import os
import re

_format_re = FORMATS['geojson']

class GeoJsonReader(GpkgReader):
	fiona_driver = 'GeoJSON'
	name_regexp = _format_re
	layername = None

	def __init__(self, source, geometry_filter=None, chunk_size:int=10_000, sync:bool=False, pbar:bool=True, **kwargs):
		chunk_size = chunk_size or 10_000
		name_match = re.match(self.name_regexp, source)
		assert name_match, f'File name {source} is not a valid {self.fiona_driver} path.'

		super(GpkgReader, self).__init__(source, geometry_filter, chunk_size, sync, pbar, **kwargs)

		try:
			self._read_schema()
		except fiona.errors.DriverError as e:
			raise RuntimeError(*e.args)


class GeoJsonWriter(GpkgWriter):
	fiona_driver = 'GeoJSON'
	target_regexp = _format_re
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
	source_extension = 'geojson'
	source_regexp = None


driver = GeoJsonDriver
