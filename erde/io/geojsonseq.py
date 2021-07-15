#!/usr/bin/python3.6

"""Line-delimited GeoJSON"""

from .geojson import GeoJsonReader, GeoJsonWriter, GeoJsonDriver
from . import FORMATS

_format_re = FORMATS['geojsonseq']

class GeoJsonSeqReader(GeoJsonReader):
	fiona_driver = 'GeoJSONSeq'
	name_regexp = _format_re

class GeoJsonSeqWriter(GeoJsonWriter):
	fiona_driver = 'GeoJSONSeq'
	target_regexp = _format_re


class GeoJsonDriver(GeoJsonDriver):
	reader = GeoJsonSeqReader
	writer = GeoJsonSeqWriter
	source_extension = None
	source_regexp = _format_re

driver = GeoJsonDriver
