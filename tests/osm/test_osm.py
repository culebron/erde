import yaargh
import pytest
from erde.op import osm

def test_commands():
	assert osm.ogr_cmd('points,multipolygons', None, 'file1.osm', 'file2.gpkg').strip() == 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file2.gpkg file1.osm points multipolygons'

	# ogr_cmd adds config file if there are columns requested
	assert osm.ogr_cmd('points,multipolygons', ['a', 'b'], 'file1.osm', 'file2.gpkg').strip() == 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file2.gpkg file1.osm points multipolygons --config OSM_CONFIG_FILE /tmp/_3_osmcfg.ini'

	# for points we request column highway, and 'b' for all
	assert osm.ogr_cmd('points,multipolygons', ['points=highway', 'b'], 'file1.osm', 'file2.gpkg').strip() == 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file2.gpkg file1.osm points multipolygons --config OSM_CONFIG_FILE /tmp/_3_osmcfg.ini'

	# this is incorrect way to ask for columns: instead of highway= it should be points/lines/multipolygons=...
	with pytest.raises(yaargh.CommandError):
		osm.ogr_cmd('points,multipolygons', ['highway=primary', 'b'], 'file1.osm', 'file2.gpkg')

	# 'geometry' column is also forbidden
	with pytest.raises(yaargh.CommandError):
		osm.ogr_cmd('points,multipolygons', ['highway', 'geometry'], 'file1.osm', 'file2.gpkg').strip()

	# unrecognized format is forbidden
	with pytest.raises(yaargh.CommandError):
		osm.ogr_cmd('points,multipolygons', None, 'file1.osm', 'file2.unknown extention')
