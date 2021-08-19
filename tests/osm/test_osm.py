from contextlib import contextmanager
import yaargh
import pytest
from unittest import mock
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


@contextmanager
def _patch_os():
	with mock.patch('os.path.exists', return_value=True) as m1, mock.patch('os.remove', return_value=None) as m2:
		yield m1, m2


def test_remove_command():
	# this command should be tested separately and then patched for other tests, to reduce patching operations
	c = osm.Remove('my_path')
	assert str(c) == "Remove('my_path')"
	with _patch_os() as (m1, m2):
		assert c() == 0

	m1.assert_called_once()
	m2.assert_called_once()

	with _patch_os() as (m1, m2):
		m1.return_value = False
		assert c() == 0

	m1.assert_called_once()
	m2.assert_not_called()

	with _patch_os() as (m1, m2):
		m2.side_effect = OSError('artificial exception')
		assert c() == 1
