from contextlib import contextmanager
from erde.op import osm
from unittest import mock
import pytest
import yaargh


def test_commands():
	for params, exp_result in (
		(('points,multipolygons', None, 'file1.osm', 'file2.gpkg'), 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file2.gpkg file1.osm points multipolygons'),
		# ogr_cmd adds config file if there are columns requested
		(('points,multipolygons', ['a', 'b'], 'file1.osm', 'file2.gpkg'), 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file2.gpkg file1.osm points multipolygons --config OSM_CONFIG_FILE /tmp/_3_osmcfg.ini'),
		# for points we request column highway, and 'b' for all
		(('points,multipolygons', ['points=highway', 'b'], 'file1.osm', 'file2.gpkg'), 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file2.gpkg file1.osm points multipolygons --config OSM_CONFIG_FILE /tmp/_3_osmcfg.ini'),
		):
		assert osm.ogr_cmd(*params)[0].strip() == exp_result

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
	with mock.patch('os.path.exists', return_value=True) as m1, mock.patch('os.remove', return_value=None) as m2, mock.patch('os.system', return_value=0) as m3:
		yield m1, m2, m3


def test_remove_command():
	# this command should be tested separately and then patched for other tests, to reduce patching operations

	c = osm.Remove('my_path')
	assert str(c) == "Remove('my_path')"
	with _patch_os() as (m1, m2, m3):
		assert c() == 0

	m1.assert_called_once()
	m2.assert_called_once()

	with _patch_os() as (m1, m2, m3):
		m1.return_value = False
		assert c() == 0

	m1.assert_called_once()
	m2.assert_not_called()

	with _patch_os() as (m1, m2, m3):
		m2.side_effect = OSError('artificial exception')
		assert c() == 1


def test_errors():
	for fnames in [['file1.osm.pbf'], ['bad-input.txt', 'good-output.osm.pbf'], ['non-existent.osm.pbf', 'non-existent.osm.bpf']]:
		with pytest.raises(yaargh.CommandError):
			osm.main(*fnames)

	# error handling when executing a command
	with _patch_os() as (exists, remove, system):
		system.return_value = 1
		with mock.patch('sys.exit') as exit:
			osm.main('file1.osm.pbf', 'file2.osm.gz')

	# sys exit must have been called once with status = 1
	exit.assert_called_once()
	exit.assert_called_with(1)


def test_main():
	default_kwargs = {'layers': 'points,lines,multipolygons', 'tags': None, 'keep_tmp_files': False, 'columns': None, 'crop': None, 'dry': True}

	tests = (
		(
			['file1.osm.pbf', 'file2.osm.gz'], {},
			["Remove('file2.osm.gz')", 'osmium cat file1.osm.pbf -o file2.osm.gz']),
		(
			['file1.osm.pbf', 'file2.osm.pbf', 'file3.osm.pbf'], {},
			["Remove('file3.osm.pbf')", 'osmium cat file1.osm.pbf file2.osm.pbf -o file3.osm.pbf']),
		(
			['file1.osm.pbf', 'file2.gpkg'], {}, ["Remove('file2.gpkg')", 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file2.gpkg file1.osm.pbf points lines multipolygons ']),
		(
			['file1.osm.pbf', 'file2.osm.pbf', 'file3.gpkg'], {}, ["Remove('/tmp/_0_cat.osm.pbf')", 'osmium cat file1.osm.pbf file2.osm.pbf -o /tmp/_0_cat.osm.pbf', "Remove('file3.gpkg')", 'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f gpkg file3.gpkg /tmp/_0_cat.osm.pbf points lines multipolygons ', "Remove('/tmp/_0_cat.osm.pbf')"]),
			(['file1.osm.pbf', 'file2.osm.bz2'], {'tags': ['wr/highway']}, ["Remove('file2.osm.bz2')", 'osmium tags-filter file1.osm.pbf wr/highway -o file2.osm.bz2']),
			(['file1.osm.pbf', 'file2.osm.bz2'], {'tags': ['wr/highway'], 'crop': 'some-file.geojson'}, ["Remove('/tmp/_0_file1.filtered.osm.pbf')", 'osmium tags-filter file1.osm.pbf wr/highway -o /tmp/_0_file1.filtered.osm.pbf', "Remove('file2.osm.bz2')", 'osmium extract /tmp/_0_file1.filtered.osm.pbf -o file2.osm.bz2 -p "some-file.geojson"', "Remove('/tmp/_0_file1.filtered.osm.pbf')"]
),
	)

	for args, kwargs, exp_result in tests:
		for d in (True, False):
			kwargs = {**default_kwargs, **kwargs, 'dry': d}
			with _patch_os() as (exists, remove, system):
				result = [str(c).strip() for c in osm.main(*args, **kwargs)]

			assert result == [i.strip() for i in exp_result]

			# test if remove was not called in dry run
			if d:
				remove.assert_not_called()
			else:
				# and was called exact amount of times in not-dry run
				removes = sum(1 for cmd in exp_result if cmd.startswith('Remove'))
				assert remove.call_count == removes
