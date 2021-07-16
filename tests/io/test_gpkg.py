from erde.io.gpkg import driver as dr
from erde.io import read_file, write_file
from time import sleep
from unittest import mock
import errno
import geopandas as gpd
import os
import pytest


def d(s): return 'tests/io/data/' + s

points_file = d('blocks-points.gpkg')
match_points = d('match-points.gpkg')


def silentremove(filename):
	filename, *rest = filename.split(':')
	try:
		os.remove(filename)
	except OSError as e:
		if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
			# re-raise exception if a different error occurred
			raise

def test_read():
	for s in (True, False):
		reader = dr.open_read(points_file, sync=s)
		for df in reader:
			assert isinstance(df, gpd.GeoDataFrame)

def test_bad_file():
	not_a_gpkg = '/tmp/not-a-gpkg.gpkg'
	for s in (True, False):
		with pytest.raises(RuntimeError):
			dr.open_read('/tmp/not-a-gpkg-2.gpkg', sync=s)

		# empty file
		with open(not_a_gpkg, 'w') as f:
			f.write('')

		with pytest.raises(RuntimeError):
			dr.open_read(not_a_gpkg, sync=s)

def test_exception_in_read():
	class TmpReader(dr.reader):
		def _read_sync(self):
			for i, df in enumerate(super()._read_sync()):
				if i == 2:
					sleep(1)
					raise RuntimeError('planned crash')
				yield df

	for s in (True, False):
		reader = TmpReader(points_file, sync=s, chunk_size=10)
		assert len(next(reader)) == 10
		assert len(next(reader)) == 10

		with pytest.raises(RuntimeError):
			next(reader)

		reader._handler.close()

def test_geometry_filter():
	for s in (True, False):
		filter_source = d('match-simple-polys.geojson')
		filter_df = read_file(filter_source)
		filter_geom = filter_df['geometry'].unary_union

		expected_names = set('ACDFGI')

		for test_filter in (filter_source, filter_df, filter_geom, None):
			stream = dr.open_read(match_points, test_filter, chunk_size=100_000, sync=s)
			for df in stream:
				assert len(df) > 0
				if test_filter is not None:  # when None, the set is bigger than expected_names
					assert set(df['name'].tolist()).issubset(expected_names)

		sleep(1)


tmp_points = '/tmp/temporary-points.gpkg'

def test_write():
	for s in (True, False):
		old_df = read_file(match_points)

		with dr.open_write(tmp_points, sync=s) as w:
			for df in dr.open_read(match_points, chunk_size=10, sync=s):
				w(df)

		new_df = read_file(tmp_points)
		assert sorted(new_df['name'].tolist()) == sorted(old_df['name'].tolist())

def test_write_error():
	for s in (False, True):
		try:
			os.unlink(tmp_points)
		except:
			pass

		with pytest.raises(RuntimeError):
			with dr.open_write(tmp_points, sync=s) as w:
				rd = dr.open_read(points_file, chunk_size=10, sync=s)
				for i, df in enumerate(rd):
					if i == 2:
						raise RuntimeError('planned exception')

					w(df)

		rd._handler.close()

		# file layer does not exist
		sleep(1)
		assert not os.path.exists(tmp_points)
		if not s:
			# if async, process must be ended
			assert not w.background_process.is_alive()
		sleep(1)

def test_write_empty():
	import fiona
	for s, ss in {True: 'sync', False: 'async'}.items():
		ds_name = f'empty-test-{ss}'
		ds_path = f'/tmp/{ds_name}.gpkg'
		silentremove(ds_path)

		with dr.open_write(ds_path):
			pass

		assert os.path.exists(ds_path)
		layers = fiona.listlayers(ds_path)
		assert ds_name in layers
