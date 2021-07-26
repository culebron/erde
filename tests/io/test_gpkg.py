from erde.io.gpkg import driver as dr
from erde import read_df
from time import sleep
import errno
import geopandas as gpd
import os
import pytest


d = 'tests/io/data/'

points_file = d + 'blocks-points.gpkg'
match_points = d + 'match-points.gpkg'


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
		reader = dr.read_stream(points_file, sync=s)
		for df in reader:
			assert isinstance(df, gpd.GeoDataFrame)

def test_bad_file():
	not_a_gpkg = '/tmp/not-a-gpkg.gpkg'
	for s in (True, False):
		with pytest.raises(FileNotFoundError):
			dr.read_stream('/tmp/not-a-gpkg-2.gpkg', sync=s)

		# empty file
		with open(not_a_gpkg, 'w') as f:
			f.write('')

		with pytest.raises(RuntimeError):
			dr.read_stream(not_a_gpkg, sync=s)

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
		filter_source = d + 'match-simple-polys.geojson'
		filter_df = read_df(filter_source)
		filter_geom = filter_df['geometry'].unary_union

		expected_names = set('ACDFGI')

		for test_filter in (filter_source, filter_df, filter_geom, None):
			stream = dr.read_stream(match_points, test_filter, chunk_size=100_000, sync=s)
			for df in stream:
				assert len(df) > 0
				if test_filter is not None:  # when None, the set is bigger than expected_names
					assert set(df['name'].tolist()).issubset(expected_names)

		sleep(1)


tmp_points = '/tmp/temporary-points.gpkg'

def test_write():
	for s in (True, False):
		print(f'gpkg test write begin s={s}')
		old_df = read_df(match_points)

		if os.path.exists(tmp_points):
			os.unlink(tmp_points)

		assert not os.path.exists(tmp_points), f"could not delete file {tmp_points} before test"
		with dr.write_stream(tmp_points, sync=s) as w:
			for df in dr.read_stream(match_points, chunk_size=10):
				w(df)

		assert os.path.exists(tmp_points), f"file {tmp_points} does not exist, but should have been created"
		new_df = read_df(tmp_points)
		assert sorted(new_df['name'].tolist()) == sorted(old_df['name'].tolist())
		print(f'gpkg test write end s={s}')

def test_write_error():
	for s in (False, True):
		print(f'gpkg test write error begin s={s}')
		try:
			os.unlink(tmp_points)
		except:
			pass

		with pytest.raises(RuntimeError):
			with dr.write_stream(tmp_points, sync=s) as w:
				rd = dr.read_stream(points_file, chunk_size=10, sync=s)
				for i, df in enumerate(rd):
					if i == 2:
						print('raising exception')
						raise RuntimeError('planned exception')

					print('writing')
					w(df)

		rd._handler.close()

		# file layer does not exist
		sleep(1)
		assert not os.path.exists(tmp_points)
		if not s:
			# if async, process must be ended
			assert not w.background_process.is_alive()
		sleep(1)
		print(f'gpkg test write error end s={s}')


def test_write_empty():
	import fiona
	for s, ss in {True: 'sync', False: 'async'}.items():
		ds_name = f'empty-test-{ss}'
		ds_path = f'/tmp/{ds_name}.gpkg'
		silentremove(ds_path)

		with dr.write_stream(ds_path):
			pass

		assert os.path.exists(ds_path)
		layers = fiona.listlayers(ds_path)
		assert ds_name in layers


def test_stream_guess_layer():
	from erde import read_stream
	# must find the only layer
	for df in read_stream(d + 'layer-name-different.gpkg'):
		assert len(df) > 0

	# must guess by filename
	for df in read_stream(d + 'guessable-layer.gpkg'):
		assert len(df) > 0

	# if 2 layers and none like file name, raises exception
	with pytest.raises(RuntimeError):
		read_stream(d + 'unguessable-layer.gpkg')


def test_read_stats():
	from erde import read_stream
	rd = read_stream(d + 'stats.gpkg')
	print()
	print(rd.stats())
	print(next(rd))

