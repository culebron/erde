from contextlib import contextmanager
from erde import read_df, read_geom, CONFIG
from erde.op import isochrone as ic
from shapely.geometry import box, Point  # needed for pickle load
from unittest import mock
import geopandas as gpd  # needed for pickle load
import pandas as pd

t = 'tests/isochrone/'
sources = read_df(t + 'sources.csv')


def get_ir():
	return ic.IsochroneRouter(sources['geometry'].values[0], 'http://localhost:5000', (5, 10, 15), 5)

def test_bbox():
	ir = get_ir()
	bound_poly = read_geom(t + 'bounds.csv')
	assert box(*ir.bounds).almost_equals(bound_poly)

def test_grid():
	ir = get_ir()
	grid = ir.grid
	grid_size = len(grid)
	assert grid_size == 1068  # for default settings

	ir2 = ic.IsochroneRouter(sources['geometry'].values[0], 'http://localhost:5000', (5, 10, 15), 5)
	ir2.grid_density = 2
	grid_size2 = len(ir2.grid)
	# check that the new grid is roughly double of the first one
	assert abs((grid_size2 - grid_size * 2) / grid_size / 2) < .1

def _get_pickled_table():
	import pickle

	# dict with records was saved to pickle, because otherwise python3.6 crashes when trying to recreate a dumped DataFrame
	with open(t + 'table.pickle', 'rb') as f:
		return pd.DataFrame(pickle.load(f))

def test_polygons():
	# test if with given table, the result is about the same
	ir = get_ir()

	polys = read_df(t + 'polygons.csv').set_index('duration')
	polys.crs = 4326
	polys.to_crs(3857, inplace=True)

	with mock.patch('erde.op.isochrone.table_route', return_value=[_get_pickled_table()]):
		for i, r in ir.polygons.to_crs(3857).iterrows():
			other = polys.loc[r.duration, 'geometry']
			# geom_almost_equals sometimes does not work even when difference is in micrometers. To fix it, let's expand polygons by 10 cm and check if the other is inside. If difference is any bigger, one will stick out.
			assert r.geometry.within(other.buffer(.1, resolution=3))
			assert other.within(r.geometry.buffer(.1, resolution=3))


class PropertyMock(mock.Mock):
	def __get__(self, obj, obj_type=None):
		return self(obj, obj_type)


def test_set_grid():
	ir = get_ir()

	# test if setting (incorrect) grid will make the router return it, rather than a normal one

	# 1. check if it returns a different one
	expected_grid = read_df(t + 'grid.csv')[:2]  # just 2 points
	assert len(ir.grid) != len(expected_grid)

	# 2. set to new one
	ir.grid = expected_grid
	# 3. check if we get it from .grid
	assert len(ir.grid) == len(expected_grid)

	ir = get_ir()
	from random import randint
	ri = randint(0, 100000)

	def new_grid(self, obj_type=None):
		return ri

	with mock.patch('erde.op.isochrone.IsochroneRouter.grid', new_callable=PropertyMock) as m:
		m.side_effect = new_grid
		assert ir.grid == ri

	m.assert_called_once()
	assert ir._grid is None


@contextmanager
def _patch_table(df):
	with mock.patch('erde.op.isochrone.table_route', return_value=[df]) as patched_route:
		yield patched_route


def test_set_routed():
	ir = get_ir()
	new_routed = read_df(t + 'routed.csv')[-10:]

	df = _get_pickled_table()
	with _patch_table(df) as patched_table:
		assert len(ir.routed) == len(df) + 1
		assert len(new_routed) != len(ir.routed)
		assert ir._routed is not None
		patched_table.assert_called_once()

	ir = get_ir()
	with _patch_table(df) as patched_table:
		assert ir._routed is None
		ir.routed = new_routed
		assert ir._routed is not None
		assert len(ir.routed) == len(new_routed)
		patched_table.assert_not_called()


def test_set_grid_step():
	ir = get_ir()
	ir2 = get_ir()
	ir2.speed *= 2
	assert ir2.grid_step == ir.grid_step * 2

	ir2.grid_step *= 2
	assert ir2.grid_step == ir.grid_step * 4


def test_small_grid():
	# in code, if the IsochroneRouter.grid is too small, some functions return None. Not sure if it's the right way to do it, but we'll test this behaviour.
	ir = get_ir()
	ir.grid = ir.grid[:3]
	with mock.patch('erde.op.isochrone.table_route', side_effect=RuntimeError) as patched_route:
		assert ir.raster is None
		assert ir.polygons is None
		patched_route.assert_not_called()


def _new_table_route(src, dst, mode, max_table_size=2_000, threads=10, annotations='duration', pbar=True, cache_name=None, executor='process', extra_params=None):
	import numpy as np
	from erde import utils
	# sources is list of 1 element (origin Point)
	# destinations is gdf of points
	p = np.array(utils.transform(src[0], 4326, 3857).coords)
	if dst.crs is None:
		dst.crs = 4326
	dst2 = dst.to_crs(3857).copy()
	# distance from source point + a wave like 7-end star
	# distance
	delta = np.array([dst2.geometry.x, dst2.geometry.y]).T - p
	# formula of star
	dst2['distance'] = np.sum(delta ** 2, axis=1) ** .5 * (np.cos(np.arctan2(*delta.T) * 7) + 2)
	dst2['duration'] = dst2['distance'] / 1.67
	dst2.to_crs(4326, inplace=True)
	dst2['geometry_dest'] = dst2['geometry']
	dst2['geometry'] = src[0]
	dst2['source_snap'] = 0
	dst2['destination_snap'] = 0
	return [dst2]


@contextmanager
def _patch_table_route():
	with mock.patch('erde.op.isochrone.table_route', side_effect=_new_table_route) as m, mock.patch.dict(CONFIG['routers'], {'foot': 'http://localhost:5001', 'local': 'http://localhost:5000'}):
		yield m


extra_params = {
	'router': ['http://localhost:5000', 'foot', 'car'],
	'durations': [(5, 10, 15), (10, 20, 30)],
	'speed': [5],
	'grid_density': [1, .5, 2],
	'max_snap': [100, 200, 500],
	'mts': [1000, 10_000, 100_000],
}


def test_cli():
	# 1. combinations of different params
	# 2. mts is forwarded to table_route
	# (sources: gpd.GeoDataFrame, router, durations, speed:float, grid_density:float = 1.0, max_snap: float = MAX_SNAP, mts: int = MAX_TABLE_SIZE, pbar:bool=False)

	for changed_k, values in extra_params.items():
		if len(values) < 2:  # this parameter does not change, no need to double-check it
			continue

		for val in values:
			params = {k: v[0] for k, v in extra_params.items()}
			params[changed_k] = val

			with _patch_table_route() as m:
				gen = ic.main(sources, **params)
				polygons_df = next(gen)

			m.assert_called_once()
			ar, kw = m.call_args

			assert set(polygons_df.duration) == set(params['durations'])
			assert all(polygons_df.geometry.geom_type == 'MultiPolygon')
			assert ar[2] == params['router']
			assert kw['max_table_size'] == params['mts']
			expected_grid_size = params['grid_density'] * max(params['durations']) ** 2 * 4.75
			actual_grid_size = len(ar[1])

			# make sure expected_grid_size is actual_grid_size with 5% precision
			assert round(expected_grid_size / actual_grid_size * 20) == 20


def test_geometry_validity():
	with _patch_table_route():
		ir = get_ir()
		poly_gdf = ir.polygons
		assert all(poly_gdf.geometry.contains(ir.origin))


def test_string_params():
	# 3. params as strings => makes difference in results
	s2 = sources.copy()
	for k, v in extra_params.items():
		s2[k] = (v * len(s2))[:len(s2)]

	assert len(s2.mts.unique()) > 1
	assert len(s2.router.unique()) > 1

	with _patch_table_route() as m:
		resp = pd.concat(ic.main(s2, **{k: k for k in extra_params.keys()}))
		print(resp)

	for args, row in zip(m.call_args_list, s2.to_dict(orient='records')):
		ar, kw = args

		assert ar[0][0] == row['geometry']
		assert ar[2] == row['router']
		assert kw['max_table_size'] == row['mts']
