from erde import read_df, read_geom
from erde.op import isochrone as ic
from shapely.geometry import box, Point
from unittest import mock
import geopandas as gpd
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


def test_polygons():
	# test if with given table, the result is about the same
	ir = get_ir()

	polys = read_df(t + 'polygons.csv').set_index('duration')

	import pickle

	# dict with records was saved to pickle, because otherwise python3.6 crashes when trying to recreate a dumped DataFrame
	with open(t + 'table.pickle', 'rb') as f:
		df = pd.DataFrame(pickle.load(f))

	with mock.patch('erde.op.isochrone.table_route', return_value=[df]):
		for i, r in ir.polygons.iterrows():
			other = polys.loc[r.duration, 'geometry']
			print(r['geometry'])
			print(other)
			assert r['geometry'].almost_equals(other)


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
