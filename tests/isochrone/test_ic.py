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
		p = ir.polygons
		assert all(p.geometry.geom_almost_equals(gpd.GeoSeries(p.duration.map(polys.geometry), crs=4326)))
