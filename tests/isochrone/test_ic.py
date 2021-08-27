from contextlib import contextmanager
from erde import read_df, read_geom, write_df
from erde.op import isochrone as ic
from shapely.geometry import box
from unittest import mock
import geopandas as gpd
import pytest

t = 'tests/isochrone/'
sources = read_df(t + 'sources.csv')


#@contextmanager
#def _requests_get():
	#with mock.patch('erde.op.table.get_retry', return_value=)
	#yield

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

	with open(t + 'table.pickle', 'rb') as f:
		with mock.patch('erde.op.table.table_route', return_value=pickle.load(f)):
			p = ir.polygons
			assert all(p.geometry.geom_almost_equals(gpd.GeoSeries(p.duration.map(polys.geometry), crs=4326)))
