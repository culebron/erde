from unittest import mock
import pytest
import geopandas as gpd
from geopandas.testing import assert_geoseries_equal
from erde import buffer, read_df
from shapely.geometry import Point

points_df = read_df('tests/buffer/points.geojson')
bufs_df = read_df('tests/buffer/buffers.geojson')
bufs_dissolved_df = read_df('tests/buffer/buffers-dissolved.geojson')

def test_geoseries():
	assert len(points_df) == 7
	# if we call buffer with geoseries, it returns geoseries

	init_gtype = points_df.geometry.geom_type
	res = 5
	bufs2_df = buffer.buffer(points_df, 500, resolution=res)

	# check that resolution works
	assert len(bufs2_df.geometry.values[0].exterior.coords) == res * 4 + 1

	# original dataframe & geometries not changed
	assert all(points_df.geometry.geom_type == init_gtype)

	# geodataframe => geodataframe
	assert isinstance(bufs2_df, gpd.GeoDataFrame)
	assert_geoseries_equal(bufs_df.geometry, bufs2_df.geometry, check_less_precise=True)
	assert len(points_df) == len(bufs2_df)  # same number of geometries (not so with dissolve)
	assert all(points_df.index == bufs2_df.index)  # same indice
	assert bufs2_df.crs == points_df.crs  # same CRS

	# if geoseries, must return geoseries
	bufs_series = buffer.buffer(points_df.geometry, 500, resolution=res)
	assert isinstance(bufs_series, gpd.GeoSeries)
	assert all(bufs_series.index == points_df.index)  # same index

	res = 10
	bufs3_df = buffer.buffer(points_df, 500, resolution=res)
	# all geometries must be different if resolution is different
	assert not any(bufs3_df.geometry.geom_almost_equals(bufs_df.geometry))
	# check resolution has effect
	assert len(bufs3_df.geometry.values[0].exterior.coords) == res * 4 + 1

	bufs4_df = buffer.buffer(points_df, 500, resolution=5, dissolve=True)
	assert len(bufs4_df) == 4

	# default crs
	p2 = points_df.copy()
	p2.crs = None

	with pytest.raises(ValueError):
		buffer.buffer(p2, 500)

	bufs5_df = buffer.buffer(p2, 500, resolution=5, default_crs=4326)
	assert_geoseries_equal(bufs5_df.geometry, bufs_df.geometry, check_less_precise=True)

	# must raise TypeError if data is not geoseries or geodataframe
	with pytest.raises(TypeError):
		buffer.buffer(points_df.geometry.values[0], 500)

	# test if cap_style/join_style params are passed to GeoPandasBase.buffer
	def test_styles(c, j):
		with mock.patch('geopandas.base.GeoPandasBase.buffer', new=mock.MagicMock(return_value=bufs_df[:1].geometry)) as mck:
			buffer.buffer(points_df[:1], 750, cap_style=c, join_style=j)

		mck.assert_called_once()
		ca = mck.call_args
		assert ca[1]['cap_style'] == c
		assert ca[1]['join_style'] == j

	test_styles(3, 3)
	test_styles(2, 2)
