from erde.op import table
import pandas as pd
import geopandas as gpd
import pytest
import random
from shapely.geometry import Point, LineString


def _make_points_list():
	return [Point(random.random() * 360 - 180, random.random() * 180 - 90) for i in range(20)]


def test_to_list():
	# list, GeoSeries or GeoDataFrame['geometry'] should work normally
	points = _make_points_list()
	for arg in [points, tuple(points), gpd.GeoSeries(points), gpd.GeoDataFrame({'geometry': points})]:
		assert table._tolist(arg) == points

	# non-points should not work and raise ValueError
	# as well as series/dataframe of points + non-points
	bufs = gpd.GeoSeries(points).buffer(.01, resolution=3).tolist()
	for arg in [gpd.GeoSeries(bufs), tuple(bufs), gpd.GeoDataFrame({'geometry': bufs}), gpd.GeoSeries(points + bufs), gpd.GeoDataFrame({'geometry': points + bufs})]:
		with pytest.raises(ValueError):
			table._tolist(arg)

	# other types of data should raise TypeError
	for arg in [dict(enumerate(points)), pd.Series(points), pd.DataFrame({'x': points}), 1, 'string sample', None]:
		with pytest.raises(TypeError):
			table._tolist(arg)


def test_index():
	points = _make_points_list()
	start_num = random.randint(1000, 2000)
	ind = list(range(start_num, start_num + len(points)))

	for arg in [gpd.GeoSeries(points, index=ind), gpd.GeoDataFrame({'geometry': points}, index=ind)]:
		retval = table._index(arg)
		assert isinstance(retval, pd.Index)
		assert retval.tolist() == ind

	for arg in [points, tuple(points)]:
		retval = table._index(arg)
		assert isinstance(retval, pd.RangeIndex)
		assert retval.tolist() == list(range(len(points)))


# error: coordinate is invalid
# error: 500 error

# def test_