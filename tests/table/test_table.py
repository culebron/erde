from contextlib import contextmanager
from erde import read_df
from erde.op import table
from shapely.geometry import Point
from unittest import mock
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import random
import re
import urllib.parse


def _make_points_list():
	return [Point(random.random() * 360 - 180, random.random() * 180 - 90) for i in range(20)]


def _get_ds():
	return read_df('tests/table/houses.csv'), read_df('tests/table/shops.csv')


def test__to_list():
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


def test__index():
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


call_params = []
responses = []

def _respond(url, params=None, retries=None):
	match = re.match(r'^.*?polyline\((?P<polyline>.*?)\)\?(?P<qs>.*)$', url)
	params = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.unquote(match['qs'])).items()}

	call_params.append(dict(params))

	result = {}
	for k in ('sources', 'destinations'):
		params[k] = num = len(params[k].split(';'))
		distance = np.random.rand(num)
		# make coordinates span correctly: -180..180, -90..90
		locations = np.round((np.random.rand(num, 2) - 0.5) * np.array([360, 180]), 6)
		name = [None] * num
		result[k] = pd.DataFrame({'distance': distance * 500, 'name': name, 'location': [list(i) for i in locations]}).to_dict(orient='records')

	# make some distances/durations nan (make true/false matrix, use it in filter)
	na = np.random.rand(params['sources'], params['destinations']) < .03
	for k in ('distances', 'durations'):
		d = np.where(na, np.nan, np.random.rand(params['sources'], params['destinations']))
		result[k] = pd.DataFrame(d).values.tolist()

	m = mock.Mock(status_code=200)
	responses.append(result)
	m.json.return_value = result
	return m


@contextmanager
def make_server():
	with mock.patch('erde.op.table.get_retry', side_effect=_respond) as m, mock.patch.dict(table.CONFIG['routers'], {'foot': 'http://localhost:5001', 'local': 'http://localhost:5000'}):
		yield m


def test_route_chunk():
	h, s = _get_ds()
	# _route_chunk(data, host_url, annotations='duration', retries=10, extra_params=None)
	with make_server() as m:
		result = table._route_chunk((table._tolist(h), table._tolist(s), 0, 0), 'http://localhost:5000', 'duration,distance', 10, {'sample_param': 123})

	m.assert_called()
	assert 'sample_param' in call_params[-1]

	resp = responses[-1]

	# checking if sources/destinations in the response are mapped into the result table.
	# distance (from point to point snapped on edge) should be the same
	for k in ('sources', 'destinations'):
		resp_items = pd.DataFrame(resp[k]).join(result.set_index(k[:-1])[f'{k[:-1]}_snap'], rsuffix='_result')
		assert all(resp_items.distance == resp_items[f'{k[:-1]}_snap'])

	# make sure the response table is put in the result dataframe
	for k in ('distance', 'duration'):
		np.testing.assert_array_equal(
			result.pivot('source', 'destination', k).values,
			pd.DataFrame(resp[k + 's']).values
		)

@mock.patch.dict(table.CONFIG['routers'], {'foot': 'http://localhost:5001', 'local': 'http://localhost:5000'})
def test_response_errors():
	# error: the server responded with "coordinates invalid"
	pts = [Point(1000, 2000), Point(100, 50), Point(100000, 200000)]
	m = mock.MagicMock()
	m.return_value.status_code = 200
	m.return_value.json.return_value = {'message': 'Coordinates are invalid', 'code': 'InvalidOptions'}
	m.return_value.content = str(m.return_value.json.return_value)

	with mock.patch('erde.op.table.get_retry', m), pytest.raises(RuntimeError):
		list(table.table_route(pts, pts, 'local'))

	# error: 500 error
	err_resp = mock.Mock(
		headers={'Server': 'nginx/1.14.2', 'Date': 'Thu, 12 Aug 2021 07:43:56 GMT', 'Content-Type': 'text/html', 'Content-Length': '193', 'Connection': 'close'},
		status_code=414,
		content='<html>\r\n<head><title>414 Request-URI Too Large</title></head>\r\n<body bgcolor="white">\r\n<center><h1>414 Request-URI Too Large</h1></center>\r\n<hr><center>nginx/1.14.2</center>\r\n</body>\r\n</html>\r\n')

	with mock.patch('requests.get', return_value=err_resp) as m:
		with pytest.raises(RuntimeError):
			list(table.table_route(pts, pts, 'local'))

		# test bad annotations
		with pytest.raises(ValueError):
			list(table.table_route(pts, pts, 'local', annotations='wrong1,wrong2,distance'))



# test if max_table_size works
def test_max_table_size():
	h, s = _get_ds()

	total_table = len(h) * len(s)
	one_row = max(len(h), len(s))
	rows = min(len(h), len(s))
	# if mts > total_table, will be 1 request
	# if one_row*2 < mts < total_table, then several rows in 1 request
	# if one_row < mts < one_row*2, then 1 request per row
	# if mts < one_row, then each row will be split
	good_table = read_df('tests/table/result.csv')[['source', 'destination', 'distance', 'duration']]
	# source,destination,distance,duration,source_snap,destination_snap,geometry

	combos = (
		(total_table + 1, 1),
		(total_table // 2 + 1, 3),
		(2 * one_row + 1, rows),
		(one_row + 1, rows),
		(one_row // 2 + 1, rows * 2),
	)
	for i, (mts, check) in enumerate(combos):
		for a, b in ((h, s), (s, h)): # ?
			with make_server() as m:
				res = pd.concat(table.table_route(a, b, 'local', max_table_size=mts))

			assert m.call_count <= check
			if check > 1:
				assert m.call_count > 1
			assert len(res) == len(good_table)


def test_main():
	h, s = _get_ds()
	with make_server():
		result = pd.concat(table.main(h, s, 'local', threads=1, keep_columns='apartments,hid'))

	# check if apartments were joined correctly
	assert 'apartments' in result
	assert result['source'].map(h['apartments']).equals(result['apartments'])

	# calling with a non-existent column should raise CommandError
	import yaargh
	with make_server(), pytest.raises(yaargh.CommandError):
		list(table.main(h, s, 'local', threads=1, keep_columns='apartments,hid,nonexistentcolumn'))
