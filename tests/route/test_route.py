from contextlib import contextmanager
from erde import utils, read_df
from erde.op import route
from shapely.geometry import LineString
from unittest import mock
from requests.exceptions import ConnectTimeout, ConnectionError
import json
import pytest
import requests


def test_get_retry():
	# normal functioning
	called_urls = []
	def new_get(url, *args, **kwargs):
		called_urls.append(url)
		return len(called_urls) - 1

	requested_urls = []
	with mock.patch('requests.get', side_effect=new_get) as mm:
		for i in range(10):
			url = f'http://localhost/{i}'
			requested_urls.append(url)
			utils.get_retry(url, {})

	assert mm.call_count == 10
	assert called_urls == requested_urls

	# connection timeout once
	ok = 'normal response'
	url = 'http://localhost/123'

	resps = [ConnectTimeout, ConnectionError, ok]

	def err(url, params, *args, **kwargs):
		resp = resps.pop(0)
		if isinstance(resp, type) and issubclass(resp, Exception):
			raise resp('planned exception')
		return resp

	# 10 retries by default, should not raise exception
	with mock.patch('requests.get', side_effect=err) as mm:
		assert utils.get_retry(url, {}) == ok

	assert mm.call_count == 3

	resps = [requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, ok]
	# connection timeout exhausts retries
	with mock.patch('requests.get', side_effect=err) as mm:
		with pytest.raises(requests.exceptions.ConnectionError):
			utils.get_retry(url, {}, retries=1)


resp1 = """{"code":"Ok","waypoints":[{"distance":0.128545,"location":[83.101985,54.830043],"name":""},{"distance":1.494989,"location":[83.103487,54.830639],"name":""}],"routes":[{"legs":[{"steps":[],"weight":11.49,"distance":133.5,"summary":"","duration":106.8}],"weight_name":"routability","geometry":"w~smImzezNCDEDGASWKQKUEO[}AGo@Ai@?Y?OMC","weight":11.49,"distance":133.5,"duration":106.8}]}"""

resp2 = """{"code":"Ok","waypoints":[{"distance":0.128545,"location":[83.101985,54.830043],"name":""},{"distance":1.494989,"location":[83.103487,54.830639],"name":""}],"routes":[{"legs":[{"steps":[],"weight":11.49,"distance":133.5,"annotation":{"nodes":[3395030499,3395030501,5179019511,3395030504,5179019510,3395030505,5179019509,3395030506,3395030507,6945546983,3395030511,3395030510,3395030509,3395030512]},"summary":"","duration":106.8}],"weight_name":"routability","geometry":"w~smImzezNCDEDGASWKQKUEO[}AGo@Ai@?Y?OMC","weight":11.49,"distance":133.5,"duration":106.8}]}"""

resp3 = """{"code":"Ok","waypoints":[{"distance":3.228324,"location":[83.102522,54.830054],"name":""},{"distance":7.458541,"location":[83.102759,54.830055],"name":""},{"distance":10.607949,"location":[83.103139,54.830115],"name":""},{"distance":4.202972,"location":[83.103442,54.830519],"name":""}],"routes":[{"legs":[{"steps":[],"weight":1.26,"distance":15.2,"annotation":{"nodes":[3395030495,3395030496]},"summary":"","duration":12.1},{"steps":[],"weight":2.16,"distance":25.9,"annotation":{"nodes":[3395030495,3395030496,4347826071,4347826070]},"summary":"","duration":20.7},{"steps":[],"weight":4.79,"distance":56.6,"annotation":{"nodes":[4347826071,4347826070,5179019507,5179019506,3395030509]},"summary":"","duration":45.4}],"weight_name":"routability","geometry":"y~smIw}ezNAo@?a@Ki@??@_@a@Ia@IMG","weight":8.21,"distance":97.7,"duration":78.2}]}"""

resp4 = """{"message":"Impossible route between points","code":"NoRoute"}"""

@contextmanager
def _sample_data(raw_resp):
	resp_json = json.loads(raw_resp)
	req_line = LineString([[83.1019860,54.8300436], [83.1035095,54.8306369]])

	m = mock.Mock()
	m.return_value.json.return_value = resp_json

	with mock.patch('erde.utils.get_retry', m):
		yield req_line, resp_json, m


def test_raw_route():
	with _sample_data(resp1) as (sample_line, resp_json, m):

		u = 'http://localhost'

		url_coords = ';'.join(f'{c[0]},{c[1]}' for c in sample_line.coords)
		requested_url = f'{u}/route/v1/driving/{url_coords}'
		params = {
			'overview': 'full',
			'alternatives': 'false',
			'steps': True,
			'geometries': 'polyline',
			'annotations': 'false',
			'generate_hints': 'false'
		}

		data = route.raw_route(sample_line, u, overview='full', steps=True)

	assert data == resp_json
	m.assert_called_with(requested_url, params, 10)


def test_route_row():
	# simple response with duration, distance, geometry
	with _sample_data(resp1) as (sample_line, resp_json, m):
		resp_geometry = utils.decode_poly(resp_json['routes'][0]['geometry'])
		data = route.route_row(sample_line, 'http://localhost')[0]
		assert set(data) == {'distance', 'duration', 'geometry', 'alternative'}
		assert data['geometry'] == resp_geometry

	# more complex, with nodes
	with _sample_data(resp2) as (sample_line, resp_json, m):
		data = route.route_row(sample_line, 'http://localhost', overview='full', annotations=['nodes'])[0]
		assert set(data) == {'distance', 'duration', 'geometry', 'nodes', 'alternative'}
		assert data['geometry'] == resp_geometry
		assert data['nodes'] == resp_json['routes'][0]['legs'][0]['annotation']['nodes']

	# nodes in more than one leg
	# (requested line: LineString([[83.1025224,54.8300251], [83.1023937,54.8304237], [83.10351,54.830637]]))
	# (but here it's unimportant, we mock the http request)
	with _sample_data(resp3) as (sample_line, resp_json, m):
		data = route.route_row(sample_line, 'http://localhost', overview='full', annotations=['nodes'])[0]

		assert set(data) == {'distance', 'duration', 'geometry', 'nodes', 'alternative'}

		legs = resp_json['routes'][0]['legs']
		nodes = []
		for leg in legs:
			n = leg['annotation']['nodes']
			nodes.extend(n[2:] if n[:2] == nodes[-2:] else n)

		assert data['nodes'] == nodes

		# test if nodes aren't missing
		node_ids = set(n for leg in legs for n in leg['annotation']['nodes'])
		assert node_ids == set(data['nodes'])

	# no route => empty list
	with _sample_data(resp4) as (sr, rj, m):
		resp = route.route_row(sample_line, 'http://localhost')
		assert resp == []

	# requests raises ConnectionTimeout after retries
	# or some inner function raises an exception => raise it
	for exc in (ConnectTimeout, ValueError):
		with mock.patch('erde.utils.get_retry', side_effect=exc), pytest.raises(exc):
			data = route.route_row(sample_line, 'http://localhost', overview='full', annotations=['nodes'])

responses = (
	{"code":"Ok","waypoints":[{"distance":3.61982,"location":[83.102203,54.829845],"name":"улица Мальцева"},{"distance":3.234077,"location":[83.102698,54.829823],"name":"улица Мальцева"}],"routes":[{"legs":[{"steps":[],"weight":3.21,"distance":32.1,"annotation":{"distance":[5.790045,12.309395,13.982567],"duration":[4.6,9.8,11.3]},"summary":"","duration":25.7}],"weight_name":"routability","geometry":"q}smIw{ezNBQ@e@?k@","weight":3.21,"distance":32.1,"duration":25.7}]},
	{"code":"Ok","waypoints":[{"distance":3.060921,"location":[83.102119,54.830271],"name":""},{"distance":2.678665,"location":[83.103208,54.830569],"name":""}],"routes":[{"legs":[{"steps":[],"weight":6.57,"distance":78.8,"annotation":{"distance":[2.645842,9.431852,6.274194,34.485784,15.575858,10.416756],"duration":[2.2,7.5,5,27.6,12.5,8.3]},"summary":"","duration":63.1}],"weight_name":"routability","geometry":"e`tmIg{ezNCEKUEO[}AGo@A_@","weight":6.57,"distance":78.8,"duration":63.1}]},
	{"code":"Ok","waypoints":[{"distance":3.536724,"location":[83.102564,54.830445],"name":""},{"distance":3.119652,"location":[83.102762,54.829825],"name":"улица Мальцева"}],"routes":[{"legs":[{"steps":[],"weight":12.32,"distance":138.1,"annotation":{"distance":[16.383175,6.274194,9.431852,8.399479,13.690647,3.956195,4.082331,6.271828,4.200657,4.741245,11.970948,4.059346,4.946488,9.329355,12.309395,18.088827],"duration":[13.1,5,7.5,6.7,11,3.2,3.3,5,3.4,3.8,9.6,3.2,4,7.5,9.8,14.5]},"summary":"","duration":110.6}],"weight_name":"routability","geometry":"iatmI_~ezNLj@DNJTJPRVF@DEFMDG?MPTBKDKB[@e@Aw@","weight":12.32,"distance":138.1,"duration":110.6},{"legs":[{"steps":[],"weight":15.82,"distance":176.9,"annotation":{"distance":[18.102699,15.575858,13.819011,8.139413,5.322123,14.734217,19.222356,18.487972,19.233995,4.472466,6.946144,24.312235,8.532533],"duration":[14.5,12.5,11.1,6.5,4.3,11.8,15.4,14.8,15.4,3.6,5.6,19.4,6.8]},"summary":"","duration":141.7}],"weight_name":"routability","geometry":"iatmI_~ezNMq@Go@Ai@?Y?OVL`@H`@H`@JFCJA@hA?Z","weight":15.82,"distance":176.9,"duration":141.7}]},
	{"code":"Ok","waypoints":[{"distance":1.729412,"location":[83.101979,54.830047],"name":""},{"distance":2.502576,"location":[83.103443,54.830521],"name":""}],"routes":[{"legs":[{"steps":[],"weight":10.98,"distance":131.1,"annotation":{"distance":[2.064474,4.082331,3.956195,13.690647,8.399479,9.431852,6.274194,34.485784,15.575858,13.819011,8.139413,5.322123,5.843467],"duration":[1.7,3.3,3.2,11,6.7,7.5,5,27.6,12.5,11.1,6.5,4.3,4.6]},"summary":"","duration":105}],"weight_name":"routability","geometry":"y~smIkzezNABEDGASWKQKUEO[}AGo@Ai@?Y?OHD","weight":10.98,"distance":131.1,"duration":105},{"legs":[{"steps":[],"weight":11.47,"distance":136.7,"annotation":{"distance":[4.207601,4.200657,4.741245,15.085998,9.170909,27.548927,14.903542,10.260316,18.487972,19.222356,8.890766],"duration":[3.3,3.4,3.8,12.1,7.3,22,11.9,8.2,14.8,15.4,7.2]},"summary":"","duration":109.4}],"weight_name":"routability","geometry":"y~smIkzezNDIDG?MGm@CYAuAKi@@_@a@Ia@IMG","weight":11.47,"distance":136.7,"duration":109.4}]}
)


def test_main():
	input_df = read_df('tests/route/multiple-routes.csv')
	expected_df = read_df('tests/route/routes-result.csv').set_index(['r_id', 'alternative'])

	m = mock.Mock(side_effect=responses)
	with mock.patch('erde.op.route.raw_route', m):
		result_df = route.main(input_df, 'foot', alternatives=3, threads=1).set_index(['r_id', 'alternative'])

	assert all(expected_df['geometry'].geom_almost_equals(result_df['geometry']))

	tpe = mock.MagicMock()
	tpe.return_value.__enter__.return_value.map = map
	m = mock.Mock(side_effect=responses)
	with mock.patch('erde.op.route.raw_route', m), mock.patch('concurrent.futures.ThreadPoolExecutor', tpe):
		result_df = route.main(input_df, 'foot', alternatives=3)

	result_df.set_index(['r_id', 'alternative'], inplace=True)
	assert all(expected_df['geometry'].geom_almost_equals(result_df['geometry']))

