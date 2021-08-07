from contextlib import contextmanager
from erde import utils
from erde.op import route
from shapely.geometry import LineString
from unittest import mock
import json
import pytest
import requests


def test_get_retry():
	# normal functioning
	called_urls = []
	def new_get(url, *args, **kwargs):
		called_urls.append(url)
		return len(called_urls) - 1

	results = []
	requested_urls = []
	with mock.patch('requests.get', side_effect=new_get) as mm:
		for i in range(10):
			url = f'http://localhost/{i}'
			requested_urls.append(url)
			results.append(route.get_retry(url, {}))

	assert mm.call_count == 10
	assert called_urls == requested_urls

	# connection timeout once
	ok = 'normal response'
	url = 'http://localhost/123'

	resps = [requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, ok]

	def err(url, params, *args, **kwargs):
		resp = resps.pop(0)
		if isinstance(resp, type) and issubclass(resp, Exception):
			raise resp('planned exception')
		return resp

	# 10 retries by default, should not raise exception
	with mock.patch('requests.get', side_effect=err) as mm:
		assert route.get_retry(url, {}) == ok

	assert mm.call_count == 3

	resps = [requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, ok]
	# connection timeout exhausts retries
	with mock.patch('requests.get', side_effect=err) as mm:
		with pytest.raises(requests.exceptions.ConnectionError):
			route.get_retry(url, {}, retries=1)


resp1 = """{"code":"Ok","waypoints":[{"hint":"smeFjLdnhYweAAAAFgAAAEsAAABDAAAAZThoQGUAKkAEcQ9B5cAAQR0AAAAVAAAASAAAAEEAAAAlAAAAIQn0BNujRAMiCfQE3KNEAwIAvxOOrKTY","distance":0.128545,"location":[83.101985,54.830043],"name":""},{"hint":"umeFjFlohYxfAAAAdQIAAAAAAAATAgAA9XX0QE9XSUIAAAAAUTsqQjwAAACTAQAAAAAAAFQBAAAlAAAA_w70BC-mRAMWD_QELaZEAwAAfwCOrKTY","distance":1.494989,"location":[83.103487,54.830639],"name":""}],"routes":[{"legs":[{"steps":[],"weight":11.49,"distance":133.5,"summary":"","duration":106.8}],"weight_name":"routability","geometry":"w~smImzezNCDEDGASWKQKUEO[}AGo@Ai@?Y?OMC","weight":11.49,"distance":133.5,"duration":106.8}]}"""

resp2 = """{"code":"Ok","waypoints":[{"hint":"smeFjLdnhYweAAAAFgAAAEsAAABDAAAAZThoQGUAKkAEcQ9B5cAAQR0AAAAVAAAASAAAAEEAAAAlAAAAIQn0BNujRAMiCfQE3KNEAwIAvxOOrKTY","distance":0.128545,"location":[83.101985,54.830043],"name":""},{"hint":"umeFjFlohYxfAAAAdQIAAAAAAAATAgAA9XX0QE9XSUIAAAAAUTsqQjwAAACTAQAAAAAAAFQBAAAlAAAA_w70BC-mRAMWD_QELaZEAwAAfwCOrKTY","distance":1.494989,"location":[83.103487,54.830639],"name":""}],"routes":[{"legs":[{"steps":[],"weight":11.49,"distance":133.5,"annotation":{"nodes":[3395030499,3395030501,5179019511,3395030504,5179019510,3395030505,5179019509,3395030506,3395030507,6945546983,3395030511,3395030510,3395030509,3395030512]},"summary":"","duration":106.8}],"weight_name":"routability","geometry":"w~smImzezNCDEDGASWKQKUEO[}AGo@Ai@?Y?OMC","weight":11.49,"distance":133.5,"duration":106.8}]}"""

resp3 = """{"code":"Ok","waypoints":[{"hint":"rWeFjLBnhYzaAAAADAAAAAAAAADKAAAA_07SQTHFrD8AAAAAXaPCQdEAAAALAAAAAAAAAMIAAAAlAAAAOgv0BOajRAM6C_QEyaNEAwAA3xCOrKTY","distance":3.228324,"location":[83.102522,54.830054],"name":""},{"hint":"tmeFjLlnhYwvAAAA8AAAADsBAABrAQAA1Mm1QBcw50FdexdC2vgrQi0AAADnAAAALgEAAFgBAAAlAAAA0Qr0BD-lRAO6CvQEWKVEAwQA3wOOrKTY","distance":3.151305,"location":[83.102417,54.830399],"name":""},{"hint":"umeFjFlohYxfAAAAdQIAAAAAAAATAgAA9XX0QE9XSUIAAAAAUTsqQjwAAACTAQAAAAAAAFQBAAAlAAAA_w70BC-mRAMWD_QELaZEAwAAfwCOrKTY","distance":1.494989,"location":[83.103487,54.830639],"name":""}],"routes":[{"legs":[{"steps":[],"weight":7.7,"distance":92.3,"annotation":{"nodes":[3395030496,3395030495,4347826068,3395030497,5179019505,3395030499,3395030501,5179019511,3395030504,5179019510,3395030505,5179019509,3395030506,3395030507]},"summary":"","duration":73.9},{"steps":[],"weight":6.98,"distance":79.3,"annotation":{"nodes":[3395030506,3395030507,6945546983,3395030511,3395030510,3395030509,3395030512]},"summary":"","duration":63.5}],"weight_name":"routability","geometry":"y~smIw}ezN?BBXFl@?LEFGLEDGASWKQKUEOCOWmAGo@Ai@?Y?OMC","weight":14.68,"distance":171.6,"duration":137.4}]}"""

@contextmanager
def _sample_data(raw_resp):
	resp_json = json.loads(raw_resp)
	req_line = LineString([[83.1019860,54.8300436], [83.1035095,54.8306369]])

	m = mock.Mock()
	m.return_value.json.return_value = resp_json

	with mock.patch('erde.op.route.get_retry', m):
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
		}

		data = route.raw_route(sample_line, u, overview='full', steps=True)

	assert data == resp_json
	m.assert_called_with(requested_url, params, 10)


def test_single_route():
	# simple response with duration, distance, geometry
	with _sample_data(resp1) as (sample_line, resp_json, m):
		resp_geometry = utils.decode_poly(resp_json['routes'][0]['geometry'])
		data = route.single_route(sample_line, 'http://localhost')
		assert set(data) == {'distance', 'duration', 'geometry'}
		assert data['geometry'] == resp_geometry

	# more complex, with nodes
	with _sample_data(resp2) as (sample_line, resp_json, m):
		data = route.single_route(sample_line, 'http://localhost', overview='full', annotations=['nodes'])
		assert set(data) == {'distance', 'duration', 'geometry', 'nodes'}
		assert data['geometry'] == resp_geometry
		assert data['nodes'] == resp_json['routes'][0]['legs'][0]['annotation']['nodes']

	# nodes in more than one leg
	# (requested line: LineString([[83.1025224,54.8300251], [83.1023937,54.8304237], [83.10351,54.830637]]))
	# (but here it's unimportant, we mock the http request)
	with _sample_data(resp3) as (sample_line, resp_json, m):
		data = route.single_route(sample_line, 'http://localhost', overview='full', annotations=['nodes'])

		assert set(data) == {'distance', 'duration', 'geometry', 'nodes'}

		legs = resp_json['routes'][0]['legs']
		nodes = []
		for leg in legs:
			n = leg['annotation']['nodes']
			if n[:2] == nodes[-2:]:
				nodes.extend(n[2:])
			elif n[:1] == nodes[-1:]:
				nodes.extend(n[1:])
			else:
				nodes.extend(n)

		assert data['nodes'] == nodes

	# requests raises ConnectionTimeout after retries
	pass

	# some inner function raises an exception => raise it
	pass
