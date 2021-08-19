"""This script was made in jupyter notebook to "reverse-engineer"
the OSRM server, to be able to run tests of table.py.

The result function (`respond`) imitates the structure of the returned content, but the numbers are all random.

(To make it reply with meaningful numbers, we could record some coordinates and their snap distance and a table of distances between, but it would take a long code to find the suitable point for a new point coordinate -- because numbers are not precise, and it would require calculating difference and then sort/compare. That's why this function simply replies with random numbers, and then in the tests, we check if the table script correctly interprets them.)

The code is saved here to restore and be able to reuse it, for example, to make a module for GrahpHopper table service.
"""

from erde import read_df
from erde.op.route import get_retry
from erde.op.table import table_route
from unittest import mock
import json
import numpy as np
import pandas as pd
import re
import urllib.parse


if __name__ == '__main__':
	with open('request.txt') as f: req = f.read()
	with open('resp.json') as f: resp = json.load(f)

	sources = pd.DataFrame(resp['sources'])
	dest = pd.DataFrame(resp['destinations'])

	durations = pd.DataFrame(resp['durations'])
	distances = pd.DataFrame(resp['distances'])

	houses = read_df('houses.csv')
	shops = read_df('shops.csv')

	# I ran requests and checked responses[0] to see what the structure of the reply was.
	responses = [None]
	def capture_osrm_output(*args, **kwargs):
		r = get_retry(*args, **kwargs)
		responses[0] = r.json()
		return r

	with mock.patch('erde.op.table.get_retry', side_effect=capture_osrm_output) as mm:
		# here i called with a sample of houses and shops to make the response fit into a screen or two.
		result_table = list(table_route(houses[:10], shops[:2], 'local', annotations='duration,distance', max_table_size=100000))

	url = list(mm.call_args)[0][0]

	def respond(url, params=None, retries=None):
		match = re.match(r'^.*?polyline\((?P<polyline>.*?)\)\?(?P<qs>.*)$', url)
		params = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.unquote(match['qs'])).items()}

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
		m.json.return_value = result
		return m

	with mock.patch('erde.op.route.get_retry', side_effect=respond):
		req_params = table_route(url)

	print(req_params)
