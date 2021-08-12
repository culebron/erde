import geopandas as gpd
from erde import CONFIG, dprint, utils, read_stream, write_stream, autocli
import sys


ANNOTATIONS = 'duration,distance'


def get_retry(url, params, retries=10, timeout=None):
	"""Requests any URL with GET params, with 10 retries.

	Parameters
	----------
	url : string
	params : dict
		GET parameters as dictionary. Values may be lists.
	retries : int
	timeout : float, optional
		Number of seconds to wait, may be less than 1.

	Returns
	-------
	requests.Response object
	"""
	from time import sleep
	import requests

	for try_num in range(retries):
		sleep(try_num)
		try:
			return requests.get(url, params=params, timeout=timeout)
		except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout):
			dprint('could not connect', end='')
			if try_num == retries - 1:
				raise

			dprint('retrying', try_num)

def raw_route(route, mode, retries=10, **params):
	"""Requests OSRM router and returns the raw response. Can be reused if you need response details.

	Parameters
	----------
	route : LineString
		Route with waypoints, can have 2 or more vertice.
	mode : str
		Name of router in Erde CONFIG['routers'] or URL to the router.
	retries : int, default 10
		Number of attemtps to make request. The delay before Nth attempt is N sec.
	**params : keyword arguments
		Parameters to URL (e.g. 'exclude').

	Returns
	-------
	dict
		Response JSON parsed as dictionary.

	"""
	host = CONFIG['routers'].get(mode, mode)
	params = {
		'overview': 'simplified',
		'alternatives': 'false',
		'steps': 'false',
		'geometries': 'polyline',
		'annotations': 'false',
		'generate_hints': 'false',
		**params
	}

	coordinates = ';'.join(f'{c[0]},{c[1]}' for c in route.coords)
	url = f'{host}/route/v1/driving/{coordinates}'
	resp = get_retry(url, params, retries)
	return resp.json()


def route_row(waypoints, mode, overview='simplified', alternatives=1, annotations=ANNOTATIONS, **params):
	"""Routes a row from dataframe or a LineString and outputs the path as a list of dicts that can be turned into GeoDataFrame.

	Parameters
	----------
	waypoints: LineString or pd.Series with 'geometry' key
		Points through which to route. If it's Series, then 'geometry' is taken from it, and the rest is treated as extra columns and copied to result items.
	mode : str
		Name of router in Erde CONFIG['routers'] or URL to the router.
	overview : str, {'simplified', 'full', 'no'}
		How detailed the response route line is. By default the geometry is simplified, roughness is proportional to the length of route.
	retries : int, default 10
		Number of attemtps to make request. The delay before Nth attempt is N sec.
	alternatives : int, default 1
		Number of alternative routes to return.
	annotations : string, default 'duration'.
		Additional metadata for each route coordinate. Possible values (may be multiple separated by comma): true, false, nodes, distance, duration, datasources, weight, speed.
	**params : keyword arguments
		Parameters to URL (e.g. 'exclude').

	Returns
	-------
	list of dicts
		Each list item is alternative route (by default there's 1), each dictionary contains the original extra items from waypoints, plus the main route data: duration (sec), geometry (LineString), distance (m), nodes (list of nodes) if annotations contain 'nodes'.

	"""
	from shapely.geometry import LineString
	from time import sleep
	import requests
	import pandas as pd

	metadata = waypoints.to_dict() if isinstance(waypoints, pd.Series) else {'geometry': waypoints}
	route_line = metadata.pop('geometry')

	try:
		sleep(0) # yield to other threads
		data = raw_route(route_line, mode, overview=overview, annotations=annotations, alternatives=alternatives, **params)
		sleep(0)

		result = []
		for alt, route in enumerate(data.get('routes', [])[:alternatives], start=1):

			route_result = {
				**metadata,
				'alternative': alt,
				'duration': route['duration'],
				'distance': route['distance'],
				'geometry': LineString(utils.decode_poly(route['geometry']))
			}

			if overview == 'full' and 'nodes' in annotations:
				nds = []
				for leg in route['legs']:
					n = leg['annotation']['nodes']
					# annotations always have start-end edges fully,
					# even when waypoint projects on a node (a corner), the edge before or after is repeated in adjacent legs
					nds.extend(n[2:] if n[:2] == nds[-2:] else n)
				route_result['nodes'] = nds
			result.append(route_result)
		sleep(0)
		return result
	except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.models.complexjson.JSONDecodeError):
		# if with all multiple retries things don't work, something is wrong,
		# no point to suppress the requests further
		print('Can\'t connect or decode JSON. Multiple retries were made, if they didn\'t help, there\'s a problem with network, URLs or requests rate (OSRM may stop responding if requested too often)', file=sys.stderr)
		raise
		# how to mark it as not-connected?
	except:
		print('Unhandled exception in the erde.op.route code with route:', route_line, file=sys.stderr)
		raise


@autocli
def main(input_data: read_stream, mode, overview='full', annotations=ANNOTATIONS, alternatives:int=1, threads:int=10) -> write_stream:
	import functools
	import itertools

	fn = functools.partial(route_row, mode=mode, overview=overview, annotations=annotations, alternatives=alternatives)
	rows = (r for i, r in input_data.iterrows())
	if threads == 1:
		result = map(fn, rows)
	else:
		from concurrent.futures import ThreadPoolExecutor
		with ThreadPoolExecutor(max_workers=threads) as tpe:
			result = tpe.map(fn, rows)

	return gpd.GeoDataFrame(itertools.chain(*result))
