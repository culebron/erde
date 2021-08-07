import geopandas as gpd
from erde import CONFIG, dprint, utils, read_stream, write_stream, autocli
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
			resp = requests.get(url, params=params, timeout=timeout)
			return resp
		except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.models.complexjson.JSONDecodeError):
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
		**params
	}

	coordinates = ';'.join(f'{c[0]},{c[1]}' for c in route.coords)
	url = f'{host}/route/v1/driving/{coordinates}'
	resp = get_retry(url, params, retries)
	return resp.json()


def single_route(route_line, mode, overview='simplified', alternatives=1, annotations=ANNOTATIONS, **params):
	"""Routes a single route line and transforms the path into LineString.

	Parameters
	----------
	Route with waypoints, can have 2 or more vertice.
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
	dict
		Dictionary with main response data: duration (sec), geometry (LineString), distance (m), nodes (list of nodes) if annotations contain 'nodes'.

	"""
	from shapely.geometry.base import EmptyGeometry
	from shapely.geometry import LineString
	from time import sleep
	import requests
	try:
		sleep(0)
		data = raw_route(route_line, mode, overview=overview, annotations=annotations, **params)
		sleep(0)

		if 'routes' not in data:
			return {'duration': None, 'distance': None, 'geometry': EmptyGeometry(), 'nodes': []}

		if len(data['routes']) > 0:
			result = []
			for route in data['routes'][:alternatives]:
				route_result = {
					'duration': route['duration'],
					'distance': route['distance'],
					'geometry': LineString(utils.decode_poly(route['geometry']))
				}

				if overview == 'full' and 'nodes' in annotations:
					nds = []
					for leg in route['legs']:
						n = leg['annotation']['nodes']
						if n[:2] == nds[-2:]:
							nds.extend(n[2:])
						elif n[:1] == nds[-1:]:
							nds.extend(n[1:])
						else:
							nds.extend(n)
					route_result['nodes'] = nds
				result.append(route_result)

			if alternatives == 1:
				return result[0]

			return result
	except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.models.complexjson.JSONDecodeError):
		return {'duration': None, 'distance': None, 'geometry': EmptyGeometry(), 'nodes': []}  # empty row
	except:
		print(route_line)
		raise

@autocli
def main(input_data: read_stream, mode, overview='full', annotations=ANNOTATIONS, threads:int=10) -> write_stream:
	from concurrent.futures import ThreadPoolExecutor
	import functools

	fn = functools.partial(single_route, mode=mode, overview=overview, annotations=annotations)
	if threads == 1:
		result = map(fn, input_data['geometry'].values)
	else:
		with ThreadPoolExecutor(max_workers=threads) as tpe:
			result = tpe.map(fn, input_data['geometry'].values)

	gdf = gpd.GeoDataFrame(result, index=input_data.index)
	for k in gdf:
		input_data[k] = gdf[k]

	return input_data
