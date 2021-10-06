from concurrent.futures import ThreadPoolExecutor
from erde import CONFIG, autocli, write_stream, utils
from functools import partial
from itertools import product

from yaargh import CommandError
import geopandas as gpd
import pandas as pd
import urllib


def _tolist(data, name='sources'):
	"""Extracts list of Points from list/df with geometries/list, so that table_route could accept any kind of data."""
	msg = 'contains geometries that are not points'
	if isinstance(data, (list, tuple)):
		if any(i.geom_type != 'Point' for i in data):
			raise ValueError(f'{name} contains geometries other than Point')
		return list(data)
	if isinstance(data, gpd.GeoSeries):
		if any(data.geom_type != 'Point'):
			raise ValueError(name + ' ' + msg)
		return data.tolist()
	if isinstance(data, gpd.GeoDataFrame):
		if any(data.geom_type != 'Point'):
			raise ValueError(name + ' ' + msg)
		return data['geometry'].tolist()
	raise TypeError('type of points data was unrecognized')


def _index(data):
	"""Function to produce index from any input to table_route, or create one on the fly."""
	if isinstance(data, (pd.Series, pd.DataFrame)):
		return data.index
	else:
		return pd.RangeIndex(len(data))


def _route_chunk(data, host_url, annotations='duration', retries=10, extra_params=None):
	"""Table-routes a piece of table, makes a DataFrame of results. For internal use.

	Parameters
	----------
	data : tuple: (sources, destinations, sources_offset, destinations_offset)
		A tuple of chunk data. Passed as tuple to simplify `map` calls.
	host_url : string
		E.g. 'http://localhost:5000'
	annotations : string, {'duration', 'distance', 'duration,distance'}, default 'duration'.
	retries : int, default 10
		How many times to make requests to service on failure to connect.
	extra_params : dict, optional
		Additional params. See https://github.com/Project-OSRM/osrm-backend/blob/master/docs/http.md#table-service

	"""
	# offsets are used to make correct indice of the result dataframe
	from polyline import encode as encode_poly

	sources, destinations, sources_offset, destinations_offset = data
	sources_count = len(sources)
	destinations_count = len(destinations)

	# OSRM takes all points as one list, and then numbers of sources & dests in it
	all_points = sources + destinations
	encoded = encode_poly([(p.y, p.x) for p in all_points])

	# numerate sources  & dests. sources come first
	source_numbers = ';'.join(map(str, range(sources_count)))
	destination_numbers = ';'.join(map(str,
		range(sources_count, sources_count + destinations_count)))


	extra_params = extra_params or {}
	params = {
		'sources': source_numbers,
		'destinations': destination_numbers,
		'generate_hints': 'false',
		'annotations': annotations,
		**extra_params
	}

	encoded_params = urllib.parse.quote_plus(urllib.parse.urlencode(params))
	# if we pass url and params separately to requests.get, it will make a malformed URL
	encoded_url = f'{host_url}/table/v1/driving/polyline({encoded})?{encoded_params}'
	resp = utils.get_retry(encoded_url, {}, retries)

	if resp.status_code != 200:
		raise RuntimeError(f'OSRM server responded with {resp.status_code} code. Content: {resp.content}')

	resp_data = resp.json()
	if resp_data.get('code', 'Ok') != 'Ok':
		raise RuntimeError(f'OSRM server responded with error message: {resp_data["message"]}')

	# if 'duration' is requested, then take resp_data['durations'], or resp_data['distances'] if distances.
	# also, 'duration,distance' might be requested, then take both and concatenate results (= join columns)
	results = []
	
	for key in annotations.split(','):
		df = pd.DataFrame(resp_data[f'{key}s']).reset_index().rename(columns={'index': 'source'}).melt(id_vars='source', var_name='destination', value_name=key)
		df[key] = df[key].astype(float)
		if len(results) > 0:
			# only append the data column
			results.append(df[[key]])
		else:
			results.append(df)

	result_df = pd.concat(results, axis=1)

	# snapping distances
	result_df['source_snap'] = result_df.source.map(pd.DataFrame(resp_data['sources'])['distance'])
	result_df['destination_snap'] = result_df.destination.map(pd.DataFrame(resp_data['destinations'])['distance'])

	# instead of join/merge lookup
	result_df['geometry'] = result_df['source'].map({i: g for i, g in enumerate(sources)})
	result_df['geometry_dest'] = result_df['destination'].map({i: g for i, g in enumerate(destinations)})

	# shift back by the given offset
	result_df['destination'] = result_df['destination'].astype(int) + destinations_offset
	result_df['source'] = result_df['source'].astype(int) + sources_offset
	return result_df


def table_route(sources, destinations, router, max_table_size=2_000, threads=10, annotations='duration', pbar=True, cache_name=None, executor='process', extra_params=None):
	"""Makes table routes between 2 sets of points (between all pairs of them), splitting requests more or less optimally to fit into max-table-size parameter.

	OSRM may set an arbitrary limit on how many cells the table can have, and deny larger requests. With smaller `max_table_size`, table will be split into more smaller requests, and then the results will be concatenated. If possible, set it on the server to 100_000, this will work much faster.

	If sources/destinations have indice, the resulting dataframe will have them too.

	Parameters
	----------
	sources : list, gpd.GeoSeries or gpd.GeoDataFrame
		Starting points.
	destination : list, gpd.GeoSeries or gpd.GeoDataFrame
		End points of trips.
	router : string
		name of routing router in the config, or URL
	max_table_size : int, default 2_000
		maximum number of sources*destinations in a single request.
	threads : int, default 10
		Number of threads

	Yields
	------
	DataFrame
		Data frame, where each row is pair of source & destination. Columns are duration, distance (if requested in `annotations`), source_snap and destination_snap (distances from requested coordinates and the nearest graph edge).
	"""
	from tqdm import tqdm
	import re
	
	if router not in CONFIG['routers'] and not re.match(r'^https?\://.*', router):
		raise ValueError(f'router must be a key in erde config routers section, or a URL. got: \'{router}\'')

	sources_indices = {i: v for i, v in enumerate(_index(sources))}
	destinations_indices = {i: v for i, v in enumerate(_index(destinations))}
	sources = _tolist(sources, 'sources')
	destinations = _tolist(destinations, 'destinations')

	ann_set = set(annotations.split(','))
	if ann_set & {'duration', 'distance'} != ann_set:
		raise ValueError("annotations must be one of these: 'duration', 'distance', or 'duration,distance' (order does not matter)")

	mts = max_table_size
	host_url = CONFIG['routers'].get(router, router)

	total_rows, total_cols = rows, cols = len(sources), len(destinations)
	if cols * rows > mts:
		if rows < cols:
			# split by sources
			rows = max(mts // cols, 1)  # max(,1) beacuse if 1 row does not fit, then at least split by 1 row
			cols = min(mts, cols)
		else:
			cols = max(mts // rows, 1)
			rows = min(mts, rows)

	_route_partial = partial(_route_chunk, host_url=host_url, annotations=annotations, extra_params=extra_params)

	with tqdm(total=total_rows * total_cols, desc='Table routing', disable=(not pbar)) as t, ThreadPoolExecutor(max_workers=threads) as tpe:
		combos = list(product(range(0, total_rows, rows), range(0, total_cols, cols)))
		slices = ((sources[s:s + rows], destinations[d:d + cols], s, d) for s, d in combos)

		# process/thread/an instance of executor
		for df in tpe.map(_route_partial, slices):
			df['source'] = df['source'].map(sources_indices)
			df['destination'] = df['destination'].map(destinations_indices)
			yield df
			t.update(len(df))


@autocli
def main(sources: gpd.GeoDataFrame, destinations: gpd.GeoDataFrame, router, annotations='duration', threads: int = 10, mts: int = 2000, keep_columns=None) -> write_stream:
	"""Makes table route requests between sources and destinations. Outputs the result as a GDF with LineString between each pair.

	Parameters
	----------
	sources : GeoDataFrame
		Points of sources.
	destinations : GeoDataFrame
		Points of destinations.
	router : string
		Name of router in Erde config, or URL (host + port).
	threads : int, default 10
		Number of threads to run and process requests.
	mts : int, default 2000
		Max table size, i.e. len(sources) * len(destinations). OSRM server handles only requests smaller than a particular amount (set in osrm-routed CLI options), and with this setting requests are split into many.
	keep_columns : string
		Comma-separated names of columns to take from sources & destinations GeoDataFrames and put into the result.

	Yields
	------
	GeoDataFrame

	"""
	t = table_route(sources['geometry'], destinations['geometry'], router, annotations=annotations, max_table_size=mts, threads=threads)

	if keep_columns is not None:
		keep_columns = keep_columns.split(',')
		for k in keep_columns:
			if k not in sources and k not in destinations:
				raise CommandError(f'column {k} not present in sources, nor in destinations. Available columns are: sources: {", ".join(list(sources))}, destinations: {", ".join(list(sources))}')
		sub_sources = sources[[k for k in keep_columns if k in sources]]
		sub_destinations = destinations[[k for k in keep_columns if k in destinations]]

	for df in t:
		df['geometry'] = utils.linestring_between(df.geometry, df.geometry_dest)
		df.drop('geometry_dest', axis=1, inplace=True)
		if keep_columns is not None:
			df = df.merge(sub_sources, left_on='source', right_index=True, suffixes=('', '_source'))
			df = df.merge(sub_destinations, left_on='destination', right_index=True, suffixes=('', '_dest'))

		yield gpd.GeoDataFrame(df, crs=4326)
