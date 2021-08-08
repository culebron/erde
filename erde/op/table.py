from erde import CONFIG, autocli, write_stream, utils
from erde.op.route import get_retry
from itertools import product
from polyline import encode as encode_poly
from tqdm import tqdm
from yaargh import CommandError
import geopandas as gpd
import pandas as pd
import urllib


def _tolist(data, name='sources'):
	msg = 'dataframe contains geometries that are not points'
	if isinstance(data, gpd.GeoSeries):
		if any(data.geom_type != 'Point'):
			raise ValueError(name + ' ' + msg)
		data = data.tolist()
	elif isinstance(data, gpd.GeoDataFrame):
		if any(data.geom_type != 'Point'):
			raise ValueError(name + ' ' + msg)
		data = data['geometry'].tolist()
	elif isinstance(data, pd.DataFrame):
		raise ValueError(f'{name} should not be pd.DataFrame')
	return data


def _index(data):
	if isinstance(data, (pd.Series, pd.DataFrame)):
		return data.index
	else:
		return pd.RangeIndex(len(data))


#@threader.catch_traceback
def route_chunk(data, host_url, result_type='duration', retries_limit=10, extra_params=None):
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
		'annotations': result_type,
		**extra_params
	}

	encoded_params = urllib.parse.quote_plus(urllib.parse.urlencode(params))
	encoded_url = f'{host_url}/table/v1/driving/polyline({encoded})?{encoded_params}'
	resp_data = get_retry(encoded_url).json()

	# if 'duration' is requested, then take resp_data['durations'], or resp_data['distances'] if distances.
	# also, 'duration,distance' might be requested, then take both and concatenate results (= join columns)
	results = []
	
	for key in result_type.split(','):
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


def table_route(sources, destinations, mode, max_table_size=10000, threads=10, result_type='duration', pbar=True, cache_name=None, executor='process', extra_params=None):
	sources_indices = {i: v for i, v in enumerate(_index(sources))}
	destinations_indices = {i: v for i, v in enumerate(_index(destinations))}
	sources = _tolist(sources, 'sources')
	destinations = _tolist(destinations, 'destinations')

	if result_type not in ('duration', 'distance', 'duration,distance'):
		raise ValueError("result_type must be one of these: 'duration', 'distance', 'duration,distance'")

	mts = max_table_size
	host_url = CONFIG['routers'].get(mode, mode)

	total_rows, total_cols = rows, cols = len(sources), len(destinations)
	if cols * rows > mts:
		if rows < cols:
			# split by sources
			rows = max(mts // cols, 1)  # max(,1) beacuse if 1 row does not fit, then at least split by 1 row
			cols = min(mts, cols)
		else:
			cols = max(mts // rows, 1)
			rows = min(mts, rows)

	with tqdm(total=total_rows * total_cols, desc='Table routing', disable=(not pbar)) as t:
		combos = list(product(range(0, total_rows, rows), range(0, total_cols, cols)))
		slices = [(sources[s:s + rows], destinations[d:d + cols], s, d) for s, d in combos]

		# process/thread/an instance of executor
		for df in map(route_chunk, slices, host_url=host_url, result_type=result_type, max_workers=threads, pbar=False, extra_params=extra_params):
			df['source'] = df['source'].map(sources_indices)
			df['destination'] = df['destination'].map(destinations_indices)
			yield df
			t.update(len(df))


@autocli
def main(sources: gpd.GeoDataFrame, destinations: gpd.GeoDataFrame, mode, output_path, threads: int = 10, mts: int = 10000, keep_columns=None) -> write_stream:
	t = table_route(sources['geometry'], destinations['geometry'], mode, max_table_size=mts, threads=threads)

	if keep_columns is not None:
		keep_columns = keep_columns.split(',')
		for k in keep_columns:
			if k not in sources and k not in destinations:
				raise CommandError(f'column {k} not present in sources, nor in destinations. Available columns are: sources: {", ".join(list(sources))}, destinations: {", ".join(list(sources))}')
		sub_sources = sources[[k for k in keep_columns if k in sources]]
		sub_destinations = destinations[[k for k in keep_columns if k in destinations]]

	for df in t:
		df = utils.linestring_between(df.geometry, df.geometry_dest).drop('geometry_dest', axis=1)
		if keep_columns is not None:
			df = df.merge(sub_sources, left_on='source', right_index=True, suffixes=('', '_source'))
			df = df.merge(sub_destinations, left_on='destination', right_index=True, suffixes=('', '_dest'))

		yield gpd.GeoDataFrame(df, crs=4326)
