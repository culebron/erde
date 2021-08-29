import geopandas as gpd


def _sj(left_df, right_df, left_on, right_on, op, how):
	left_tmp = _df_on(left_df, left_on, 'left')
	right_tmp = _df_on(right_df, right_on, 'right')
	return gpd.sjoin(left_tmp, right_tmp, op=op, how=how)


def sjfull(left_df, right_df, left_on='geometry', right_on='geometry', suffixes=('', '_right'), join='inner', op='intersects'):
	"""Full sjoin: makes sjoin by temporary geometries and puts both geometries in the resulting dataframe.

	Use left_on and right_on to join by, for instance, buffers, but have the original points in the result.

	Parameters
	----------
	left_df : GeoDataFrame
	right_df : GeoDataFrame
	left_on : str or GeoSeries, default 'geometry'
		Column in the left GeoDataFrame or a GeoSeries with the same index, by which to do spatial join. These are not added anywhere.
	right_on : str or GeoSeries, default 'geometry'
		Same in the right GeoDataFrame
	suffixes : 2-tuple of str, default ('', '_right')
		Suffixes added if colum names coincide, same as in pd.DataFrame.merge
	join : str, {'left', 'inner', 'right'}, default 'left'
		What kind of join to do.

		* 'inner' keeps only records with matches
		* 'left' keeps records from left_df if there's no right match
		* 'right' keeps records from right_df if there's no left match
	op : str, {'intersects', 'within', 'contains'}, default 'intersects'
		How geometries should match, e.g. left-contains-right.
	"""

	m = _sj(left_df, right_df, left_on, right_on, op, join).drop('geometry', axis=1).reset_index()
	index_left, index_right = 'index' + suffixes[0], 'index' + suffixes[1]
	m.rename(columns={'index': index_right if join == 'right' else index_left})
	m['geometry' + suffixes[0]] = m[index_left].map(left_df['geometry'])
	m['geometry' + suffixes[1]] = m[index_right].map(right_df['geometry'])
	return m


def sagg(left_df, right_df, agg, left_on='geometry', right_on='geometry', suffixes=('', '_right'), join='left', op='intersects'):
	"""Spatial aggregation. Aggregates the `right_df` attributes that spatially match `left_df`. E.g. if `left_df` is regions, and `right_df` is residential bulidings, this function can aggregate residents by regions:

		regions_with_residents = sagg(regions_gdf, buildings_gdf, {'residents': 'sum'})

	Parameters
	----------

	left_df : GeoDataFrame
		Main dataframe, by which to aggregate.
	right_df : GeoDataFrame
		What dataframe to aggregate.
	agg : dict
		What to aggregate, format is the same as in pd.DataFrame.agg or gpd.dissolve.
	left_on : str or GeoSeries, default 'geometry'
		Column in the left GeoDataFrame or a GeoSeries with the same index, by which to do spatial join. These are not added anywhere.
	right_on : str or GeoSeries, default 'geometry'
		Same in the right GeoDataFrame
	suffixes : 2-tuple of str, default ('', '_right')
		Suffixes added if colum names coincide, same as in pd.DataFrame.merge
	join : str, {'left' or 'inner'}, default 'left'
		What kind of join to do.
	op : str, {'intersects', 'within', 'contains'}, default 'intersects'
		How geometries should match, e.g. left-contains-right.

	Returns
	-------
	GeoDataFrame
		A new dataframe, same as `left_df`, but with aggregated columns from `agg` argument.
	"""

	if not isinstance(agg, dict):
		raise TypeError('agg argument must be a dict')

	if len(agg) == 0:
		raise ValueError('agg argument can\'t be empty')

	m = _sj(left_df, right_df, left_on, right_on, op, join)
	ind = m.index_right if 'join' != 'right' else m.index
	for k in agg.keys():  # we put the data columns here, because they may contain `geometry` (_right), which gets lost after sjoin.
		m[k] = ind.map(right_df[k])

	m2 = m.groupby(m.index).agg(agg)
	return left_df.join(m2, lsuffix=suffixes[0], rsuffix=suffixes[1], how=join)


def slookup(left_df, right_df, columns, left_on='geometry', right_on='geometry', suffixes=('', '_right'), join='left', op='intersects'):
	"""Spatial lookup. For each row in left_df finds the matching record in right_df and takes the required columns. E.g. for each business, find its region:

		business_plus_region = slookup(business_gdf, regions_gdf, 'name', suffixes=('', '_region'))

	or

		business_plus_region = slookup(business_gdf, regions_gdf, ['name', 'phone_code'], suffixes=('', '_region'))

	Since lookup may find multiple matching geometries of right_df, it takes the first one. GeoPandas sjoin usually keeps the order as in original dataframes, but it's not guaranteed.

	Parameters
	----------

	left_df : GeoDataFrame
		For what to look up.
	right_df : GeoDataFrame
		Where to look up.
	columns : str or iterable of str
		Name(s) of column(s) to lookup and add to the left_df.

	Other parameters are the same as in `sagg`.

	Returns
	-------
	GeoDataFrame
		A new dataframe, same as `left_df`, but also with looked up columns.
	"""

	if isinstance(columns, str):
		columns = [columns]

	return sagg(left_df, right_df, {k: 'first' for k in columns}, left_on, right_on, suffixes, join, op)


def sfilter(left_df, filter_geom, left_on='geometry', right_on='geometry', negative=False, op='intersects'):
	"""Filters left_df by geometries in right_df.

	Parameters
	----------
	left_df : GeoDataFrame
		What to filter.
	filter_geom : GeoDataFrame, GeoSeries, shapely geometry
		With what to filter.
	left_on : str or GeoSeries, default 'geometry'
		Column in the left GeoDataFrame or a GeoSeries with the same index, by which to do spatial join. Not added anywhere.

		For example, these can be buffers instead of original geometries (points), to filter by being within a distance.
	right_on : str or GeoSeries, default 'geometry'
		Same in the right GeoDataFrame
	negative : bool, default False
		Inverse filtering (keep those that don't match right_df geometries).
	op : str, {'intersects', 'within', 'contains'}, default 'intersects'
		How geometries should match, e.g. left-contains-right.

	Returns
	-------
	GeoDataFrame
		This is filtered `left_df` (a view of the original, not a copy).
	"""

	from shapely.geometry.base import BaseGeometry
	if not isinstance(filter_geom, (gpd.GeoDataFrame, gpd.GeoSeries, BaseGeometry)):
		raise TypeError(f'filter_geom should be GeoDataFrame, GeoSeries or shapely geometry, got {filter_geom.__class__} instead')

	if isinstance(filter_geom, BaseGeometry):
		filter_geom = gpd.GeoDataFrame({'geometry': [filter_geom]}, crs=left_df.crs)
	elif isinstance(filter_geom, gpd.GeoSeries):
		filter_geom = gpd.GeoDataFrame({'geometry': filter_geom})

	m = _sj(left_df, filter_geom, left_on, right_on, op, 'inner')
	isin = left_df.index.isin(m.index)
	if negative: isin = ~isin
	return left_df[isin]


def _df_on(df, geom, kind):
	"""Creates a temporary GeoDataFrame with same index and requested geometry (column name or GeoSeries), which is used in the other functions for sjoin."""
	if kind not in ('left', 'right'):
		raise ValueError("`kind` argument can be 'left' or 'right'")

	if isinstance(geom, gpd.GeoSeries):
		if not geom.index.equals(df.index):
			raise ValueError(f'{kind}_on GeoSeries index differs from that of {kind} dataframe')
		return gpd.GeoDataFrame({'geometry': geom}, index=df.index)

	if isinstance(geom, str):
		return df[[geom]]

	raise TypeError(f'{kind}_on argument must be either string, or GeoSeries')
