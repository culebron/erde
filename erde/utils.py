from shapely.geometry import LineString, Point
import geopandas as gpd
import pandas as pd


def transform(obj, crs_from, crs_to):
	"""Transforms obj (a shapely geometry) between CRS."""
	from pyproj import Transformer
	from shapely.ops import transform as transform_
	transformer = Transformer.from_crs(crs_from, crs_to, always_xy=True)
	return transform_(transformer.transform, obj)


def decode_poly(encoded_line):
	"""Decodes Google polyline format and reverses lat/lon to lon/lat coords. Used for routing with OSRM."""
	import polyline
	return LineString([(i[1], i[0]) for i in polyline.decode(encoded_line)])


def encode_poly(line):
	"""Reverses coords to lon/lat and encodes into Google polyline format. Used for routing with OSRM."""
	import polyline
	return polyline.encode([(i[1], i[0]) for i in line.coords])


def linestring_between(points1, points2):
	"""Connects 2 series of points with LineString. If both points1, and points2 are GeoSeries, both series must have same indice, and GeoSeries is returned.

	Parameters
	----------
	points1 : gpd.GeoSeries or iterable with __len__
		Points from which the linestring will begin.
	points2 : gpd.GeoSeries or iterable with __len__
		Points at which the linestring will end.

	Returns
	-------
	list with
	"""
	gs1 = isinstance(points1, gpd.GeoSeries)
	gs2 = isinstance(points2, gpd.GeoSeries)

	if gs1 and gs2:
		if not points1.index.equals(points2.index):
			raise ValueError("points1 and points2 GeoSeries must have same indice")
		return gpd.GeoSeries([LineString(i) for i in zip(points1, points2)], index=points1.index)

	if len(points1) != len(points2):
		raise ValueError('points1 and points2 must be the same length')
	return [LineString(i) for i in zip(points1, points2)]


def coslat(geom):
	"""Calculates latittude cosine coefficient for geoseries or a single geometry. If geometry type is not point, centroid is taken.

	If geom is a single geometry, we assume its CRS is 4326."""
	import numpy as np
	from shapely.geometry.base import BaseGeometry
	if not isinstance(geom, (gpd.GeoSeries, BaseGeometry)):
		raise TypeError(f'geom must be GeoSeries or BaseGeometry, got {geom.__class__} instead.')

	if isinstance(geom, BaseGeometry):
		v = transform(transform(geom, 4326, 3857).centroid, 3857, 4326)
	else:
		v = geom.to_crs(3857).centroid.to_crs(4326)

	return np.cos(np.radians(v.y))


def crossjoin(df1, df2, **kwargs):
	"""Shortcut for tables crossjoin (cartesian product)."""
	df1['_tmpkey'] = 1
	df2['_tmpkey'] = 1

	res = pd.merge(df1, df2, on='_tmpkey', **kwargs).drop('_tmpkey', axis=1)
	df1.drop('_tmpkey', axis=1, inplace=True)
	df2.drop('_tmpkey', axis=1, inplace=True)
	res.index = pd.MultiIndex.from_product((df1.index, df2.index))
	return res


def lonlat2gdf(df):
	"""Makes GeoDataFrame from a DataFrame that has lon/lat columns, or x/y. Columns may be lon/lat, lng/lat, long/lat, longitude/latitude, x/y, X/Y.
	"""
	if 'lon' in df and 'lat' in df:
		fn = lambda i: (i.lon, i.lat)
	elif 'lng' in df and 'lat' in df:
		fn = lambda i: (i.lng, i.lat)
	elif 'long' in df and 'lat' in df:
		fn = lambda i: (i.long, i.lat)
	elif 'longitude' in df and 'latitude' in df:
		fn = lambda i: (i.longitude, i.latitude)
	elif 'x' in df and 'y' in df:
		fn = lambda i: (i.x, i.y)
	elif 'X' in df and 'Y' in df:
		fn = lambda i: (i.X, i.Y)
	else:
		raise ValueError('Could not find appropriate columns. Possible combinations: lon/lat, lng/lat, long/lat, longitude/latitude, x/y, or X/Y')

	df['geometry'] = df.apply(lambda i: Point(*fn(i)), axis=1)
	return gpd.GeoDataFrame(df, crs=4326)


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
	from erde import dprint
	from time import sleep
	import requests

	for try_num in range(retries + 1):
		sleep(try_num)
		try:
			return requests.get(url, params=params, timeout=timeout)
		except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout):
			dprint('could not connect', end='')
			if try_num == retries - 1:
				raise

			dprint('retrying', try_num)


def lookup(left_df, right_df, column_names, left_on, right_on, suffixes=('', '_right'), how='left'):

	if isinstance(column_names, str):
		column_names = [column_names]

	def df_on(df, on_cols, side):
		if isinstance(on_cols, pd.Series):
			if not on_cols.index.equals(df.index):
				raise ValueError(f'{side}_on is a series and it\'s index is not equal to {side}_df index')

			return pd.DataFrame({'left_on': on_cols}), ['left_on']
		elif isinstance(on_cols, str):
			return df[[on_cols]], [on_cols]
		return df[on_cols], on_cols

	left_tmp, left_on_ = df_on(left_df, left_on, 'left')
	right_tmp, right_on_ = df_on(right_df, right_on, 'left')

	for k in column_names:
		right_tmp[k] = right_df[k]

	res = left_tmp.merge(right_tmp, left_on=left_on_, right_on=right_on_, how=how)
	res2 = res.groupby(left_on_).agg({k: 'first' for k in column_names}).reset_index()
	for k in column_names:
		left_df[k] = res2
