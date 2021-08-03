import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

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


def coslat(geoseries):
	"""Calculates latittude cosine coefficient for geoseries. The geoseries argument may be any geometry type that has a centroid."""
	import numpy as np
	return geoseries.to_crs(3857).centroid.to_crs(4326).y.pipe(np.radians).pipe(np.cos)


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
