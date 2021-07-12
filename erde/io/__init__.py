from shapely import wkt, wkb
import fiona
import geopandas as gpd
import os
import pandas as pd
import re
import sys


FORMATS = {
	'csv': r'^.*\.(csv|txt)$',
	'geojson': r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>geojson))$',
	'shp': r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>shp))$',
	'geojsonseq': r'^(?P<file_path>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>geojsonl\.json|geojsonl))$',
	'gpkg': r'^(?P<file_path>(?:.*/)?(?P<file_name>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>gpkg)))(?:\:(?P<layer_name>[a-z0-9_-]+))?$',
	'postgresql': r'^(?P<engine>postgresql\://(?:(?P<user>[^:\/?#\s]+)(?:\:(?P<password>[^:\/?#\s]+))?:)?(?P<host>[^?/#\s]+)(?:\:(?P<port>\d+))?/(?P<db>[^?/#\s]+))(?:/(?P<table_or_query>[^?/#\s@]+))?(?:@(?P<geometry_columns>[^?/#\s]+))?$',
	'xls': r'^(?P<path>.*\.xlsx?)(?:\:(?P<sheet>[a-z0-9_-]+))?$',
}


def _try_gdf(df, geometry_columns=('geometry', 'WKT'), crs=None):
	if isinstance(geometry_columns, str):
		geometry_columns = [geometry_columns]

	geometry_ok = False
	for k in geometry_columns:
		if k in df:
			try:
				df['geometry'] = df[k].apply(wkt.loads)
				if k != 'geometry': df.pop(k)
			except (TypeError, AttributeError):
				print("warning: can't transform empty or broken geometry", file=sys.stderr)
			else:
				geometry_ok = True

	if geometry_ok:
		return gpd.GeoDataFrame(df, crs=crs)

	return df


def connect_postgres(connection_string):
	from sqlalchemy import create_engine
	m = re.match(FORMATS['postgresql'], connection_string)
	engine = create_engine(m['engine'])
	return engine


def read_xls(path, path_match, *args, **kwargs):
	path, sheet = path_match['path'], path_match['sheet']
	excel_dict = pd.read_excel(path, sheet_name=sheet, **kwargs)  # OrderedDict of dataframes
	return _try_gdf(excel_dict.popitem()[1])  # pop item, last=False, returns (key, value) tuple


def read_csv(path, path_match, crs=None, geometry_columns=('geometry', 'WKT'), *args, **kwargs):
	source_df = pd.read_csv(path, **kwargs)
	return _try_gdf(source_df, geometry_columns, crs)


def gpd_read(path, crs=None, *args, **kwargs):
	source_df = gpd.read_file(path, *args, **kwargs)
	if crs is not None:
		source_df.crs = crs

	return source_df


def read_geojsonseq(path, path_match, crs=None, *args, **kwargs):
	return gpd_read(path, crs, driver='GeoJSONSeq', *args, **kwargs)


def read_geojson(path, path_match, crs=None, *args, **kwargs):
	return gpd_read(path, crs, driver='GeoJSON', *args, **kwargs)


def read_shp(path, path_match, crs=None, *args, **kwargs):
	return gpd_read(path, crs, driver='ESRI Shapefile', *args, **kwargs)


def read_gpkg(path, path_match, crs=None, *args, **kwargs):
	match = re.match(FORMATS['gpkg'], path)
	filename = match['file_name']
	file_path = match['file_path']
	layer_name = match['layer_name']
	own_name = match['file_own_name']

	if layer_name in ('', None):
		try:
			layers = fiona.listlayers(file_path)
		except ValueError:
			raise ValueError('Fiona driver can\'t read layers from file %s' % filename)

		if len(layers) == 1 and layer_name in ('', None):
			layer_name = layers[0]
		elif own_name in layers:
			layer_name = own_name
		else:
			raise ValueError('Can\'t detect default layer in %s. Layers available are: %s' % (filename, ', '.join(layers)))

	return gpd_read(file_path, driver='GPKG', crs=crs, layer=layer_name, **kwargs)


def read_postgresql(path, path_match, *args, **kwargs):
	with connect_postgres(path).begin() as conn:
		df = pd.read_sql(path_match['table_or_query'], conn)

	gcc = path_match['geometry_columns']
	if gcc is None:
		return df

	gcc = path_match['geometry_columns'].split(',')
	for col in gcc:
		if col in df:
			df[col] = df[col].apply(bytes.fromhex).apply(wkb.loads)

	gdf = gpd.GeoDataFrame(df)
	if 'geometry' not in gcc:
		gdf._geometry_column = gcc[0]

	return gdf


def read_file(path, *args, **kwargs):
	for fmt, regexp in FORMATS.items():
		match = re.match(regexp, path)
		if match:
			return globals()[f'read_{fmt}'](path, match, *args, **kwargs)
	raise ValueError(f'{path}: file format not recognized')


def write_csv(df, path, path_match, *args, **kwargs):
	if os.path.exists(path):
		os.unlink(path)

	df.to_csv(path, index=False)


def write_gpkg(df, path, path_match, *args, **kwargs):
	filepath = path_match['file_path']
	layer_name = path_match['layer_name'] or path_match['file_own_name']

	if os.path.exists(filepath):
		if layer_name in fiona.listlayers(filepath):
			fiona.remove(filepath, 'GPKG', layer_name)

	df.to_file(filepath, driver='GPKG', encoding='utf-8', layer=layer_name)


def write_geojson(df, path, path_match, *args, driver='GeoJSON', **kwargs):
	if os.path.exists(path):
		os.unlink(path)

	df.to_file(path, driver=driver, encoding='utf-8')


def write_shp(df, path, path_match, *args, **kwargs):
	df.to_file(path, encoding='utf-8')


def write_geojsonseq(df, path, path_match, *args, **kwargs):
	write_geojson(df, path, path_match, *args, driver='GeoJSONSeq', **kwargs)


def write_postgresql(df, path, path_match, *args, **kwargs):
	df = df.copy()
	table_name = path_match['table_or_query']
	with connect_postgres(path).begin() as connection:
		if 'geometry' in df:
			df['geometry'] = df['geometry'].apply(wkb.dumps).apply(bytes.hex)

		pd.io.sql.execute('DROP TABLE IF EXISTS %s' % table_name, connection)
		df.to_sql(table_name, connection, chunksize=1000)
		if 'geometry' in df:
			if not df.crs and -181 < df['geometry'].extents[0] < 181:
				crs_num = 4326
			elif not df.crs:
				crs_num = 3857
			else:
				crs_num = df.crs.to_epsg()

			pd.io.sql.execute("""
				ALTER TABLE %s
				ALTER COLUMN "geometry" TYPE Geometry""" % table_name, connection)
			pd.io.sql.execute("""
				UPDATE %s SET "geometry"=st_setsrid(geometry, %s)
				""" % (table_name, crs_num), connection)


def write_file(df, path, *args, **kwargs):
	for fmt, regexp in FORMATS.items():
		match = re.match(regexp, path)
		if match:
			return globals()[f'write_{fmt}'](df, path, match, *args, **kwargs)

	raise ValueError(f'{path}: file format not recognized')
