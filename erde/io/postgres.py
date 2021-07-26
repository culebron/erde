from .base import BaseDriver
from shapely import wkb
import geopandas as gpd
import pandas as pd
import re


PATH_REGEXP = r'^(?P<engine>postgresql\://(?:(?P<user>[^:\/?#\s]+)(?:\:(?P<password>[^:\/?#\s]+))?:)?(?P<host>[^?/#\s]+)(?:\:(?P<port>\d+))?/(?P<db>[^?/#\s]+))(?:/(?P<table_or_query>[^?/#\s@]+))?(?:@(?P<geometry_columns>[^?/#\s]+))?$'


def connect_postgres(connection_string):
	from sqlalchemy import create_engine
	m = re.match(PATH_REGEXP, connection_string)
	engine = create_engine(m['engine'])
	return engine




class PostgresDriver(BaseDriver):
	path_regexp = PATH_REGEXP

	@classmethod
	def read_df(cls, path, path_match, *args, **kwargs):
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

	@classmethod
	def write_df(cls, df, path, path_match, *args, **kwargs):
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

driver = PostgresDriver
