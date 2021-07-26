from erde import io, read_df, write_df
import os
import pytest
import pandas as pd
import geopandas as gpd


d = 'tests/io/data/'

def test_read_file():
	# unrecognized format
	with pytest.raises(ValueError):
		read_df('not existing path')

	read_df(f'{d}points.xlsx')

	df = read_df(f'{d}points.gpkg', crs=4326)
	assert df.crs == 4326


def test_geom_types():
	for gt in ('points', 'lines'):
		for fmt in ('csv', 'gpkg', 'geojson', 'geojsonl.json', 'shp'):
			output = f'/tmp/points.{fmt}'
			df = read_df(f'{d}points.{fmt}')
			write_df(df, output)
			write_df(df, output)  # write twice to test if it can overwrite normally
			os.unlink(output)

	with pytest.raises(ValueError):
		write_df(df, 'not existing format')


def test_exceptions_gpkg():
	with pytest.raises(ValueError): read_df(f'{d}points.gpkg:nosuchlayer')
	with pytest.raises(ValueError): read_df(f'{d}points.gpkg:')
	with pytest.raises(ValueError): read_df(f'{d}no_file.gpkg')

	# must read and guess the layer by file own name
	read_df(f'{d}multiple-layers.gpkg')
	with pytest.raises(ValueError):
		read_df(f'{d}multiple-layers-unclear.gpkg')

def test_pg_id():
	pgstr = 'postgresql://erdetest:erdetest@localhost:5432/erdetest'
	for gtype in ('points', 'lines'):
		df = read_df(f'{d}{gtype}.gpkg')
		write_df(df, f'{pgstr}/{gtype}')

		df2 = read_df(f'{pgstr}/{gtype}@geometry')
		assert df2.geometry.equals(df.geometry)

	with io.postgres.connect_postgres(pgstr).begin() as engine:
		with open(f'{d}houses.sql') as f:
			pd.io.sql.execute(f.read(), engine)

	gdf = read_df(f'{pgstr}/houses@centroid')
	assert isinstance(gdf, gpd.GeoDataFrame)

	df = read_df(f'{pgstr}/houses')  # same but no geometry
	assert isinstance(df, pd.DataFrame)
