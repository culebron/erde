from erde import io
import os
import pytest
import pandas as pd
import geopandas as gpd


d = 'tests/io/data/'

def test_read_file():
	# unrecognized format
	with pytest.raises(ValueError):
		io.read_file('not existing path')

	io.read_file(f'{d}points.xlsx')

	df = io.read_file(f'{d}points.gpkg', crs=4326)
	assert df.crs == 4326


def test_geom_types():
	for gt in ('points', 'lines'):
		for fmt in ('csv', 'gpkg', 'geojson', 'geojsonl.json', 'shp'):
			output = f'/tmp/points.{fmt}'
			df = io.read_file(f'{d}points.{fmt}')
			io.write_file(df, output)
			io.write_file(df, output)  # write twice to test if it can overwrite normally
			os.unlink(output)

	with pytest.raises(ValueError):
		io.write_file(df, 'not existing format')

def test_exceptions_csv():
	# non-geometry or non-existent column
	df = io.read_file(f'{d}points.csv')

	df2 = io.read_file(f'{d}points.csv', geometry_columns='number')
	assert len(df) == len(df2)
	assert 'number' in df2
	df2 = io.read_file(f'{d}points.csv', geometry_columns='random_column_does_not_exist')
	assert len(df) == len(df2)

def test_exceptions_gpkg():
	with pytest.raises(ValueError): io.read_file(f'{d}points.gpkg:nosuchlayer')
	with pytest.raises(ValueError): io.read_file(f'{d}points.gpkg:')
	with pytest.raises(ValueError): io.read_file(f'{d}no_file.gpkg')

	# must read and guess the layer by file own name
	io.read_file(f'{d}multiple-layers.gpkg')
	with pytest.raises(ValueError):
		io.read_file(f'{d}multiple-layers-unclear.gpkg')

	df = io.read_file(f'{d}points-broken.csv')
	assert len(df) == 8

def test_pg_id():
	pgstr = 'postgresql://erdetest:erdetest@localhost:5432/erdetest'
	for gtype in ('points', 'lines'):
		df = io.read_file(f'{d}{gtype}.gpkg')
		io.write_file(df, f'{pgstr}/{gtype}')

		df2 = io.read_file(f'{pgstr}/{gtype}@geometry')
		assert df2.geometry.equals(df.geometry)

	with io.connect_postgres(pgstr).begin() as engine:
		with open(f'{d}houses.sql') as f:
			pd.io.sql.execute(f.read(), engine)

	gdf = io.read_file(f'{pgstr}/houses@centroid')
	assert isinstance(gdf, gpd.GeoDataFrame)

	df = io.read_file(f'{pgstr}/houses')  # same but no geometry
	assert isinstance(df, pd.DataFrame)
