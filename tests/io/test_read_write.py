from erde import io, read_df, write_df
from shapely.geometry import Point
from unittest import mock
import geopandas as gpd
import os
import pandas as pd
import pytest


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
	for n in (f'{d}points.gpkg:nosuchlayer', f'{d}points.gpkg:', f'{d}no_file.gpkg', f'{d}multiple-layers-unclear.gpkg'):
		with pytest.raises(ValueError):
			read_df(n)

	# must read and guess the layer by file own name
	read_df(f'{d}multiple-layers.gpkg')

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


def test_write_geom():
	g = Point(1, 2)
	from random import choice
	crs = choice((4326, 3857, 2003, 2004, 2005))
	fn = '/tmp/some-file.gpkg'
	with mock.patch('erde.write_df') as wr:
		from erde import write_geom
		write_geom(g, fn, crs=crs)

	df = wr.call_args[0][0]
	assert len(df) == 1  # exactly one geometry
	assert df.geometry[0] == g  # geometry is the only one
	assert wr.call_args[0][1] == fn  # filename is what's expected
	assert df.crs == crs  # CRS is set to the written GDF


def test_read_geom():
	from erde import read_geom
	for gt in ('points', 'lines'):
		for fmt in ('csv', 'gpkg', 'geojson', 'geojsonl.json', 'shp'):
			fn = f'{d}points.{fmt}'
			df = read_df(fn)
			assert len(df) > 1  # if there's just 1 line, use another file in this test!
			g = read_geom(fn)
			assert g.equals(df.geometry[0])
