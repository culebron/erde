import geopandas as gpd
import pytest
from erde import utils, read_df
from erde.op import sjoin, buffer
from shapely import wkt
from shapely.geometry import LineString

pl1 = wkt.loads('LINESTRING (82.956142 55.050099,83.174036 54.923359,83.019111 54.845166,82.801218 54.963546,82.913163 55.043800,83.124060 54.926231,83.008117 54.879681,82.861188 54.966989)')

pl2 = wkt.loads('LINESTRING (83.53793 56.10852, 86.04281 55.34134, 87.18539 53.78736, 83.75766 53.36991, 73.5184 55.01512)')

pts = read_df('tests/sjoin/points.geojson')
polys = read_df('tests/sjoin/polys.geojson')

houses = read_df('tests/utils/houses.csv')
houses.crs = 4326
schools = read_df('tests/utils/schools.csv')
schools.crs = 4326

def test_polyline():
	assert utils.encode_poly(pl1) == 'c~~nI{jiyNbwW{pi@tgNhg]{bVxpi@qtNszTx}Uceh@|aHrsUu`Phu['

	l2 = utils.decode_poly('gumuIa_{|NzytCofhNjonHcd~E`ppAhn|Sqi`Ijzn}@')

	assert isinstance(l2, LineString)
	assert l2.equals(pl2)


def test_linestring_between():
	# error: index1 not equal index2
	with pytest.raises(ValueError):
		utils.linestring_between(pts.geometry, polys.geometry)

	hs = sjoin.sjfull(houses, schools, right_on=buffer.main(schools.geometry, 500))

	assert 'geometry_right' in hs and 'geometry' in hs
	lines = utils.linestring_between(hs['geometry'], hs['geometry_right'])
	assert isinstance(lines, gpd.GeoSeries)
	assert lines.index.equals(hs['geometry'].index)

	for i, v in hs['geometry'].items():
		assert LineString([v, hs['geometry_right'].loc[i]]).equals(lines.loc[i])

	# index is a part of other index => error
	with pytest.raises(ValueError):
		utils.linestring_between(hs['geometry'], hs[hs.index < 100]['geometry_right'])

	lst1 = hs['geometry'].tolist()
	lst2 = hs['geometry_right'].tolist()
	lines2 = utils.linestring_between(lst1, lst2)
	assert isinstance(lines2, list)
	for a, b, c in zip(lst1, lst2, lines2):
		assert LineString([a, b]) == c

	# different lengths => error
	with pytest.raises(ValueError):
		utils.linestring_between(lst1, lst2[:-3])


def test_coslat():
	import numpy as np
	import pandas as pd

	for df in (schools, houses, pts):
		geom = df.geometry
		if any(geom.geom_type != 'Point'):
			geom = geom.to_crs(3857).centroid.to_crs(4326)

		coslat1 = geom.apply(lambda g: np.cos(np.radians(g.y)))
		coslat2 = utils.coslat(df.geometry)
		assert isinstance(coslat2, pd.Series)
		assert all(coslat1 - coslat2 < .000000001)


def test_crossjoin():
	import pandas as pd
	df1 = pd.DataFrame({'a': list(range(5)), 'b': list(range(5, 10))})
	df2 = pd.DataFrame({'c': list(range(10, 17))})

	cj = utils.crossjoin(df1, df2)
	assert len(cj) == len(df1) * len(df2)
	for a, g in cj.groupby('a'):
		assert set(g['c']) == set(range(10, 17))

	assert '_tmpkey' not in cj


def test_lonlat2gdf():
	h = houses.copy()

	for x, y in (('lon', 'lat'), ('lng', 'lat'), ('long', 'lat'), ('longitude', 'latitude'), ('x', 'y'), ('X', 'Y')):
		h[x] = h.geometry.x
		h[y] = h.geometry.y

		res = utils.lonlat2gdf(h[[x, y]].copy())  # copy to avoid SettingWithCopyWarning
		assert h.geometry.equals(res.geometry)

	h['tst1'] = h.geometry.x
	h['tst2'] = h.geometry.y
	with pytest.raises(ValueError):
		utils.lonlat2gdf(h[['tst1', 'tst2']])


def test_transform():
	h2 = houses[:10]
	from functools import partial
	g3857 = h2.geometry.apply(partial(utils.transform, crs_from=4326, crs_to=3857))
	g3857.crs = 3857  # to avoid CRS mismatch warning

	assert all(h2.to_crs(3857).geometry.geom_almost_equals(g3857))
