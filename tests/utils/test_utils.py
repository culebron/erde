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
