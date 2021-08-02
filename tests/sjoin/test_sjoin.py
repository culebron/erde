import numpy as np
import pandas as pd
import pytest
from erde import read_df
from erde.op import sjoin

pts = read_df('tests/sjoin/points.geojson')
polys = read_df('tests/sjoin/polys.geojson')


def sets_dict(d):
	# if a poly has no matching points, sum(name) will be 0 (int)
	# make it an empty set
	return {k: set(v) if isinstance(v, str) else set()
		for k, v in d.items()}


def test_sagg():
	j = sjoin.sagg(polys, pts, {'number': 'sum'})
	result = j.set_index('name')['number'].to_dict()
	assert result == {'X': 9, 'Y': 3, 'Z': 0, 'W': 3}

	j = sjoin.sagg(polys, pts, {'name': 'sum'})
	result = j.set_index('name')['name_right'].to_dict()

	assert sets_dict(result) == sets_dict({'X': 'CFI', 'Y': 'ADG', 'Z': 0, 'W': 'AB'})


def test_sagg_errors():
	with pytest.raises(TypeError):
		sjoin.sagg(pts, polys, 1)

	with pytest.raises(TypeError):
		sjoin.sagg(pts, polys, ['a', 'b', 'c'])

	with pytest.raises(ValueError):
		sjoin.sagg(pts, polys, dict())


def test_slookup():
	j = sjoin.slookup(pts, polys, 'name', suffixes=('', '_poly'))
	assert set(j) == {'name', 'number', 'geometry', 'name_poly'}

	result = j.set_index('name')['name_poly']

	assert result.equals(pd.Series({'A': 'Y', 'B': 'W', 'C': 'X', 'D': 'Y', 'E': np.nan, 'F': 'X', 'G': 'Y', 'H': np.nan, 'I': 'X'}))

	j = sjoin.slookup(pts, polys, 'name', suffixes=('', '_poly'), join='inner')
	result = j.set_index('name')['name_poly']
	assert result.equals(pd.Series({'A': 'Y', 'B': 'W', 'C': 'X', 'D': 'Y', 'F': 'X', 'G': 'Y', 'I': 'X'}))


def test_sfilter():
	j1 = sjoin.sfilter(polys, pts)
	assert set(j1['name']) == {'X', 'Y', 'W'}
	j1n = sjoin.sfilter(polys, pts, negative=True)
	assert set(j1n['name']) == {'Z'}

	j2 = sjoin.sfilter(pts, polys)
	assert set(j2['name']) == set('ABCDFGI')
	j1n = sjoin.sfilter(pts, polys, negative=True)
	assert set(j1n['name']) == set('EH')


def test_df_on():
	# must pass with 'geometry' and the geometry column
	sjoin._df_on(pts, 'geometry', 'left')
	sjoin._df_on(pts, pts.geometry, 'right')

	sjoin._df_on(pts, pts.geometry.to_crs(3857).buffer(1000), 'right')

	with pytest.raises(ValueError):
		sjoin._df_on(pts, 'geometry', 'front')

	with pytest.raises(ValueError):
		sjoin._df_on(pts, polys.geometry, 'left')

	with pytest.raises(TypeError):
		sjoin._df_on(pts, list(range(10)), 'left')
