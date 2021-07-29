# sgroup
# less rows
# aggregation is correct
# left join works

from erde import read_df, sjoin

pts = read_df('tests/sjoin/points.geojson')
polys = read_df('tests/sjoin/polys.geojson')

def test_sgroup():
	j = sjoin.sgroup(polys, pts, {'number': 'sum'})
	result = j.set_index('name')['number'].to_dict()
	assert result == {'X': 9, 'Y': 3, 'Z': 0, 'W': 3}

	j = sjoin.sgroup(polys, pts, {'name': 'sum'})
	result = j.set_index('name')['name_right'].to_dict()
	for k, v in result.items():
		# if a poly has no matching points, sum(name) will be 0 (int)
		# make it an empty set
		result[k] = set(v) if isinstance(v, str) else set()
	assert result == {'X': set('CFI'), 'Y': set('ADG'), 'Z': set(), 'W': set('AB')}


# slookup
# считаем что с чем группируется и проверяем в выдаче

# sfilter
# проверка на тех же датасетах с точками
