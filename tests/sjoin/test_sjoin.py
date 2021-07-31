# sgroup
# less rows
# aggregation is correct
# left join works

from erde import read_df
from erde.op import sjoin

pts = read_df('tests/sjoin/points.geojson')
polys = read_df('tests/sjoin/polys.geojson')

def sets_dict(d):
	# if a poly has no matching points, sum(name) will be 0 (int)
	# make it an empty set
	return {k: set(v) if isinstance(v, str) else set()
		for k, v in d.items()}

def test_sgroup():
	j = sjoin.sgroup(polys, pts, {'number': 'sum'})
	result = j.set_index('name')['number'].to_dict()
	assert result == {'X': 9, 'Y': 3, 'Z': 0, 'W': 3}

	j = sjoin.sgroup(polys, pts, {'name': 'sum'})
	result = j.set_index('name')['name_right'].to_dict()

	assert sets_dict(result) == sets_dict({'X': 'CFI', 'Y': 'ADG', 'Z': 0, 'W': 'AB'})


# slookup
# считаем что с чем группируется и проверяем в выдаче

# sfilter
# проверка на тех же датасетах с точками
