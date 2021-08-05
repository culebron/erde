import pytest
from erde import read_df, length

gdf4326 = lambda: read_df('tests/length/line-4326.csv')
gdf3857 = lambda: read_df('tests/length/line-3857.csv')

def test_length():
	# simple tests: the same line in 2 CRS must get a sensible length value
	r4326 = gdf4326()
	assert 'length' not in r4326
	result1 = length(r4326, default_crs=4326)
	assert 'length' in result1
	assert abs(result1['length'].values[0] - 2000) / 2000 < 0.01

	r3857 = gdf3857()
	assert 'length' not in r3857
	result2 = length(r3857, default_crs=3857)
	assert 'length' in result2
	assert abs(result2['length'].values[0] - 2000) / 2000 < 0.01

def test_column_name():
	for col in ('length_actually', 'random_name', '___some_more_name'):
		r4326 = gdf4326()
		assert col not in r4326
		assert 'length' not in r4326
		result1 = length(r4326, column_name=col, default_crs=4326)
		assert 'length' not in result1
		assert col in result1
		assert abs(result1[col].values[0] - 2000) / 2000 < 0.01

def test_irrelevant_lengths():
	df = read_df('tests/length/irrelevant-objects.csv')
	for nullify in (True, False):
		df2 = length(df.copy(), nullify_irrelevant=nullify, skip_transform=True)
		is_ls = df2.geometry.geom_type.str.endswith('LineString')
		if not nullify:
			assert all(df2[is_ls]['length'] > 0)
		na = all(df2[~is_ls]['length'].isna())
		assert na if nullify else not na

def test_no_crs():
	# no crs + no default_crs + skip_transform = False => exception
	# crs or default_crs or skip_transform => no exception
	r3857 = gdf3857()
	with pytest.raises(ValueError):
		length(r3857)

	good_variants = (
		(3857, None, False),
		(None, 3857, False),
		(None, None, True),
	)
	for crs, def_crs, skip in good_variants:
		df = gdf3857()
		df.crs = crs
		length(df, default_crs=def_crs, skip_transform=skip)
