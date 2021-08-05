import pytest
from erde import read_df, area

gdf4326 = lambda: read_df('tests/area/rectangle-4326.csv')
gdf3857 = lambda: read_df('tests/area/rectangle-3857.csv')

def test_area():
	# simple tests: the same rectangle in 2 CRS must get a sensible area value
	r4326 = gdf4326()
	assert 'area' not in r4326
	result1 = area(r4326, default_crs=4326)
	assert 'area' in result1
	assert abs(result1['area'].values[0] - 250000) / 250000 < 0.01

	r3857 = gdf3857()
	assert 'area' not in r3857
	result2 = area(r3857, default_crs=3857)
	assert 'area' in result2
	assert abs(result2['area'].values[0] - 250000) / 250000 < 0.01

def test_column_name():
	for col in ('surface_area', 'random_name', '___some_more_name'):
		r4326 = gdf4326()
		assert col not in r4326
		assert 'area' not in r4326
		result1 = area(r4326, column_name=col, default_crs=4326)
		assert 'area' not in result1
		assert col in result1
		assert abs(result1[col].values[0] - 250000) / 250000 < 0.01

def test_irrelevant_areas():
	df = read_df('tests/area/irrelevant-objects.csv')
	for nullify in (True, False):
		df2 = area(df.copy(), nullify_irrelevant=nullify, skip_transform=True)
		is_poly = df2.geometry.geom_type.str.endswith('Polygon')
		assert all(df2[is_poly]['area'] > 0)
		na = all(df2[~is_poly]['area'].isna())
		assert na if nullify else not na

def test_no_crs():
	# no crs + no default_crs + skip_transform = False => exception
	# crs or default_crs or skip_transform => no exception
	r3857 = gdf3857()
	with pytest.raises(ValueError):
		area(r3857)

	good_variants = (
		(3857, None, False),
		(None, 3857, False),
		(None, None, True),
	)
	for crs, def_crs, skip in good_variants:
		df = gdf3857()
		df.crs = crs
		area(df, default_crs=def_crs, skip_transform=skip)
