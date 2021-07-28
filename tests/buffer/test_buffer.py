from erde import buffer, read_df

df = read_df('tests/buffer/points.geojson')

def test_geoseries():
	# if we call buffer with geoseries, it returns geoseries



# geoseries vs geodataframe
# crs vs crs is none
# resolution default vs 3 vs 5
