from erde import read_df, read_stream

d = 'tests/io/data/'

def test_numeric_file():
	# just coverage
	df = read_df(d + 'numeric.csv')
	assert len(df) == 4
	assert set(df) == {'a', 'b', 'c'}
	for df in read_stream(d + 'numeric.csv', chunk_size=2):
		assert len(df) == 2
		assert set(df) == {'a', 'b', 'c'}
