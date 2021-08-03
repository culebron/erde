import pytest
from unittest import mock
from erde import convert, read_df


def assert_3857(geoseries):
	assert any((geoseries.x > 180) | (geoseries.x < 180))


cities = read_df('tests/convert/cities.csv')
c = cities_4326 = cities.copy()
cities_4326.crs = 4326
input_chunks = [c[i:i+5].copy() for i in range(0, len(c), 5)]

def _run_test(input_chunks, *args, **kwargs):
	input_chunks = [d.copy() for d in input_chunks]
	read_iter = mock.MagicMock(return_value=input_chunks)
	reader = mock.MagicMock(__enter__=read_iter)
	read_stream = mock.MagicMock(return_value=reader)

	output_chunks = []
	def writer(df):
		output_chunks.append(df)

	write_stream = mock.Mock(return_value=mock.MagicMock(__enter__=mock.MagicMock(return_value=writer)))

	with mock.patch('erde.op.convert.write_stream', write_stream), mock.patch('erde.op.convert.read_stream', read_stream):
		convert(*args, **kwargs)

	return output_chunks, write_stream, read_stream


def test_convert_file_formats():
	# convert between formats, no crs set. writes all chunks.

	output_chunks, writer, reader = _run_test(input_chunks, 'tests/convert/cities.csv', '/tmp/no-write.gpkg')
	assert len(output_chunks) == len(input_chunks)
	for src, res in zip(input_chunks, output_chunks):
		assert len(src) == len(res)
		assert src.geometry.equals(res.geometry)  # no conversion

	reader.assert_called_with('tests/convert/cities.csv', sync=False)
	writer.assert_called_with('/tmp/no-write.gpkg', sync=False)

def test_convert_crs():
	output_chunks, writer, reader = _run_test(input_chunks, 'tests/convert/cities.csv', '/tmp/no-write.gpkg', to_crs=3857)

	test_df = output_chunks[0]
	assert test_df.crs == 3857
	assert len(output_chunks) > 0
	for indf, outdf in zip(input_chunks, output_chunks):
		assert indf.geometry.to_crs(3857).equals(outdf.geometry)
		assert_3857(outdf.geometry)


def test_cant_convert_naive():
	with pytest.raises(RuntimeError):
		_run_test([cities], 'tests/convert/cities.csv', '/tmp/no-write.gpkg', to_crs=3857)  # how to catch an exception? make it a context manager?

def test_force_crs():
	output_chunks, writer, reader = _run_test(input_chunks, 'tests/convert/cities.csv', '/tmp/no-write.gpkg', from_crs=3857)

	for indf, outdf in zip(input_chunks, output_chunks):
		assert indf.geometry.equals(outdf.geometry)
		assert outdf.crs == 3857

	for df in input_chunks:
		df.crs = 4326  # reset back

def test_force_convert():
	output_chunks, writer, reader = _run_test(input_chunks, 'tests/convert/cities.csv', '/tmp/no-write.gpkg', from_crs=3857, to_crs=4326)

	for outdf in output_chunks:
		assert outdf.geometry.x.abs().max() < 1  # we took 4326, put it in Google Merkator, like metres => converted to 4326 (the result must be a lot less than 1 degree)

	# convert gdf, force crs, with to_crs
