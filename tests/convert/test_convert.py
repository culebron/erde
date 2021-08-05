import pytest
from unittest import mock
from erde import convert, read_df


def assert_3857(geoseries):
	assert any((geoseries.x > 180) | (geoseries.x < 180))


cities = read_df('tests/convert/cities.csv')
c = cities_4326 = cities.copy()
cities_4326.crs = 4326
input_chunks = [c[i:i+5].copy() for i in range(0, len(c), 5)]


def test_convert_file_formats():
	# convert between formats, no crs set. returns the df as is

	result = convert(cities_4326)
	assert result.crs == 4326
	assert result.equals(cities_4326)


def test_convert_crs():
	result = convert(cities_4326, to_crs=3857)
	assert result.crs == 3857
	assert len(result) == len(cities_4326)
	assert list(result) == list(cities_4326)
	assert cities_4326.geometry.to_crs(3857).equals(result.geometry)


def test_cant_convert_naive():
	with pytest.raises(RuntimeError):
		convert(cities, to_crs=3857)


def test_force_crs():
	assert cities.crs is None
	result = convert(cities.copy(), from_crs=3857)

	assert cities.geometry.equals(result.geometry)
	assert result.crs == 3857

def test_force_convert():
	result = convert(cities.copy(), from_crs=3857, to_crs=4326)

	assert result.geometry.x.abs().max() < 1  # we took 4326, put it in Google Merkator, like metres => converted to 4326 (the result must be a lot less than 1 degree)
