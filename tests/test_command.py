from unittest import mock
import geopandas as gpd
import pytest

from erde import command

def test_decorate():
	def clifunc(input_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
		return input_data[:10]

	clifunc2 = command(clifunc)
	assert clifunc2 == clifunc
	assert hasattr(clifunc2, '_argh')

	# pretend we're decorating in __main__
	m1 = mock.MagicMock()
	m2 = mock.MagicMock(__name__='__main__')
	m1.return_value = m2
	with mock.patch('inspect.getmodule', m1), mock.patch('yaargh.dispatch', mock.MagicMock()):
		clifunc3 = command(clifunc)

	assert not hasattr(clifunc3, '_argh')
	assert clifunc3 != clifunc
