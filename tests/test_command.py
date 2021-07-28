from contextlib import contextmanager
from erde import command, read_df
from unittest import mock
import geopandas as gpd
import pytest
import sys

d = 'tests/io/data/'
df = read_df(d + 'points.gpkg')

def test_decorate():
	def clifunc(input_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
		return input_data[:10]

	clifunc2 = command(clifunc)
	assert clifunc2 == clifunc
	assert hasattr(clifunc2, '_argh')

@contextmanager
def _mock_context_manager(*args, **kwargs):
	yield mock.Mock()

def test_cli_call():
	def clifunc(input_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
		return input_data[:10]

	def crashing_func(input_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
		raise ValueError("let's crash")

	# pretend we're decorating in __main__
	m1 = mock.MagicMock()
	m2 = mock.MagicMock(__name__='__main__')
	m1.return_value = m2
	with mock.patch('inspect.getmodule', m1), mock.patch('yaargh.dispatch', mock.MagicMock()):
		clifunc3 = command(clifunc)
		crashing_func2 = command(crashing_func)

		m3 = mock.MagicMock()
		setattr(m3, 'output-file', '/tmp/test-output-file.gpkg')
		m4 = mock.Mock(return_value=m3)
		with mock.patch('yaargh.ArghParser.parse_args', new=m4):
			def run_with_patches(ipdb=0, pudb=0):
				with mock.patch('erde.IPDB', ipdb), mock.patch('erde.PUDB', pudb), mock.patch('ipdb.slaunch_ipdb_on_exception', mock.MagicMock()) as mipdb, mock.patch('erde._handle_pudb', mock.MagicMock()) as mpudb:
					clifunc3(df)

				return mipdb, mpudb

			mi, mp = run_with_patches(0, 0)
			mi.assert_not_called()
			mp.assert_not_called()

			mi, mp = run_with_patches(1, 0)
			mi.assert_called_once()
			mp.assert_not_called()

			mi, mp = run_with_patches(0, 1)
			mi.assert_not_called()
			mp.assert_called_once()

			with mock.patch('erde.PUDB', 1), mock.patch('pudb.post_mortem', mock.MagicMock()):
				crashing_func2(df)

	assert not hasattr(clifunc3, '_argh')
	assert clifunc3 != clifunc
