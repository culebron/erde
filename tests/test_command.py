from contextlib import contextmanager
from erde import autocli, read_df
from unittest import mock
import geopandas as gpd

d = 'tests/io/data/'
df = read_df(d + 'points.gpkg')

def test_decorate():
	def clifunc(input_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
		return input_data[:10]

	clifunc2 = autocli(clifunc)
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
	m1.return_value.__name__='__main__'
	with mock.patch('inspect.getmodule', m1), mock.patch('yaargh.dispatch', mock.MagicMock()):
		clifunc3 = autocli(clifunc)
		crashing_func2 = autocli(crashing_func)

		m4 = mock.MagicMock()
		m4.return_value.__getitem__.return_value.__getitem__.return_value = '/tmp/test-output-file.gpkg'
		with mock.patch('yaargh.ArghParser.parse_known_args', new=m4):
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
