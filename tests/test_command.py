from contextlib import contextmanager
from erde import autocli, read_df, read_stream, write_stream
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

	def streaming_func(input_data: read_stream) -> write_stream:
		return input_data[:10]

	# pretend we're decorating in __main__
	m1 = mock.MagicMock()
	m1.return_value.__name__='__main__'

	m4 = mock.MagicMock()
	m4.return_value.__getitem__.return_value.__getitem__.return_value = '/tmp/test-output-file.gpkg'
	with mock.patch('inspect.getmodule', m1), mock.patch('yaargh.dispatch', mock.MagicMock()), mock.patch('yaargh.ArghParser.parse_known_args', new=m4):
		clifunc3 = autocli(clifunc)
		crashing_func2 = autocli(crashing_func)

		# tests if debuggers were initialized (but no exception is raised here)
		for ipdb, pudb in ((0, 0), (1, 0), (0, 1)):
			with mock.patch('erde.IPDB', ipdb), mock.patch('erde.PUDB', pudb), mock.patch('ipdb.slaunch_ipdb_on_exception', mock.MagicMock()) as mipdb, mock.patch('erde._handle_pudb', mock.MagicMock()) as mpudb:
				clifunc3(df)

			for param, obj in ((ipdb, mipdb), (pudb, mpudb)):
				attr = 'assert_not_called' if param == 0 else 'assert_called_once'
				getattr(obj, attr)()

			return mipdb, mpudb

		# make sure debugger is called when an exception is raised
		with mock.patch('erde.PUDB', 1), mock.patch('pudb.post_mortem', mock.MagicMock()) as pm_debug:
			crashing_func2(df)

		pm_debug.assert_called_once()

	assert not hasattr(clifunc3, '_argh')
	assert clifunc3 != clifunc
