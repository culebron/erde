from contextlib import contextmanager
from erde import autocli, read_df, read_stream, write_stream, ErdeDecoratorError
from unittest import mock
import geopandas as gpd
import pytest

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
	def df_df(input_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
		return input_data[:10]

	def crash(input_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
		raise ValueError("let's crash")

	def stream_stream(input_data: read_stream) -> write_stream:
		return input_data[:10]

	def stream_nothing(input_data: read_stream):
		pass

	def df_yield(input_data: gpd.GeoDataFrame) -> write_stream:
		for i in range(0, len(input_data), 5):
			yield input_data[i:i + 5]

	def stream_yield(input_data: read_stream) -> write_stream:
		for i in range(0, len(input_data), 5):
			yield input_data[i:i + 5]

	funcs = {'df_df': df_df, 'crash': crash, 'stream_stream': stream_stream, 'stream_nothing': stream_nothing, 'df_yield': df_yield, 'stream_yield': stream_yield}

	# pretend we're decorating in __main__
	m1 = mock.MagicMock()
	m1.return_value.__name__='__main__'

	m4 = mock.MagicMock()
	m4.return_value.__getitem__.return_value.__getitem__.return_value = '/tmp/test-output-file.gpkg'
	with mock.patch('inspect.getmodule', m1), mock.patch('yaargh.dispatch', mock.MagicMock()), mock.patch('yaargh.ArghParser.parse_known_args', new=m4):

		dec_funcs = {k: autocli(v) for k, v in funcs.items()}
		path = 'tests/area/irrelevant-objects.csv'
		funcs_args = {'df_df': df, 'crash': df, 'stream_stream': path, 'stream_nothing': path, 'df_yield': df, 'stream_yield': path}
		# tests if debuggers were initialized (but no exception is raised here)
		# the function that accepts GeoDataFrame will not open path itself
		# (yaargh decorator works only when dispatched)
		# but read_stream annotation is opened in decorated function, hence string path
		for k, func in dec_funcs.items(): # , streaming_dec:
			if k == 'crash': continue
			for ipdb, pudb in ((0, 0), (1, 0), (0, 1)):
				with mock.patch('erde.IPDB', ipdb), mock.patch('erde.PUDB', pudb), mock.patch('ipdb.slaunch_ipdb_on_exception', mock.MagicMock()) as mipdb, mock.patch('erde._handle_pudb', mock.MagicMock()) as mpudb:
					print('calling to execute', k)
					func(funcs_args[k])

				for param, obj in ((ipdb, mipdb), (pudb, mpudb)):
					attr = 'assert_not_called' if param == 0 else 'assert_called_once'
					getattr(obj, attr)()

		assert not hasattr(dec_funcs['df_df'], '_argh')
		assert dec_funcs['df_df'] != df_df

		# make sure debugger is called when an exception is raised
		with mock.patch('erde.PUDB', 1), mock.patch('pudb.post_mortem', mock.MagicMock()) as pm_debug:
			dec_funcs['crash'](df)

		pm_debug.assert_called_once()

		def bad1(input_data: read_stream, input_data2: read_stream):
			pass

		def bad2(input_data: gpd.GeoDataFrame):
			yield 1
			yield 2

		def bad3(input_data: read_stream) -> gpd.GeoDataFrame:
			return input_data

		for f in (bad1, bad2, bad3):
			print(f.__name__)
			with pytest.raises(ErdeDecoratorError):
				autocli(f)
