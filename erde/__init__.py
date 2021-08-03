from contextlib import ExitStack, contextmanager
from datetime import timedelta
from functools import wraps
import geopandas as gpd
import inspect
import os
import pandas as pd
import time
import yaargh


ENV_VARS = {'ELOG': 0, 'IPDB': 0, 'PUDB': 0, 'ECACHE': None, 'ESYNC': 0}
ELOG, IPDB, PUDB, ECACHE, ESYNC = [os.environ.get(k, v) for k, v in ENV_VARS.items()]


def dprint(*args, **kwargs):
	if ELOG == '1':
		print(*args, **kwargs)


def read_df(path, *args, **kwargs):
	"""Reads the entire file/db table into (Geo)DataFrame.

	Parameters
	----------
	path : str
		Path to file or db/table URL
	args, kwargs
		Arguments forwarded to driver function (gpd.read_file, pd.read_csv, etc.)
	"""
	from .io import select_driver
	dr, pm = select_driver(path)
	return dr.read_df(path, pm, *args, **kwargs)


def write_df(df, path, *args, **kwargs):
	"""Writes entire (Geo)DataFrame into file (layer in a file) and closes it. By default, if file/layer exists, it's overwritten. Drivers like GPKG may support append mode, you need to pass such argument as a keyword argument.

	Parameters
	----------

	df : pd.DataFrame, gpd.GeoDataFrame
		Dataframe to write.
	path : str
		Target path.
	args, kwargs
		Parameters passed to driver function.
	"""
	from .io import select_driver
	dr, pm = select_driver(path)
	dr.write_df(df, path, pm, *args, **kwargs)


# when you put these types in annotation, @autocli decorator will use these functions instead of the class instatiations
TYPE_OPENERS = {
	pd.DataFrame: read_df,
	gpd.GeoDataFrame: read_df
}

@contextmanager
def _handle_pudb():
	# pudb exception handler, put here to make patching possible for tests.
	import pudb
	try:
		yield
	except Exception:
		pudb.post_mortem()


def autocli(func):
	"""
	Turns func into command-line script with (ya)argh. Automatically opens GeoDataFrames and DataFrames from supported file formats.

	If output type is pd.DataFrame/gpd.GeoDataFrame, the decorated function adds `output-file` argument (required) and automatically saves output to it.

	If you import func directly, leaves it as is, but stores for command line entry point.

	E.g. myscript.py:

		@autocli
		def main(input_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
			return input_df

	Running:

		$ python3 myscript.py

	The script should exit with error and print help text.

	Decorated function also can catch exceptions if an env variable is set:

		$ IPDB=1 python3 myscript.py source.gpkg target.gpkg

	If there's an exception, you'll see IPDB shell to debug the error immediately. Note: it can't work with multiprocessing apps.

	If you import the function or entire module, it is not altered:

		> from myscript.py import main
		> my_df == main(my_df)
		True

	"""

	sig = inspect.signature(func)
	has_output_df = sig.return_annotation in (pd.DataFrame, gpd.GeoDataFrame)
	parser = yaargh.ArghParser()

	@wraps(func)
	def decorated(*args, **kwargs):
		execution_start = time.time()

		with ExitStack() as stack:
			# enter debuggers stack if an env vars is set
			if IPDB == 1:
				import ipdb
				stack.enter_context(ipdb.slaunch_ipdb_on_exception())
			elif PUDB == 1:
				stack.enter_context(_handle_pudb())

			retval = func(*args, **kwargs)
			if retval is None:
				return

			if has_output_df:
				args = parser.parse_args()
				output_file = getattr(args, 'output-file')
				write_df(retval, output_file)

		print(f'Total execution time {str(timedelta(seconds=time.time() - execution_start))[:-5]}s')

	frm = inspect.stack()[1]
	mod = inspect.getmodule(frm[0])
	for k, par in sig.parameters.items():
		an = par.annotation
		if an is not inspect._empty:  # par.default is inspect._empty and   < removed.
			if par.default is not inspect._empty:
				names = ['-' + par.name[0], '--' + par.name]
			else:
				names = [par.name]
			decorated = yaargh.decorators.arg(*names, type=TYPE_OPENERS.get(an, an))(decorated)

	if has_output_df: # add output-file as the last argument
		parser.add_argument('output-file')

	# check if frame is __main__
	if mod.__name__ == '__main__':
		# if so, it's command line call, check if output argument is needed
		# wrap arguments if their types are in TYPE_OPENERS
		yaargh.set_default_command(parser, decorated)
		yaargh.dispatch(parser)
		return decorated

	# otherwise it's an import
	func._argh = decorated
	return func


def read_stream(path, geometry_filter=None, chunk_size=10_000, pbar=False, sync=True, *args, **kwargs):
	"""Creates a reader object to read files/databases in chunks as dataframes.

	Parameters
	----------
	path : str
		path to a file or a table in a database. The format is detected automatically from the path (see `erde.io` for supported drivers).
	geometry_filter : optional, shapely.geometry or path to file (will be opened with read_df at once), or GeoSeries, or GeoDataFrame.
		Geometries to filter the opened source objects.
	chunk_size : int, default 10_000
		Maximum number of rows in each dataset.
	pbar : bool, default False
		Show progress bar.
	sync : bool, default True
		Don't create a new process, read the file in the main process.

	`args` and `kwargs` are passed to drivers, see modules in erde.io.

	To run a reader in a parallel process, use it as a context manager:

		with read_stream(path) as reader:
			for df in reader: ...

	This can be overridden by `sync=True` option:

		with read_stream(path, sync=True) as reader:
			for df in reader: ...

	If reader is iterated directly, it will work in the same process:

		for df in read_stream(path): ...
	"""
	from .io import select_driver
	dr, pm = select_driver(path)
	return dr.read_stream(path, geometry_filter, chunk_size, pbar, sync, *args, **kwargs)


def write_stream(path, sync=True, *args, **kwargs):
	"""Creates a writer object (context manager) to write multiple dataframes into one file. Must be used as context manager.

	Parameters
	----------
	path : str, filename or path to database table
	sync : bool, default True
		Set to `False` to run the writer in the background process.
	args, kwargs : parameters passed to writer driver (see erde.io modules)

	Example:

		with write_stream('/tmp/my_file.gpkg') as write:
			for df in data_generator():
				write(df)
	"""
	from .io import select_driver
	dr, pm = select_driver(path)
	return dr.write_stream(path, sync=sync, *args, **kwargs)


commands = ['buffer', 'convert']

import importlib

raw_funcs = {i: importlib.import_module(f'erde.op.{i}').main for i in commands}

__all__ = []
# creating import shortcuts for commands, e.g.: `erde.op.buffer.main` => `erde.buffer`
# note for devs: this imports all modules in op, hence they should not import many other libraries in module root. A lazy import would work, but it'll hide funcs signatures.
for k, v in raw_funcs.items():
	globals()[k] = v
	__all__.append(k)

def entrypoint():
	import yaargh

	p = yaargh.ArghParser()
	yaargh.add_commands(p, [yaargh.decorators.named(k)(v._argh) for k, v in raw_funcs.items()])
	yaargh.dispatch(p)
