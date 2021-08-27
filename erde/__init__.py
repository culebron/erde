from contextlib import ExitStack, contextmanager
from datetime import timedelta
import geopandas as gpd
import os
import pandas as pd
import time
import yaargh

from .cfg import CONFIG


ELOG = IPDB = PUDB = ECACHE = ESYNC = None
DEFAULT_ENV_VARS = {'ELOG': 0, 'IPDB': 0, 'PUDB': 0, 'ECACHE': None, 'ESYNC': 0}

for k, v in DEFAULT_ENV_VARS.items():
	var = os.environ.get(k, v)
	try:
		var = int(var)
	except:
		pass
	globals()[k] = var


def dprint(*args, **kwargs):
	"""Stub for appropriate logging."""
	if ELOG == '1':
		print(*args, **kwargs)


class ErdeDecoratorError(Exception):
	"""A separate class to distinguish misconfiguration discovered when decorating functions."""
	pass


@contextmanager
def debug_capture():
	"""Handles all exceptions with IPDB or PUDB debuggers."""
	with ExitStack() as stack:
		# enter debuggers stack if an env vars is set
		if IPDB == 1:
			import ipdb
			stack.enter_context(ipdb.slaunch_ipdb_on_exception())
		elif PUDB == 1:
			stack.enter_context(_handle_pudb())

		yield stack


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


def read_geom(path, *args, **kwargs):
	"""Reads first geometry from a file (whichever format openable by read_df, including CSV with WKT).

	First geometry is read to prevent heavy unary_union operation."""
	return read_df(path, *args, **kwargs).geometry.values[0]


def write_geom(geometry, path, crs=None, *args, **kwargs):
	"""Writes a single geometry to a file, a shorthand to skip creating a GeoDataFrame."""
	write_df(gpd.GeoDataFrame({'geometry': [geometry]}, crs=crs), path, *args, **kwargs)


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


@contextmanager
def _handle_pudb():
	"""A context manager to capture errors with PUDB, which does not have such feature."""
	# it's put in module global to make it patcheable for tests.
	import pudb
	try:
		yield
	except Exception:
		pudb.post_mortem()


# when you put these types in annotation, @autocli decorator will use these functions instead of the class instatiations
TYPE_OPENERS = {
	pd.DataFrame: read_df,
	gpd.GeoDataFrame: read_df
}


def autocli(func):
	"""
	Turns func into command-line script with (ya)argh. Automatically opens GeoDataFrames and DataFrames from supported file formats.

	If output type is pd.DataFrame/gpd.GeoDataFrame, the decorated function adds `output-path` argument (required) and automatically saves output to it.

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

	from functools import wraps
	import inspect

	sig = inspect.signature(func)
	has_output_df = sig.return_annotation in (pd.DataFrame, gpd.GeoDataFrame)
	has_output_stream = sig.return_annotation == write_stream
	subparser = yaargh.ArghParser()

	@wraps(func)
	def decorated(*args, **kwargs):
		execution_start = time.time()

		with debug_capture() as stack:
			"""
			all possible combos of input/output and function/generator (2*2*3 == 12):
			input:  2: one call (with any # of df), stream
			func:   2: normal func, generator
			output: 3: nothing, df, stream

			1)  one    func nothing = ok
			2)  one    func df      = ok
			3)  one    func stream  = ok, (will write 1ce to stream)
			4)  one    gen  nothing = FORBIDDEN
			5)  one    gen  df      = FORBIDDEN
			6)  one    gen  stream  = ok

			7)  stream func nothing = ok
			8)  stream func df      = FORBIDDEN
			9)  stream func stream  = ok
			10) stream gen  nothing = FORBIDDEN
			11) stream gen  df      = FORBIDDEN
			12) stream gen  stream  = ok

			remaning combos still too different to fit into if/else tree.
			we make 3 layers (reader, func and writer):
				reader/single df => generator,
				func/generator => generator,
				writer => callable object or a (dummy) func

			"""
			if has_output_stream or has_output_df:
				known_args = subparser.parse_known_args()
				output_path = getattr(known_args[0], 'output-path', None) or known_args[1][-1]
				kwargs.pop('output-path', None)  # output-path leaks into kwargs when it's added to parser, so pop it from there just in case

			if input_streams == 1:
				def reader():
					with read_stream(args[stream_arg_id], sync=False) as rd:
						for df in rd:
							args2 = list(args)
							args2[stream_arg_id] = df
							yield args2
			else:
				def reader():
					yield args

			if has_output_stream:
				# df -> stream, stream -> stream
				writer = stack.enter_context(write_stream(output_path, sync=False))
			elif has_output_df:
				writer = lambda df: write_df(df, output_path)
			else:
				writer = lambda df: None

			for args2 in reader():
				retval = func(*args2, **kwargs)
				retval = retval if inspect.isgeneratorfunction(func) else [retval]

				for df in retval:
					writer(df)

		import sys
		print(f'Total execution time {str(timedelta(seconds=time.time() - execution_start))[:-5]}s', file=sys.stderr)

	frm = inspect.stack()[1]
	mod = inspect.getmodule(frm[0])

	input_streams = 0
	stream_arg_id = None

	for i, (k, par) in enumerate(sig.parameters.items()):
		an = par.annotation
		if an is not inspect._empty:  # par.default is inspect._empty and   < removed.
			if an == read_stream:  # streaming cli app
				input_streams += 1  # must count number of read_stream, as only 1 is allowed
				stream_arg_id = i
				continue

			# argument with default vaulue = optional, & it must start with dashes
			# otherwise its considered positional (required), and that contradiction causes an exception
			if par.default is not inspect._empty:
				names = ['-' + par.name[0], '--' + par.name]
			else:
				names = [par.name]

			if an != bool:
				decorated = yaargh.decorators.arg(*names, type=TYPE_OPENERS.get(an, an))(decorated)

	if input_streams > 1:
		raise ErdeDecoratorError(f'Argument of read_stream type can be only one, got {input_streams} instead')

	if inspect.isgeneratorfunction(func) and not has_output_stream:
		# case 4, 5, 10 & 11
		raise ErdeDecoratorError('If function is a generator, it must have return type as `write_stream`: `def main(...) -> write_stream:`.')

	if input_streams == 1 and has_output_df:
		# case 8 & 11
		raise ErdeDecoratorError('function decorated with @autocli has read_stream in input, but a single dataframe in output. Such combination is currently forbidden. Reduce capability will be added later with a separate decorator. You may change return annotation to `write_stream`.')

	if mod.__name__ == '__main__':
		yaargh.set_default_command(subparser, decorated)
		# output-path should be added here, to be the last argument
		if has_output_stream or has_output_df:
			subparser.add_argument('output-path')

		yaargh.dispatch(subparser)
		return decorated  # returning for test code to check the decorated function

	# otherwise it's an import
	func._argh = decorated
	func._has_output = has_output_stream or has_output_df
	return func


commands = ['buffer', 'convert', 'area', 'length', 'route', 'table', 'osm']

import importlib

raw_funcs = {i: importlib.import_module(f'erde.op.{i}').main for i in commands}

__all__ = ['CONFIG']
# creating import shortcuts for commands, e.g.: `erde.op.buffer.main` => `erde.buffer`
# note for devs: this imports all modules in op, hence they should not import many other libraries in module root. A lazy import would work, but it'll hide funcs signatures.
for k, v in raw_funcs.items():
	globals()[k] = v
	__all__.append(k)


def entrypoint():
	import yaargh
	p = yaargh.ArghParser()
	spa = p.add_subparsers()
	for k, v in raw_funcs.items():
		p1 = spa.add_parser(k)
		p1.set_default_command(yaargh.decorators.named(k)(v._argh))
		# we must patch the arguments here for output-path, in the subparser of the command, before it's dispatched
		# otherwise, output-path won't be in positional args
		if v._has_output:
			p1.add_argument('output-path')

	yaargh.dispatch(p)
