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

def _read(*args, **kwargs):
	from .io import read_file
	return read_file(*args, **kwargs)

def _write(*args, **kwargs):
	from .io import write_file
	return write_file(*args, **kwargs)

# when you put these types in annotation, @command decorator will use these functions instead of the class instatiations
TYPE_OPENERS = {
	pd.DataFrame: _read,
	gpd.GeoDataFrame: _read
}


def command(func):
	"""
	Turns func into command-line script with (ya)argh. Automatically opens GeoDataFrames and DataFrames from supported file formats.

	If output type is pd.DataFrame/gpd.GeoDataFrame, the decorated function adds `output-file` argument (required) and automatically saves output to it.

	If you import func directly, leaves it as is, but stores for command line entry point.

	E.g. myscript.py:

		@command
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
				import pudb
				@contextmanager
				def handle_pudb():
					try:
						yield
					except Exception:
						pudb.post_mortem()

				stack.enter_context(handle_pudb())

			retval = func(*args, **kwargs)
			if retval is None:
				return

			args = parser.parse_args()
			if has_output_df:
				output_file = getattr(args, 'output-file')
				_write(retval, output_file)

		print(f'Total execution time {timedelta(seconds=time.time() - execution_start)}s')

	# check if frame is __main__
	frm = inspect.stack()[1]
	mod = inspect.getmodule(frm[0])
	if mod.__name__ == '__main__':
		# if so, it's command line call, check if output argument is needed
		if has_output_df:
			parser.add_argument('output-file')

		# wrap arguments if their types are in TYPE_OPENERS
		for k, par in sig.parameters.items():
			if par.default is inspect._empty and par.annotation is not inspect._empty:
				an = par.annotation
				func = yaargh.decorators.arg(par.name, type=TYPE_OPENERS.get(an, an), default=par.default)(func)

		yaargh.set_default_command(parser, func)
		yaargh.dispatch(parser)
		return

	# otherwise it's an import
	mod._argh = decorated
	return func
