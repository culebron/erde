from . import check_path_exists
from .base import BaseDriver
from erde.io import _try_gdf
import pandas as pd

class XlsDriver(BaseDriver):
	path_regexp = r'^(?P<path>.*\.xlsx?)(?:\:(?P<sheet>[a-z0-9_-]+))?$'

	@classmethod
	def read_df(cls, path, path_match, *args, **kwargs):
		check_path_exists(path)
		path, sheet = path_match['path'], path_match['sheet']
		excel_dict = pd.read_excel(path, sheet_name=sheet, engine='openpyxl', **kwargs)  # OrderedDict of dataframes
		return _try_gdf(excel_dict.popitem()[1])  # pop item, last=False, returns (key, value) tuple

	@classmethod
	def write_df(cls, df, path, *args, **kwargs):
		raise NotImplementedError()

driver = XlsDriver
