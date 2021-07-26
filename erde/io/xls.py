from .base import BaseDriver
from erde.io import _try_gdf
import pandas as pd

class XlsDriver(BaseDriver):
	path_regexp = r'^(?P<path>.*\.xlsx?)(?:\:(?P<sheet>[a-z0-9_-]+))?$'

	@staticmethod
	def read_df(path, path_match, *args, **kwargs):
		path, sheet = path_match['path'], path_match['sheet']
		excel_dict = pd.read_excel(path, sheet_name=sheet, engine='openpyxl', **kwargs)  # OrderedDict of dataframes
		return _try_gdf(excel_dict.popitem()[1])  # pop item, last=False, returns (key, value) tuple

	@staticmethod
	def write_df(df, path, *args, **kwargs):
		raise NotImplementedError()

driver = XlsDriver
