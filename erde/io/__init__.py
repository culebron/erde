from shapely import wkt
import geopandas as gpd
import os
import sys


def _try_gdf(df, geometry_columns=('geometry', 'WKT'), crs=None):
	from shapely.errors import WKTReadingError
	if isinstance(geometry_columns, str):
		geometry_columns = [geometry_columns]

	geometry_ok = False
	for k in geometry_columns:
		if k in df:
			try:
				df['geometry'] = df[k].apply(wkt.loads)
			except (TypeError, AttributeError, WKTReadingError):
				print("warning: can't transform empty or broken geometry", file=sys.stderr)
			else:
				if k != 'geometry': df.pop(k)
				geometry_ok = True
				break

	if geometry_ok:
		return gpd.GeoDataFrame(df, crs=crs)

	return df


def select_driver(path):
	for driver in drivers.values():
		path_match = driver.can_open(path)
		if path_match:
			return driver, path_match
	else:
		raise ValueError(f'{path}: file format not recognized')


def check_path_exists(path):
	if not os.path.exists(path):  # immediately raise error to avoid crashing much later
			raise FileNotFoundError(f'file {path} does not exist')

from . import csv, fgb, gpkg, geojson, geojsonseq, postgres, shp, xls
drivers = {'csv': csv.driver, 'fgb': fgb.driver, 'gpkg': gpkg.driver, 'geojson': geojson.driver, 'geojsonl.json': geojsonseq.driver, 'postgres': postgres.driver, 'shp': shp.driver, 'xls': xls.driver}
