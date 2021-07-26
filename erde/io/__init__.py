from shapely import wkt
import geopandas as gpd
import os
import sys


def _try_gdf(df, geometry_columns=('geometry', 'WKT'), crs=None):
	if isinstance(geometry_columns, str):
		geometry_columns = [geometry_columns]

	geometry_ok = False
	for k in geometry_columns:
		if k in df:
			try:
				df['geometry'] = df[k].apply(wkt.loads)
				if k != 'geometry': df.pop(k)
			except (TypeError, AttributeError):
				print("warning: can't transform empty or broken geometry", file=sys.stderr)
			else:
				geometry_ok = True

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


def read_df(path, *args, **kwargs):
	dr, pm = select_driver(path)
	return dr.read_df(path, pm, *args, **kwargs)


def write_df(df, path, *args, **kwargs):
	dr, pm = select_driver(path)
	dr.write_df(df, path, pm, *args, **kwargs)


def read_stream(path, geometry_filter=None, chunk_size=1, pbar=False, sync=True, *args, **kwargs):
	dr, pm = select_driver(path)
	return dr.read_stream(path, geometry_filter, chunk_size, pbar, sync, *args, **kwargs)


def write_stream(path, sync=True, *args, **kwargs):
	dr, pm = select_driver(path)
	return dr.write_stream(path, sync=sync, *args, **kwargs)


def check_path_exists(path):
	if not os.path.exists(path):  # immediately raise error to avoid crashing much later
			raise FileNotFoundError(f'file {path} does not exist')

from . import csv, gpkg, geojson, geojsonseq, postgres, shp, xls
drivers = {'csv': csv.driver, 'gpkg': gpkg.driver, 'geojson': geojson.driver, 'geojsonl.json': geojsonseq.driver, 'postgres': postgres.driver, 'shp': shp.driver, 'xls': xls.driver}
