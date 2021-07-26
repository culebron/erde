from shapely import wkt
import geopandas as gpd
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


def read_df(path, *args, **kwargs):
	for driver in drivers:
		path_match = driver.can_open(path)
		if path_match:
			return driver.read_df(path, path_match, *args, **kwargs)

	raise ValueError(f'{path}: file format not recognized')

def write_df(df, path, *args, **kwargs):
	for driver in drivers:
		path_match = driver.can_open(path)
		if path_match:
			return driver.write_df(df, path, path_match, *args, **kwargs)

	raise ValueError(f'{path}: file format not recognized')


def read_stream(path, chunk_size=1, pbar=False, sync=True):
	return []  # TODO: implement


from . import csv, gpkg, geojson, geojsonseq, postgres, shp, xls
drivers = [csv.driver, gpkg.driver, geojson.driver, geojsonseq.driver, postgres.driver, shp.driver, xls.driver]