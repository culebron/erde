"""
This is a demo script that generates and yields dataframes one by one, rather than returning everything at once. Such behavior reduces memory requirements.

You can import the main function from another script and use it as a generator of GeoDataFrames, and either yield from that script, or concat them. See `other_script.py`.
"""

from erde import autocli, write_stream, utils
from erde.op.sjoin import sfilter
from shapely.geometry import Point
import geopandas as gpd
import sys
import numpy as np


@autocli
def main(polygons: gpd.GeoDataFrame, step:float, crop:bool=False) -> write_stream:
	"""Generates a rectangular grid of points in equal steps over polygon bounding box.

	Parameters
	----------
	polygons: GeoDataFrame (path in command-line)
	step: float
		Distance between points by X and Y in metres.
	crop: bool
		Keep only points that are within the polygons.
	"""
	if polygons.crs is None:
		polygons.crs = 4326
		print('Polygons CRS is None, assuming 4326 (lon/lat)', file=sys.stderl)

	polygons['coslat'] = utils.coslat(polygons.geometry)
	for i, r in polygons.to_crs(3857).iterrows():
		x1, y1, x2, y2 = r['geometry'].bounds
		step_local = step / r.coslat
		grid = np.mgrid[x1:x2 +.1:step_local, y1:y2 + .1:step / r.coslat].T
		gs = grid.shape
		
		gdf = gpd.GeoDataFrame({'geometry': [Point(*i) for i in grid.reshape(gs[0] * gs[1], gs[2])]}, crs=3857).to_crs(4326)
		gdf['polygon'] = i
		if crop:
			gdf = sfilter(gdf, r['geometry'])

		yield gdf
