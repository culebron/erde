"""A sample script that builds isochrones and calculates their area, and distance reachability (isochrone area / (duration ** 2))."""

from erde import area, autocli, isochrone, write_stream
import geopandas as gpd

@autocli
def main(sources: gpd.GeoDataFrame, router='local', durations='5,10,15', speed:int=5) -> write_stream:
	for iso_gdf in isochrone(sources, router, durations, speed):
		iso_gdf = area(iso_gdf)
		iso_gdf['reach_index'] = iso_gdf['area'] / (iso_gdf['duration'] ** 2) / 1000
		import numpy as np
		iso_gdf['area'] = np.round(iso_gdf['area'] / 10_000, 1)  # convert square metres into hectares
		yield iso_gdf
