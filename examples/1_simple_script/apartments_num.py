"""
To see a simple working demo:

	python3 apartments_num.py houses.geojson schools.geojson /tmp/output.geojson

To see full help message:

	python3 apartments_num.py -h

Or provide no arguments to see command arguments:

	python3 apartments_num.py
"""

from erde import autocli, write_df
from erde.op import sjoin, buffer
import geopandas as gpd

@autocli
def main(houses_df: gpd.GeoDataFrame, schools_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
	"""Calculates demand for each school (in apartments, assuming demography is the same)."""

	# Calculating house x school demand: divide number of apartments by the number of schools in 1 km radius.
	# Then take each school and sum the demand of houses within 1 km.
	# The @autocli API allows only one output dataframe, so we make only 1 output table.
	schools_df['school'] = schools_df.index.tolist()
	school_buf = buffer.main(schools_df.geometry, 1000, default_crs=4326)

	demand = sjoin.sgroup(houses_df, schools_df, {'school': 'count'}, right_on=school_buf)
	demand['apts_demand'] = demand.apartments / demand.school
	write_df(demand, '/tmp/h.geojson')
	return sjoin.sgroup(schools_df, demand, {'apts_demand': 'sum'}, left_on=school_buf)
