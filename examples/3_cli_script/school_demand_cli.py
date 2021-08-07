from erde import autocli
from erde.op import sjoin, buffer
from geopandas import GeoDataFrame as gdf

@autocli
def main(houses_df: gdf, schools_df: gdf) -> gdf:
	schools_df['school'] = schools_df.index.tolist()
	school_buf = buffer.main(schools_df.geometry, 1000, default_crs=4326)

	demand = sjoin.sagg(houses_df, schools_df, {'school': 'count'}, right_on=school_buf)
	demand['apts_demand'] = demand.apartments / demand.school
	return sjoin.sagg(schools_df, demand, {'apts_demand': 'sum'}, left_on=school_buf)
