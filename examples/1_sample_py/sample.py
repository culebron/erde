from erde import autocli
from geopandas import GeoDataFrame as GDF

@autocli
def main(input_data: GDF, sample_size:float) -> GDF:
	return input_data.sample(sample_size)
