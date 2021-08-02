# Example #2: Command-Line Script

This is the same as example #1, but with CLI utility.

## Try Running It

	python3 school_demand_cli.py houses.csv schools.csv /tmp/demand.geojson

To see full help message:

	python3 apartments_num.py -h

Or provide no arguments to see command arguments:

	python3 apartments_num.py

This function can be reused in other scripts later.

## Minimal Boilerplate Code

	from erde import autocli
	from geopandas import GeoDataFrame as gdf

	@autocli
	def main(input_df: gdf) -> gdf:
		return input_df
