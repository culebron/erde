# Command-line script

This is the same as example #1, but with CLI utility.

To see a simple working demo:

	python3 apartments_num.py houses.geojson schools.geojson /tmp/output.geojson

To see full help message:

	python3 apartments_num.py -h

Or provide no arguments to see command arguments:

	python3 apartments_num.py

This function can be reused in other scripts later.

## Minimal boilerplate code

	from erde import autocli
	from geopandas import GeoDataFrame as gdf

	@autocli
	def main(input_df: gdf) -> gdf:
		return input_df
