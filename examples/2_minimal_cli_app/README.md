# One-Line Script

[sample.py](sample.py) script is an example of minimal boilerplate.

	from erde import autocli
	from geopandas import GeoDataFrame as GDF

	@autocli
	def main(input_data: GDF, sample_size:float) -> GDF:
		return input_data.sample(sample_size)

Usage: If you're unaware how to run a script, simply do this and you'll see the arguments/options:

	$ python3 sample.py
	usage: sample.py [-h] input_data sample_size output-path
	sample.py: error: the following arguments are required: input_data, sample_size, output-path

or, to print more verbose help:

	$ python3 sample.py -h

Actual call:

    $ python3 sample.py all_data.geojson 0.5 50_percent_data.csv
