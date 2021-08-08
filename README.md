# Erde: Hiking GIS Library

This is a toolset to make minimum-boilerplate GIS processing scripts. It also contains some commonly used tools.

## Features

### Routing:

* build routes in large quantities: make geometries & get route geometries as GIS files
* table routes: take 2 datasets of N & M points and calculate all N\*M time/distances
* mass isochrones with OSRM: take N points and m travel durations, and get N\*m isochrones in 1 line

Examples: from command line:

    $ erde isochrone my_houses.gpkg foot 5,10,15 my_isochrones.gpkg

from code/Jupyter:

    from erde import isochrone
    areas_df = isochrone(houses_df, 'foot', [5, 10, 15])

### GIS-specific tools

* shortcuts for common usecases of sjoin: lookup, aggregate by geometry, and filter by geometry
* area/length/buffer in metres, all cleanup done under the hood
* CRS conversion

### Code organization

**chunk processing:** process large datasets in chunks, but still as dataframes, with @autocli or own code
**@autocli decorator** turns a function into a GIS-aware CLI app (without argparse pain)

## Example

[sample.py](examples/1_sample_py/sample.py)

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
