# Erde: Hiking-Light GIS Library for Python

This library is a toolset to start projects light and quick, with minimum overhead or boilerplate, with no special hardware or software. Yet it allows scaling up and process large datasets.

## Features

1. Shorthands: read/write a GeoDataFrame from/to any format, write a single geometry into a geo-format file (no need to create a GeoDataFrame by hands).
2. Chunk-wise IO for multiple formats.
3. OSRM routing for large datasets: route lines, table routing, isochrones.
4. Create command-line scripts with one decorator. Your scripts may process entire files, or chunks, or even generate chunks.
5. Frequently used tools for GIS: area, length, etc.
6. Lookups, aggregations with spatial join.
7. Export or filter OSM files (.osm[.pbf|.gz|.bz2])

## Features in Details

### Shorthands

Instead of `gdf.to_file('path', driver='GeoJSON')` the library offers shorthand functions that recognize formats and layers:

	write_df(geodataframe1, 'file.geojson')
	write_df(geodataframe2, 'another-file.gpkg:special-layer')
	write_df(geodataframe3, 'third-file.csv')
	
GeoDataFrames are saved to/read from CSV files automatically, under the hood.

Save a single geometry object to a file:

	write_geom(polygon, 'single-polygon.csv')

### Easy Command-Line Scripts

This code creates an app that opens and saves files for you, converts types of parameters and makes help file. No more argparse hell!

	from erde import autocli
	from geopandas import GeoDataFrame as GDF

	@autocli
	def main(input_data: GDF, sample_size:float) -> GDF:
		return input_data.sample(sample_size)

call `python myscript.py` to see command line arguments.

See [the example](examples/2_minimal_cli_app/) for more code and instructions.

### Routing

* `erde route` takes a file with lines, treats them like waypoints, and outputs a file with original attributes, route geometries, and metadata: distance, duration, nodes.

		erde route input.gpkg car route_geoms.gpkg

Example datasets: input and output:

![datasets of routing directions and outputs](tests/route/reykjavik.jpg)

* `erde table` takes 2 datasets of N & M points and calculates all N\*M durations/distances between them.

		erde table houses.csv shops.csv car distance-matrix.gpkg

* `erde isochrone`  takes N points and m travel durations, and get N\*m isochrones in 1 line

Examples: from command line:

	$ erde isochrone my_houses.gpkg foot 5,10,15 my_isochrones.gpkg

from code/Jupyter:

	from erde import isochrone
	areas_df = isochrone(houses_df, 'foot', [5, 10, 15])

### OSM Export and Conversion

`erde osm` filters, crops by polygon and converts OSM files, and can merge several OSM files into one. It is a wrapper around [osmium-tool](https://osmcode.org/osmium-tool/manual.html) (up-to-date Ubuntu packages are available for [18.04LTS and newer](https://packages.ubuntu.com/source/bionic/osmium-tool)) and [GDAL ogr2ogr tool](https://gdal.org/programs/ogr2ogr.html) (Ubuntu users need to install `gdal-bin`)

Examples:

* Filter by tags:

		erde osm my-country.osm.pbf wr/highway my-country-highways.osm.pbf

* Filter by tags and crop by polygon:

		erde osm my-country.osm.pbf wr/highway my-city-highways.osm.pbf --crop my-city.geojson

* Convert to GeoPackage and extract only linestrings:

		erde osm my-country.osm.pbf wr/highway city-hw.gpkg --crop my-city.geojson -l lines

* Merge several files:

		erde osm country1.osm.pbf country2.osm.pbf country1-country2-hw.osm.pbf

* Filter by tag, merge and convert only linestrings:

		erde osm country1.osm.pbf country2.osm.pbf wr/highway country1-country2-hw.gpkg -l lines

### Spatial Joins and Aggregations

Most times, you need `gpd.sjoin` for 3 things:

* intersect geometries and aggregate some field from those objects in the other dataframe
* lookup another table (city/region) and get a field from there (size, name, domestic product, incomes, etc.)
* filter a dataframe by objects in the other one

Erde has 3 functions for those cases in [sjoin](./erde/op/sjoin.py) module: `slookup`, `sagg` and `sfilter`.

### GIS-specific Tools

* shortcuts for common usecases of sjoin: lookup, aggregate by geometry, and filter by geometry
* area/length/buffer in metres, all cleanup done under the hood
* CRS conversion


