# Sentinel 2 Processing Test

This is a demo made as a test excercise. It extracts satellite images for areas in an arbitrary file, downloads Sentinel 2 tiles and calculates NDVI (normalized difference vegetation index) for the given areas.

## Prerequisites

### Required Libraries

* [`requirements.txt`](https://github.com/culebron/erde/blob/master/requirements.txt): among these, GeoPandas requires binary libraries: `libgeos-dev libspatialindex-dev gdal-bin libgdal-dev`
* [rasterio](https://rasterio.readthedocs.io/en/latest/installation.html)
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)

### AWS Access Key

To run the demo, you need to have an AWS user. Downloading the image tiles is a paid service, so don't run it carelessly.

Log into the control panel and generate a key for either your own user, or an IAM user (it has limited access to improve security). The key can be generated in 2 different formats, but both will work in this app.

Put the key in this folder.

## Test Run

Running from system shell:

    $ python3 example.py test_areas.geojson 2020-07-15 rootkey.csv

Expected output:

	Area #0 results:
	Mean NDVI: 2.499
	Median NDVI: 0.32
	Variance: 319.328
	Quantiles: {1: 0.01, 5: 0.06, 10: 0.11, 25: 0.24, 50: 0.32, 95: 18.79, 99: 29.57}
	Pixels in the area: 123709

	Area #1 results:
	Mean NDVI: 0.522
	Median NDVI: 0.36
	Variance: 9.549
	Quantiles: {1: 0.23, 5: 0.28, 10: 0.3, 25: 0.33, 50: 0.36, 95: 0.41, 99: 0.45}
	Pixels in the area: 55167

## Self-Help in Command-Line

If you forgot the command line arguments, just run

    $ python3 example.py

and it will print a compact list of them. Or

    $ python3 example.py -h

to see the full help text.

## Known Limitations

The script just checks the least clouded image in the span timeline, so if it's long, the best image may be much earlier. A complete app would require downloading a set of images, extracting the areas (may be partially if images are half-clouded), and then interpolating the NDVI values. This is a complex task out of the scope of the test.

## How It Can Be Extended

Current researches of vegetation and terrain disruptions use interpolated (modelled) NDVI over the course of the year. [Example 1](https://sciendo.com/pdf/10.2478/forj-2019-0020), [example 2](https://www.hindawi.com/journals/amete/2015/725427/).

So, the next logical step would be to get the better images over the course of the year, extract their cloud-free pieces and calculate their NDVI.

We can actually generate a fine-grained matrix, then warp all the imagery on them, and record all cloud-free values for each pixel, and then interpolate each pixel over time. This would give us a better mean/median NDVI for an area.

