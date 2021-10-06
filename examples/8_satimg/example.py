from erde import autocli
from io import StringIO
from rasterio.warp import reproject
import numpy as np
import boto3
import geopandas as gpd
import os.path
import pandas as pd
import rasterio as ri
import rasterio.features, rasterio.mask
import re
import requests
import sys
import tempfile

options = {'verbose': False}


def vprint(*args, **kwargs):
	if options['verbose']:
		print(*args, **kwargs)


def get_tiles(geoms_df):
	headers = {'Accept':'application/json'}
	params = {
		'bbox': str(list(geoms_df.total_bounds)),
		'time': '2020-07-01T00:00:00Z/2020-07-31T23:59:59Z',
		'collection': 'sentinel-2-l1c',  # this option seems to be ignored, we filter collections later on
		'limit': 100
	}

	vprint(f'Requesting tiles for geometries. Params: {params}')
	resp = requests.get('https://sat-api.developmentseed.org/stac/search', params=params, headers=headers)

	rj = resp.json()
	# response info contains data polygon
	# we still need to check geometry intersection, because we requested bbox instead
	df = gpd.read_file(StringIO(resp.content.decode()))

	# assets are another sub-dict, so let's attach it too
	assets_df = pd.DataFrame([{k: v['href'] for k, v in f['assets'].items()} for f in rj['features']])
	for k in assets_df:
		df[k] = assets_df[k]

	return df[
		(df['collection'] == 'sentinel-2-l1c') &
		(df['eo:cloud_cover'] < 50) &  # maybe not worth?
		(df['geometry'].intersects(geoms_df.geometry.unary_union))].sort_values('eo:cloud_cover')  # get low-cloudy first


def read_keys(path):
	# read AWS keys file
	# unfortunately it comes in 2 flavors:
	# 1) 2 lines like "key=value"
	# 2) legitimate CSV
	# we check both options

	# format 1
	with open(path) as f:
		keys = dict(l.strip().split('=') for l in f)

	if ('AWSAccessKeyId' in keys and 'AWSSecretKey' in keys):
		return (keys['AWSAccessKeyId'], keys['AWSSecretKey'])

	# no, it wasn't format 1
	# format 2:
	df = pd.read_csv(path)
	r = df.loc[0]
	if not ('Access key ID' in r and 'Secret access key' in r):
		# it wasn't format 2 either, none matched, raising error
		print('format not supported.\n\nFormat #1: AWSAccessKeyId=...<newline>AWSSecretKey=...\n\nFormat #2: CSV table with header: Access key ID,Secret access key', file=sys.stderr)
		raise RuntimeError('AWS credentials file must be received from user keys page, or IAM users page and downloaded')

	return (r['Access key ID'], r['Secret access key'])


def fetch_image(url, keys=None):
	"""Downloads image either from S3 bucket, or directly by URL, then caches.

	To download from S3 bucket, must be called with keys tuple (key_id, secret_key).
	"""
	match = re.match(r'^https\://(?P<Bucket>.*).s3.amazonaws.com/(?P<Key>.*)$', url)

	if not match:
		# a jpg or json asset
		vprint(f'Downloading image from URL directly: {url}')
		return requests.get(url).content

	vprint(f'Downloading image from S3: {url}')
	tmp_path = tempfile.gettempdir() + '/' + match.groupdict()['Key'].replace('/', '-')
	if os.path.exists(tmp_path):
		vprint(f'Image was already cached in {tmp_path}')
		return ri.open(tmp_path)

	session = boto3.Session(*keys)
	client = session.client('s3') # , 'eu-north') <- not working
	vprint('Requester pays for this download!')
	content = client.get_object(RequestPayer='requester', **match.groupdict())['Body'].read()

	# we're saving the file to tmp folder to be able to both cache and read a window (accelerates reading from disk)
	with open(tmp_path, 'wb') as wf:
		wf.write(content)
		vprint(f'Cached the image in {tmp_path}')

	# we could have returnend a handle to a memfile, but that's more complexity
	return ri.open(tmp_path)


def reproj(a, b):
	"""Shorthand to reproject a data matrix from a dictionary [dataset, data, transformation] """

	data_a, ds_a = a
	data_b, ds_b = b
	return reproject(
		data_a, data_b.copy(),
		src_transform=ds_a.transform, src_crs=ds_a.crs,
		dst_transform=ds_b.transform, dst_crs=ds_b.crs)


def tile_stats(urls_dict, geom_row, keys):
	"""Statistics for tile. Separated for testing purposes.


	"""
	def get_ds(v):
		ds = fetch_image(urls_dict[v], keys)
		data, trans = ri.mask.mask(ds, geom_row.geometry.to_crs(ds.crs), crop=True)
		return data[0], ds

	water_data, water_ds = water = get_ds('B01')
	all_cells = np.sum((water_data > 0) * 1)  # there can be 9-filled no-data pixels

	"""This is an arbitrary filter for cloud coverage, made intentionally for test/demo purposes. The normal way would have been installing the whole s2cloudless package which is 1GB big. Plus it would have downloaded more than a gigabyte for each tile, because it requires almost all bands to classify pixels.

	For demo, I only use band 1 and set up an arbitrary threshold of value >2000 for cloud-covered pixels. This might not work in winter.

	With s2cloudless, this would have looked like downloading all tiles and calling that package, then getting a 2D np.array as a mask."""
	good_cells = np.sum(((water_data > 0) & (water_data < 2000)) * 1)

	if good_cells / all_cells < .8:  # TODO: mask is not taken into account
		return

	nir_data, nir_ds = nir = get_ds('B8A')

	# transform water matrix to hi-res (NIR)
	water_hr, water_hr_trans = reproj(water, nir)
	# arbitrary cloud filter again
	good_nir = (water_hr > 0) & (water_hr < 2000)

	# reprojecting low-res SWIR into hi-res
	swir_hr, swir_hr_trans = reproj(get_ds('B12'), nir)

	ndvi = (nir_data - swir_hr) / (nir_data + swir_hr)

	# filter out those cells that are cloudy and that return nan (warping produces overlapping pixels with near-zero or zero values that are not filtered out by clouds mask)
	good_ndvi = ndvi[good_nir & ~np.isnan(ndvi)]

	quantiles = np.array([.01, .05, .1, .25, .5, .95, .99])
	quantile_stat = dict(zip((quantiles * 100).astype(int), np.round(np.quantile(good_ndvi, quantiles), 2)))

	return {'Mean NDVI': np.round(good_ndvi.mean(), 3),
		'Median NDVI': quantile_stat[50],
		'Variance': np.round(good_ndvi.var(), 3),
		'Quantiles': quantile_stat,
		'Pixels in the area': len(nir_data[~np.isnan(nir_data)])
	}


@autocli
def main(geoms_df: gpd.GeoDataFrame, keys_path, date, verbose:bool=False):
	keys = read_keys(keys_path)
	options['verbose'] = verbose

	for i in range(len(geoms_df)):
		geom_row = geoms_df.iloc[i:i + 1]  # let's get row as df, to preserve useful to_crs and other properties
		vprint(f'Searching for tiles for area #{i}')

		tiles_df = get_tiles(geom_row)
		for img_num in range(len(tiles_df)):
			#for img_num in [0]:
			row = tiles_df.iloc[img_num].to_dict()
			stat_results = tile_stats(row, geom_row, keys)
			if not stat_results:
				continue

			print(f'Area #{i} results:')
			for k, v in stat_results.items():
				print(f'{k}:', v)
			print('')

			break
		else:
			print(f'No tiles or cloudless images for geometry {i}')
