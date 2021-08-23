"""OSM files filtering and conversion."""

from erde import autocli, dprint
from yaargh.decorators import arg
from yaargh.exceptions import CommandError

STEM = r'(?P<path>.*/)?(?P<stem>[^/\.]+)\.(?P<extention>[^\./]|osm(|\.pbf|\.gz|\.bz2|\.zip))$'
OSM_FILE = r'.*\.osm(\.(pbf|gz|bz2)|)$' # osm formats supported by libosmium: osm, osm.pbf, osm.gz, osm.bz2
DEFAULT_OGR_COLUMNS = {
	'points': 'name,highway,ref,address,is_in,place,man_made',
	'lines': 'name,highway,waterway,aerialway,barrier,man_made',
	'multipolygons': 'name,type,amenity,admin_level,barrier,boundary,building,landuse,natural',
	'multilinestrings': 'name,type',
	'other_relations': 'name,type'
}


def ogr_cmd(layers, columns, last_output, expected_output):
	from erde import io
	for k, drv in io.drivers.items():
		if drv.can_open(expected_output):
			output_format = k
			break
	else:
		raise CommandError(f'unknown format {expected_output}')

	ogr_layers = ' '.join(layers.split(','))

	extra_config = ''
	artifacts = []
	if columns is not None:

		for k in DEFAULT_OGR_COLUMNS:
			DEFAULT_OGR_COLUMNS[k] = ''

		ogr_tags = {**DEFAULT_OGR_COLUMNS}
		for t in columns:
			tagnames = t.split(',')
			if 'geometry' in tagnames:
				raise CommandError('tag "geometry" is not normally used in OSM, and this name is reserved for GeoPandas')
			if '=' in t:
				try:
					tk, tv = t.split('=')
					ogr_tags[tk] = tv
				except (KeyError, ValueError):
					raise CommandError(f'columns parameter usage:--columns <geom_type>=col1,col2, or --columns col1,col2. geom_type should be one of these: {", ".join(DEFAULT_OGR_COLUMNS)}')
			else:
				for k in ogr_tags.keys():
					ogr_tags[k] = t

		ogr_cfg_file = '/tmp/_3_osmcfg.ini'

		with open(ogr_cfg_file, 'w') as f:
			f.write('closed_ways_are_polygons=aeroway,amenity,boundary,building,building:part,craft,geological,historic,landuse,leisure,military,natural,office,place,shop,sport,tourism,highway=platform,public_transport=platform\n'
				'attribute_name_laundering=yes\n')
			for k, v in ogr_tags.items():
				f.write(f'\n[{k}]\nosm_id=yes\nattributes={v}\n')

		extra_config = f'--config OSM_CONFIG_FILE {ogr_cfg_file}'
		artifacts = [ogr_cfg_file]

	return f'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f {output_format} {expected_output} {last_output} {ogr_layers} {extra_config}', artifacts


def tags_cmd(tags, last_output, expected_output):
	return f'osmium tags-filter {last_output} ' + ' '.join(tags) + f' -o {expected_output}', None

def crop_cmd(crop, last_output, expected_output):
	"""Command to crop OSM file by area.

	>>> crop_cmd('my_area.geojson', 'file1.osm', 'file2.osm.pbf')
	'osmium extract file1.osm -o file2.osm.pbf -p "my_area.geojson"'
	"""
	return f'osmium extract {last_output} -o {expected_output} -p "{crop}"', None

def cat_cmd(last_output, expected_output):
	"""Command to convert or concat files.

	>>> cat_cmd('file1.osm', 'file2.osm.pbf')
	'osmium cat file1.osm -o file2.osm.pbf'
	>>> cat_cmd(['file1.osm', 'file2.osm.pbf'], 'file3.osm.gz')
	'osmium cat file1.osm file2.osm.pbf -o file3.osm.gz'
	>>> cat_cmd(['file1.osm', 'file2.osm.pbf'], ['file3.osm.gz'])
	'osmium cat file1.osm file2.osm.pbf -o file3.osm.gz'
	"""
	return f'osmium cat {last_output} -o {expected_output}', None


class Remove:
	"""Most commands are the same: osmium <args> or ogr2ogr <args>.
	Removing files is OS-specific and should be in code, but still printable for dry run. This object is both runnable and printable.

	>>> Remove('sample_path')
	Remove('sample_path')
	"""
	def __init__(self, path):
		self.path = path

	def __call__(self):
		# like os.system, it returns a status code
		# 0 = ok, other = fail
		import os
		try:
			if os.path.exists(self.path):
				os.remove(self.path)
		except OSError:
			return 1
		else:
			return 0

	def __repr__(self):
		return f"Remove('{self.path}')"


@autocli
@arg('--tags', action='append')
@arg('--columns', action='append')
def main(*filenames, layers='points,lines,multipolygons', tags=None, keep_tmp_files=False, columns=None, crop=None, dry:bool=False):
	"""
	Export OSM PBF dump to GPKG.

	Parameters
	==========

	filenames: str
		Path to OSM files, the last one is the output (could be OSM or GPKG)
	tags: [str]
		tags filter to tags. `--tags landuse=residential --tags natural=water --tags highway`
	crop: str
		Path to file with (multi)polygons to filter data by place.
	columns: [str]
		Tags to make as columns in the output file, will replace the original list of such columns. Format: `landuse,natural,construction` (same columns for all types) or `--columns points=highway,railway --columns lines=highway` (points and lines get different kinds of columns).
	l (layers): str, default: `points,lines,multipolygons`
		Comma-separated list of layers.
	k: (keep-tmp-files): bool, default `False`
		keep intermediary files in `/tmp/` after usage.
	d (dry): bool, default `False`
		Dry run, no execution.
	"""
	import os
	import re
	import sys
	from functools import partial
	from collections import defaultdict

	fns = len(filenames)
	if fns < 2:
		raise CommandError('Provide at least 2 filenames: <input file> <output file>, or multiple: <input1> <input2> ... <output>.')

	*input_paths, output_path = filenames
	for i, input_path in enumerate(input_paths):
		if not (input_path.endswith('.osm') or input_path.endswith('.osm.pbf')):
			raise CommandError(f'can\'t recognize format of {input_path}')
		if not os.path.exists(input_path):
			raise CommandError(f'file {input_path} does not exist')

	output_is_osm = re.match(OSM_FILE, output_path)
	many_inputs = len(input_paths) > 1

	stage_props = (
		(tags is not None,
			tags_cmd, (tags,), 'many', 'filtered.osm.pbf'),
		(crop is not None,
			crop_cmd, (crop,), 'many', 'cropped.osm.pbf'),
		(many_inputs or (tags is None and crop is None and output_is_osm),
			cat_cmd, (), 'many-one', 'osm.pbf'),
		(not output_is_osm,
			ogr_cmd, (layers, columns), 'one-one', 'gpkg'),
	)

	stages = [data for cond, *data in stage_props if cond]
	max_file = len(input_paths) - 1
	max_stage = len(stages) - 1

	cleanup = defaultdict(list)
	output = defaultdict(list)
	output[-1] = input_paths
	commands = []
	last_output = None

	# cycle through files, because we should walk through
	for i, last_output in enumerate(input_paths):
		last_file = (i == max_file)
		m = re.match(STEM, last_output)
		stem = m['stem']

		for j, (func, args, number, suffix) in enumerate(stages):
			last_stage = j == max_stage

			if number != 'many' and not last_file:
				break  # only on the last file should we go into cat and ogr stages

			if last_stage:
				expected_output = output_path
			elif func == cat_cmd:
				expected_output = f'/tmp/_{j}_cat.{suffix}'
			else:
				expected_output = f'/tmp/_{j}_{stem}.{suffix}'

			if number != 'many':
				last_output = ' '.join(output.pop(j - 1))
			cmd, artifacts = partial(func, *args)(last_output, expected_output)
			output[j].append(expected_output)
			last_output = expected_output

			commands += [cmd] + [Remove(a) for a in cleanup[j - 1]]
			cleanup[j - 1] = []
			if artifacts is not None: cleanup[j] += artifacts
			if not last_stage:
				cleanup[j].append(expected_output)

	commands.extend([Remove(la) for la in cleanup[j]])

	if dry:
		print('Dry run of erde osm. Generated commands:')
		for i, c in enumerate(commands):
			print(f'{i}: {c}')
	else:
		for c in commands:
			dprint(c)
			res = c() if callable(c) else os.system(c)
			if res != 0:
				print(f'error in command {c}')
				sys.exit(1)

	return commands  # return for testability
