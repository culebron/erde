"""OSM files filtering and conversion."""

from erde import autocli, dprint
from yaargh.decorators import arg
from yaargh.exceptions import CommandError

OSM_FILE = r'.*\.osm(\.(pbf|gz|bz2))$' # osm formats supported by libosmium: osm, osm.pbf, osm.gz, osm.bz2
DEFAULT_OGR_COLUMNS = {
	'points': 'name,highway,ref,address,is_in,place,man_made',
	'lines': 'name,highway,waterway,aerialway,barrier,man_made',
	'multipolygons': 'name,type,amenity,admin_level,barrier,boundary,building,landuse,natural',
	'multilinestrings': 'name,type',
	'other_relations': 'name,type'
}


def ogr_cmd(layers, columns, cmd_input, cmd_output):
	from erde import io
	for k, drv in io.drivers.items():
		if drv.can_open(cmd_output):
			output_format = k
			break
	else:
		raise CommandError(f'unknown format {cmd_output}')

	ogr_layers = ' '.join(layers.split(','))

	extra_config = ''
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
					ogr_tags[tk] += ',' + tv
				except (KeyError, ValueError):
					raise CommandError(f'add-columns parameter usage:--columns key=value, or --columns value. Key should be one of these: {", ".join(DEFAULT_OGR_COLUMNS)}')
			else:
				for k in ogr_tags.keys():
					ogr_tags[k] += ',' + t

		ogr_cfg_file = '/tmp/_3_osmcfg.ini'
		with open(ogr_cfg_file, 'w') as f:
			f.write('closed_ways_are_polygons=aeroway,amenity,boundary,building,building:part,craft,geological,historic,landuse,leisure,military,natural,office,place,shop,sport,tourism,highway=platform,public_transport=platform\n'
				'attribute_name_laundering=yes\n')
			for k, v in ogr_tags.items():
				f.write(f'\n[{k}]\nosm_id=yes\nattributes={v}\n')

		extra_config = f'--config OSM_CONFIG_FILE {ogr_cfg_file}'

	return f'ogr2ogr --config OSM_USE_CUSTOM_INDEXING NO -gt 65535 -f {output_format} {cmd_output} {cmd_input} {ogr_layers} {extra_config}'


def filter_cmd(tags, cmd_input, cmd_output):
	import re
	tags_filter = ' '.join(w for k in tags for w in re.split(r'\s+', k))
	return f'osmium tags-filter {cmd_input} {tags_filter} -o {cmd_output}'


def crop_cmd(crop, cmd_input, cmd_output):
	return f'osmium extract {cmd_input} -o {cmd_output} -p "{crop}"'


def cat_cmd(cmd_input, cmd_output):
	return f'osmium cat {cmd_input} -o {cmd_output}'


class Remove:
	def __init__(self, path):
		self.path = path

	def __call__(self):
		import os
		try:
			if os.path.exists(self.path):
				os.remove(self.path)
		except OSError:
			return 1
		else:
			return 0

	def __str__(self):
		return f'remove {self.path} file'


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
	from pathlib import Path
	from functools import partial

	fns = len(filenames)
	if fns < 2:
		raise CommandError('Provide at least 2 filenames: <input file> <output file>, or multiple: <input1> <input2> ... <output>.')

	*input_paths, output_path = filenames
	for i, input_path in enumerate(input_paths):
		if not (input_path.endswith('.osm') or input_path.endswith('.osm.pbf')):
			raise CommandError(f'can\'t recognize format of {input_path}')
		if not os.path.exists(input_path):
			raise CommandError(f'file {input_path} does not exist')

	# partial(ogr_cmd, layers, columns), f'/tmp/_{stem}.gpkg'
	chain = [(None, input_paths)]
	stems = [Path(p).stem for p in input_paths]

	if bool(crop):
		chain.append([partial(crop_cmd, crop), [f'/tmp/_{s}_cropped.osm.pbf' for s in stems]])
	if tags is not None:
		chain.append([partial(filter_cmd, tags), [f'/tmp/_{s}_filtered.osm.pbf' for s in stems]])

	if len(input_paths) > 1:
		chain.append([cat_cmd, ['/tmp/_concat.osm.pbf']])

	if not re.match(OSM_FILE, output_path):
		chain.append([partial(ogr_cmd, layers, columns), [output_path]])

	chain[-1][1] = [output_path]

	final_commands = []
	for j, ip in enumerate(input_paths):
		last_artifact = None
		last_out = None
		for i, (cmd, output_paths) in enumerate(chain):
			if i == 0: # first item is [None, input_paths]
				last_out = output_paths
				continue

			if len(output_paths) == 1:  # concat of multiple into one
				break

			cleanup_prev = None
			if dry:
				cleanup_next = f'rm {output_paths[j]}'
				command = cmd(last_out[j], output_paths[j])
				if last_artifact:
					cleanup_prev = f'rm {last_artifact[j]}'
			else:
				cleanup_next = Remove(output_paths[j])
				command = cmd(last_out[j], output_paths[j])
				if last_artifact:
					cleanup_prev = Remove(last_artifact[j])

			# cleanup target file before writing to it
			final_commands.extend([cleanup_next, command])
			if cleanup_prev is not None and not keep_tmp_files:
				final_commands.append(cleanup_prev)

			last_artifact = last_out = output_paths

	for i, (cmd, output_paths) in enumerate(chain):
		if len(output_paths) > 1:
			last_out = output_paths
			continue

		if cmd is None:
			last_out = output_paths
			continue

		j = 0
		cleanup_prev = None
		if dry:
			cleanup = f'rm {output_paths[j]}'
			command = cmd(last_out[j], output_paths[j])
			if last_artifact:
				cleanup_prev = f'rm {last_out[j]}'
		else:
			cleanup = Remove(output_paths[j])
			command = cmd(last_out[j], output_paths[j])
			if last_artifact:
				cleanup_prev = Remove(last_artifact[j])

		final_commands.extend([cleanup, command])
		if cleanup_prev and not keep_tmp_files:
			final_commands.append(cleanup_prev)

		last_artifact = last_out = output_paths

	if dry:
		print('Dry run of erde osm. Generated commands:')
		for i, c in enumerate(final_commands):
			print(f'{i}: {c}')
	else:
		for c in final_commands:
			dprint(c)
			res = c() if callable(c) else os.system(c)
			if res != 0:
				print(f'error in command {c}')
				sys.exit(1)
