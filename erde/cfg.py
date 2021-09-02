import os
import yaml

PROJECT_CONFIG_PATH = os.path.join(os.curdir, 'erde.yml')
USER_CONFIG_PATH = os.path.join(os.path.expanduser("~"), '.erde.yml')

# to override this config, put .erde.yml into your home folder
CONFIG = {
	'routers': {
		'local': 'http://localhost:5000'
	}
}


# code by Schlomo https://stackoverflow.com/a/15836901/171278
class MergeError(Exception):
	pass

def data_merge(a, b):
	"""merges b into a and return merged result

	NOTE: tuples and arbitrary objects are not handled as it is totally ambiguous what should happen"""
	key = None
	try:
		if a is None or isinstance(a, (str, int, float)):
			a = b
		elif isinstance(a, list):
			if isinstance(b, list):
				a.extend(b)
			else:
				a.append(b)
		elif isinstance(a, dict):
			if isinstance(b, dict):
				for key in b:
					a[key] = data_merge(a.get(key, None), b[key])
			else:
				raise MergeError('Cannot merge non-dict "%s" into dict "%s"' % (b, a))
		else:
			raise MergeError('NOT IMPLEMENTED "%s" into "%s"' % (b, a))
	except TypeError as e:
		raise MergeError('TypeError "%s" in key "%s" when merging "%s" into "%s"' % (e, key, b, a))
	return a


for p in (USER_CONFIG_PATH, PROJECT_CONFIG_PATH):
	if not os.path.exists(p):
		continue
	with open(p) as f:
		CONFIG = data_merge(CONFIG, yaml.load(f, Loader=yaml.FullLoader))
