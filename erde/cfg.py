import os
import yaml

PROJECT_CONFIG_PATH = os.path.join(os.curdir, 'erde.yml')
USER_CONFIG_PATH = os.path.join(os.path.expanduser("~"), '.erde.yml')

# to override this config, put .erde.yml into your home folder
CONFIG = {
	'routers': {
		'car': 'https://routing.openstreetmap.de/routed-car'
	}
}


# code by Schlomo https://stackoverflow.com/a/15836901/171278
class YamlReaderError(Exception):
	pass

def data_merge(a, b):
	"""merges b into a and return merged result

	NOTE: tuples and arbitrary objects are not handled as it is totally ambiguous what should happen"""
	key = None
	try:
		if a is None or isinstance(a, str) or isinstance(a, int) or isinstance(a, float):
			a = b
		elif isinstance(a, list):
			if isinstance(b, list):
				a.extend(b)
			else:
				a.append(b)
		elif isinstance(a, dict):
			if isinstance(b, dict):
				for key in b:
					if key in a:
						a[key] = data_merge(a[key], b[key])
					else:
						a[key] = b[key]
			else:
				raise YamlReaderError('Cannot merge non-dict "%s" into dict "%s"' % (b, a))
		else:
			raise YamlReaderError('NOT IMPLEMENTED "%s" into "%s"' % (b, a))
	except TypeError as e:
		raise YamlReaderError('TypeError "%s" in key "%s" when merging "%s" into "%s"' % (e, key, b, a))
	return a


for p in (USER_CONFIG_PATH, PROJECT_CONFIG_PATH):
	if not os.path.exists(p):
		continue
	with open(p) as f:
		CONFIG = data_merge(CONFIG, yaml.load(f, Loader=yaml.FullLoader))
