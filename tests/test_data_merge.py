from unittest import mock
import pytest

def test_merge():
	from erde.cfg import data_merge, MergeError
	# no nested data
	a = {'key1': 'val1', 'key2': 123, 'key3': 4.56}
	b = {'key1': 'val2', 'key4': ['x', 'y', 'z']}

	assert data_merge(a, b) == {**a, **b}

	# nested
	a = {'key1': {'key2': 456, 'key3': {'key4': 987, 'key5': 'xxxxx'}}}
	b = {'key1': {'key3': {'key4': 987, 'key5': 'overwritten'}}}

	d = data_merge(a, b)
	assert d == {'key1': {'key2': 456, 'key3': {'key4': 987, 'key5': 'overwritten'}}}

	# extend or append to list
	a = {'key1': [1, 2, 3]}
	b = {'key1': 4}
	assert data_merge(a, b) == {'key1': [1, 2, 3, 4]}

	a = {'key1': [1, 2, 3]}
	b = {'key1': [4, 5, 6]}
	assert data_merge(a, b) == {'key1': [1, 2, 3, 4, 5, 6]}

	class MockDict(dict):
		def __setitem__(self, k, v):
			raise TypeError("this dict should raise TypeError on setitem")

	a = MockDict()

	with pytest.raises(MergeError):
		data_merge(a, b)

	a = {'key1': {'key2': 1}}
	b = {'key1': 321}
	with pytest.raises(MergeError):
		data_merge(a, b)

	a = (1, 2, 3)
	b = 1
	with pytest.raises(MergeError):
		data_merge(a, b)


def test_load_files():
	import random
	local_router = f'https://localhost:{random.randint(1000, 32767)}'
	with mock.patch('yaml.load', return_value={'routers': {'local': local_router}}):
		from importlib import reload
		from erde import cfg
		reload(cfg)  # erde & cfg are imported when the test module is loaded, so they must be reloaded

	assert cfg.CONFIG['routers']['local'] == local_router
