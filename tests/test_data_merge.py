import pytest
from erde.cfg import data_merge, MergeError

def test_merge():
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

	a = {'key1': {'key2': 1}}
	b = {'key1': 321}
	with pytest.raises(MergeError):
		data_merge(a, b)

	a = (1, 2, 3)
	b = 1
	with pytest.raises(MergeError):
		data_merge(a, b)