import pytest
from erde import read_stream

def test_read_broken_geojson():
	path = '/tmp/broken.geojson'
	with open(path, 'w') as f:
		f.write('!!!!!!!!!!!!!!!!!!!1')

	with pytest.raises(RuntimeError):
		read_stream(path)
