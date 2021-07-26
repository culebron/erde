from erde import read_stream
from erde.io import select_driver
import pytest

def test_broken_file_cause_exception():
	broken_file = '/tmp/broken-shp-file.shp'
	with open(broken_file, 'w') as f:
		f.write('nothing')

	rd, pm = select_driver(broken_file)
	with pytest.raises(RuntimeError):
		read_stream(broken_file)
