import pytest
from unittest import mock
from erde import read_df, read_stream, write_stream
from erde.io import csv

d = 'tests/io/data/'

test_df = read_df(d + 'numeric.csv')

def test_numeric_file():
	# just coverage
	df = read_df(d + 'numeric.csv')
	assert len(df) == 4
	assert set(df) == {'a', 'b', 'c'}
	for df in read_stream(d + 'numeric.csv', chunk_size=2):
		assert len(df) == 2
		assert set(df) == {'a', 'b', 'c'}

def test_write_cancel():
	# this mock trick didn't work
	# from erde.io.csv import CsvWriter
	# class CsvWriterMock(CsvWriter):
	# 	def __init__(self, *args, **kwargs):
	# 		super().__init__(*args, **kwargs)
	# 		self._cancel = mock.MagicMock(wraps=self._cancel)

	#with mock.patch('erde.csv.CsvWriter', CsvWriterMock):
	with pytest.raises(RuntimeError):
		with write_stream('/tmp/test-write-cancel.csv') as wr:
			wr(test_df)
			raise RuntimeError('test')

	#wr._cancel.assert_called_once()

def test_write_to_buffer():
	from io import StringIO

	buf = StringIO()
	with csv.CsvWriter(buf, sync=True) as wr:  # must be in same process
		wr(test_df)
		wr(test_df)

		assert len(buf.getvalue()) > 0


