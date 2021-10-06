from erde import read_df
from unittest import mock
import example
import json
import pytest
from datetime import datetime

# fixtures
areas = read_df('test_areas.geojson')
example.TMP_DIR = '.'  # cropped images for the test_areas are in current folder
start_day = datetime(2020, 7, 15)
end_day = datetime(2020, 7, 15)

with open('./data/resp.json') as f:
	resp_json = json.load(f)

def test_get_tiles():
	m = mock.MagicMock()
	m.return_value.json.return_value = resp_json
	m.return_value.content.decode.return_value = json.dumps(resp_json)

	# should process the response and return correct dataframe
	with mock.patch('requests.get', side_effect=m):
		data = example.get_tiles(areas, start_day, end_day)
		assert len(data) > 0
		for k in ('B01', 'B8A', 'B12'):
			assert k in data

		assert all(data['collection'] == 'sentinel-2-l1c')

	# if response is empty, should raise RuntimeError
	m.return_value.json.side_effect = ValueError
	m.return_value.content.decode.return_value = ''
	with mock.patch('requests.get', side_effect=m), pytest.raises(RuntimeError):
		example.get_tiles(areas, start_day, end_day)

def test_read_keys():
	assert example.read_keys('data/key1.csv') == ('123', '456')
	assert example.read_keys('data/key2.csv') == ('abc', 'def')
	with pytest.raises(RuntimeError):
		example.read_keys('data/key3.csv')
