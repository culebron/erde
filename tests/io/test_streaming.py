import pytest

d = 'tests/io/data/'

def test_stream_reading():
	from erde.io import read_stream, write_stream, select_driver, drivers

	for fmt in ['csv', 'geojson', 'gpkg', 'geojsonl.json', 'shp']:
		with write_stream(f'/tmp/points-stream.{fmt}', sync=True, chunk_size=2) as wr:
			for df in read_stream(f'{d}points.{fmt}'):
				print(df)	
				wr(df)

			path = f'not_existing_path.{fmt}'
			assert select_driver(path)[0] == drivers[fmt]
			with pytest.raises(FileNotFoundError):
				read_stream(path)
