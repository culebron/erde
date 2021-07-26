import pytest

d = 'tests/io/data/'

def test_stream_reading():
	from erde import read_stream, write_stream, io

	for fmt in ['csv', 'geojson', 'gpkg', 'geojsonl.json', 'shp']:
		with write_stream(f'/tmp/points-stream.{fmt}', sync=True, chunk_size=2) as wr:
			for df in read_stream(f'{d}points.{fmt}'):
				print(df)	
				wr(df)

			path = f'not_existing_path.{fmt}'
			assert io.select_driver(path)[0] == io.drivers[fmt]
			with pytest.raises(FileNotFoundError):
				read_stream(path)
