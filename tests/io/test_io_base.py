import pytest
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from unittest.mock import patch, Mock
from erde.io.base import BaseReader


d = 'tests/io/data/'

def test_base_reader():

	# (self, source, geometry_filter=None, chunk_size: int = 10_000, sync: bool = False, pbar: bool = True, queue_size=10, **kwargs)

	df = gpd.read_file(d + 'polygons.gpkg')
	s = 10
	dfs = [df[i:i + s] for i in range(0, len(df), s)]
	gen_obj = (i for i in df.geometry.values)


	# calling BaseReader, with path to file as geometry_filter.
	with patch('erde.io.read_stream', return_value=dfs) as mock:
		BaseReader('mock source', 'path_to_geofile.gpkg')

	mock.assert_called_with('path_to_geofile.gpkg', chunk_size=1, pbar=False, sync=True)

	# cover the case where geometry_filter is None
	with patch('erde.io.read_stream', return_value=dfs) as mock:
		br = BaseReader('another mock')

	mock.assert_not_called()

	# now, br is used as a filter for other readers
	br.total_rows = 10

	# trying other objects as geometry_filter:
	# single geometries; another BaseReader; generator; geoseries; geodataframe; list of geometries
	for obj in [Point(1, 2), LineString([[1, 2], [3, 4]]), Polygon([[0, 1], [1, 1], [1, 0], [0, 0], [0, 1]], []), br, gen_obj, df.geometry, df, df.geometry.to_list()]:
		BaseReader('mock source', geometry_filter=obj)

	# geometry_filter can't be anything other than those above
	with pytest.raises(TypeError):
		BaseReader('mock source', Mock)

def test_parallel():
	for s in (False, True):
		with BaseReader(d + 'polygons.gpkg', chunk_size=3, sync=s) as rd:
			pass
