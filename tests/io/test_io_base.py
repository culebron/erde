import pytest
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from unittest.mock import patch, Mock


def d(s): return 'tests/io/data/' + s

def test_base_reader():
	from erde.io.base import BaseReader

	# (self, source, geometry_filter=None, chunk_size: int = 10_000, sync: bool = False, pbar: bool = True, queue_size=10, **kwargs)

	df = gpd.read_file(d('polygons.gpkg'))
	s = 10
	dfs = [df[i:i + s] for i in range(0, len(df), s)]
	gen_obj = (i for i in df.geometry.values)


	with patch('erde.io.read_stream', return_value=dfs):
		BaseReader('mock source', 'path_to_geofile.gpkg')

	br = BaseReader('another mock')
	br.total_rows = 10

	for obj in [Point(1, 2), LineString([[1, 2], [3, 4]]), Polygon([[0, 1], [1, 1], [1, 0], [0, 0], [0, 1]], []), br, gen_obj, df.geometry, df, df.geometry.to_list()]:
		BaseReader('mock source', geometry_filter=obj)

	with pytest.raises(TypeError):
		BaseReader('mock source', Mock)