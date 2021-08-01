import geopandas as gpd
import numpy as np
from erde import autocli

@autocli
def main(data, radius, dissolve=False, default_crs=None, *args, **kwargs):
	"""
	Creates buffer as in shapely.buffer
	https://shapely.readthedocs.io/en/stable/manual.html#object.buffer

	Parameters
	----------

	* `data` GeoSeries or GeoDataFrame, the geometries to make buffer of.
	* `radius` float, radius in metres.
	* `dissolve`: bool, default `False`
		unite overlapping geometries. Index will be lost. If `data` is a GeoDataFrame, will return a new GeoDataFrame with new index and no other columns.
	* `resolution`: int, default 10. Number of vertice in a 90° arc.

	Shapely-specific parameters (see Shapely doc for more info):

	* `cap_style`: int (1 - round, 2 - flat, 3 - square). Style of buffers at ends of lines.
	* `join_style`: int (1 - round, 2 - mitre, 2 - bevel), style of buffer around corners.
	* `mitre_limit`: float (1..5) how far a sharp join should protrude.
	"""
	if not isinstance(data, (gpd.GeoSeries, gpd.GeoDataFrame)):
		raise TypeError('geodata must be GeoSeries/GeoDataFrame')

	if data.crs is None:
		if default_crs is None:
			raise ValueError(f'data ({data.__class__}) has no crs. Set the CRS of the GeoSeries/GeoDataFrame manually, or call with `default_crs=4326` (or any appropriate) to set default behavior.')  # This exception is raised because setting CRS 4326 silently would cause unexpected behavior and exceptions in other locations. Crashing earlier is better.

		data.crs = default_crs

	old_crs = data.crs
	series = data if isinstance(data, gpd.GeoSeries) else data[data._geometry_column_name]

	series = series.to_crs(3857)
	buf = series.buffer(radius / series.centroid.to_crs(4326).y.pipe(np.radians).pipe(np.cos), *args, **kwargs).to_crs(old_crs)

	if isinstance(data, gpd.GeoSeries):
		return gpd.GeoSeries(buf.unary_union, crs=old_crs) if dissolve else buf

	data = data.copy()
	data[data._geometry_column_name] = buf
	return gpd.GeoDataFrame({data._geometry_column_name: buf.unary_union}, crs=old_crs) if dissolve else data
