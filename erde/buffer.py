import geopandas as gpd
import numpy as np

def buffer(data, radius, resolution=10):
	if not isinstance(data, (gpd.GeoSeries, gpd.GeoDataFrame)):
		raise TypeError('geodata must be GeoSeries/GeoDataFrame')

	if data.crs is None:
		data.crs = 4326

	old_crs = data.crs
	series = data if isinstance(data, gpd.GeoSeries) else data[data._geometry_column_name]

	series = series.to_crs(3857)
	buf = series.buffer(radius / series.centroid.to_crs(4326).y.pipe(np.radians).pipe(np.cos), resolution=resolution).to_crs(old_crs)

	if isinstance(data, gpd.GeoSeries):
		return buf

	data[data._geometry_column_name] = buf
	return data
