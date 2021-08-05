from erde import autocli, utils, read_stream, write_stream

def _nullify(geoseries, nullify_irrelevant):
	import numpy as np
	if not nullify_irrelevant:
		return geoseries.area
	return np.where(geoseries.geom_type.str.endswith('Polygon'), geoseries.area, [np.nan] * len(geoseries))


@autocli
def main(input_data: read_stream, column_name='area', skip_transform:bool=False, nullify_irrelevant:bool=False, default_crs=None) -> write_stream:
	"""Calculates area of geometries in metres (or in CRS units if skip_transform==True), sanitizes the input: checks and transforms CRS, may set area of irrelevant geometries to null.

	Parameters
	----------
	input_data : GeoDataFrame
		GeoDataFrame to take and write to.
	column_name : string, default 'area'
		How to call the new column with area values. Existing column will be overridden.
	skip_transform : bool, default False
		If False, geometries are converted to Pseudo-Mercator (epsg:3857) and area is in metres. If True, areas are calculated in current units.
	nullify_irrelevant : bool, default False
		If True, for geometries other than (Multi)Polygon, area value will be nan.
	default_crs : int or dict or pyproj object, optional
		If input_data will have no CRS, set it to default_crs.

	Returns
	-------
	GeoDataFrame with new column added.

	The function/script does not assume any CRS of input_data, because otherwise it will either crash from infinite coordinates or return irrelevant areas, and the reason will be hard to find. You have to provide `default_crs` argument if you're assured it will work correctly.
	"""

	if default_crs is None and input_data.crs is None and not skip_transform:
		raise ValueError('Input data has no CRS to transform from. Set input_data CRS, or provide default_crs, or set skip_transform')
	elif default_crs is not None:
		input_data = input_data.copy()
		input_data.crs = default_crs

	if skip_transform:
		input_data[column_name] = _nullify(input_data.geometry, nullify_irrelevant)
		return input_data

	input_data[column_name] = _nullify(input_data.geometry.to_crs(3857), nullify_irrelevant) * utils.coslat(input_data.geometry) ** 2
	return input_data
