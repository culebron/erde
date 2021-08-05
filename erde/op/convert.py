from erde import read_stream, write_stream, autocli


@autocli
def main(df: read_stream, to_crs:int=None, from_crs:int=None) -> write_stream:
	"""Converts files/tables between formats and CRS."""
	if from_crs is not None:
		df.crs = from_crs

	if to_crs is not None:
		if df.crs is None:
			raise RuntimeError(f'Input GeoDataFrame has no CRS, but convesion is requested with to_crs={to_crs}')

		df.to_crs(to_crs, inplace=True)

	return df
