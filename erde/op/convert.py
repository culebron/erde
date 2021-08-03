from erde import autocli, read_stream, write_stream
import yaargh


def toint(val, name):
	try:
		return int(val)
	except ValueError:
		raise yaargh.CommandError(f'{name} must be int')


@autocli
def main(path1, path2, to_crs=None, from_crs=None):
	"""Converts files/tables between formats and CRS."""
	with read_stream(path1, sync=False) as rd, write_stream(path2, sync=False) as wr:
		for df in rd:
			if from_crs is not None:
				df.crs = toint(from_crs, 'from_crs')

			if to_crs is not None:
				if df.crs is None:
					raise RuntimeError(f'Input GeoDataFrame has no CRS, but convesion is requested with to_crs={to_crs}')

				df.to_crs(toint(to_crs, 'to_crs'), inplace=True)

			wr(df)
