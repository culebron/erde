from erde import autocli

@autocli
def main(output_path):
	from erde.op import table

	responses = [None]
	def capture_osrm_output(*args, **kwargs):
		r = list(table.table_route(*args, **kwargs))
		responses[0] = r
		return r

	from unittest import mock

	with mock.patch('erde.op.isochrone.table_route', side_effect=capture_osrm_output):
		from erde.op import isochrone as ic
		from erde import read_df
		sources = read_df('tests/isochrone/sources.csv')

		ir = ic.IsochroneRouter(sources['geometry'].values[0], 'http://localhost:5000', (5, 10, 15), 5)
		ir.routed

	import pickle
	with open(output_path, 'wb') as f:
		pickle.dump(responses[0], f)
