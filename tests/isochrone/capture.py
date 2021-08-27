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

	df = responses[0][0]
	import pickle
	with open(output_path, 'wb') as f:
		# dict with records was saved to pickle, because otherwise python3.6 crashes when trying to recreate a dumped DataFrame
		pickle.dump(df.to_dict(orient='records'), f)
