# this script uses routing as an imported function and writes derived data only to the result file.

from erde import autocli, route, length, read_stream, write_stream

@autocli
def main(input_data: read_stream) -> write_stream:
	if input_data.crs is None:
		input_data.crs = 4326
	routed_df = length(route(input_data, 'https://routing.openstreetmap.de/routed-foot'))
	input_data = length(input_data)
	input_data['routed_length'] = routed_df['length']
	input_data['extra_travel_ratio'] = input_data['routed_length'] / input_data['length']
	return input_data
