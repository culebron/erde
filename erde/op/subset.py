from erde import autocli, read_stream, write_stream

def parse_str(columns):
	result = []
	for i in columns.split(','):
		j = [k.strip() for k in i.strip().split(':')]

		for k in j:
			if len(k) == 0 or (len(k) == 1 and k.startswith('-')):
				raise ValueError(f'Bad column name: "{i}": zero name length.')

		if len(j) > 2:
			raise ValueError(f"column name must have 0 or 1 colons (:) got {len(i) - 1} in '")

		if len(j) == 2 and j[0].startswith('-'):
			raise ValueError(f"name {i} is removed, but is also renamed: '{i}'")

		if len(j) == 1:
			j += [None]
		result.append(j)
	return result


@autocli
def main(input_data: read_stream, columns) -> write_stream:
	"""Renames, removes and checks presence of columns in a (geo)dataframe.

	Parameters
	----------
	input_data: DataFrame, GeoDataFrame
	columns: str or iterable
		Comma-separated list of names, or dict, or list of name pairs. See formats below.

	Returns
	-------
	DataFrame, GeoDataFrame
		New dataframe that contains new columns.

	Columns List Format
	-------------------
	From a command-line, pass comma-separated names of columns. To rename a column, write names pair like a dict. (Whitespaces are not significant, they'll be removed automatically.)

		old_name: new_name

	This will rename old_name to new_name, and drop the rest of columns. To keep the rest of them, provide an asterisk:

		old_name:new_name,*

	To assert a column is in dataframe, without renaming, just write its name:

		old_name:new_name,*,this_must_exist

	To drop a column, add minus sign in the beginning:

		-remove_col1,-remove_col2

	If you write only the columns to drop, the rest will be kept (asterisk is assumed). If those columns are missing, nothing will happen. If you add renaming, the rest will be dropped too:

		-remove_col1,-remove_col2,old_name:new_name

	Calling this Function from Code
	-------------------------------
	If you import the function, columns don't need to be a string: you can pass a dict or a list of name pairs.

	>>> import pandas as pd
	>>> df = pd.DataFrame({'col1': range(10, 20), 'col2': range(100,110), 'col3': range(1000,1010)})
	>>> list(main(df, {'col1': 'col4'}))
	['col4']
	>>> list(main(df, 'col1: col5,*'))
	['col5', 'col2', 'col3']
	>>> list(main(df, (('col1', 'col4'), ('col2', 'new2'))))
	['col4', 'new2']
	>>> list(main(df, '-col1'))
	['col2', 'col3']
	>>> list(main(df, 'col2:new2, -col1'))
	['new2']
	"""
	if not isinstance(columns, (str, list, tuple, dict)):
		raise TypeError(f'columns must be string, or an iterable of names pairs. Got {columns.__class__} instead')

	if isinstance(columns, str):
		columns = parse_str(columns)
	elif isinstance(columns, dict):
		columns = tuple(columns.items())

	kept = [k for k, v in columns if not k.startswith('-') and k != '*']
	renamed = {k: v for k, v in columns if v is not None}
	removed = [k[1:] for k, v in columns if k.startswith('-')]
	# keep others if '*' in the expression, or if we only remove some columns
	others = (['*', None] in columns) or (len(kept) == 0 and len(removed) > 0)

	for i in kept:
		if i not in input_data:
			raise KeyError(f'column {i} not in df (columns present: {", ".join(list(input_data))})')

	df2 = input_data if others else input_data[kept]
	# copy the objects only here
	df3 = df2.copy().rename(columns=renamed)
	df4 = df3.drop(removed, axis=1, errors='ignore')
	return df4.copy()
