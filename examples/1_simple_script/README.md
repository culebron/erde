# Example #1: Using read_df/write_df and Other Utilities

The script calculates demand for each school (in apartments, assuming demography is the same).

First, we determine the "demand": each house apartments are divided by the number of schools in 1 km radius. Then, for each school we sum the demand of houses within 1 km.

## Functions

Both function guess the format.

* `read_df` detects whether CSV has geometry column, if no, creates a simple `pd.DataFrame`
* `write_df` saves to required format. Should it be CSV, it converts geometry to WKT.

\b
	from erde import read_df, write_df
	df = read_df('my_path.csv')
	# do something
	write_df(df, 'new_path.gpkg')
