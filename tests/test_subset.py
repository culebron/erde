import pandas as pd
from erde.op import subset
from pytest import raises

def test_parse_strings_errors():
	good_string = 'old1:new1,old2,old3:new3'
	assert subset.parse_str(good_string) == [['old1', 'new1'], ['old2', None], ['old3', 'new3']]

	bad_string = 'old1:new1,old2:new2:verynew2'
	with raises(ValueError):
		subset.parse_str(bad_string)

	with raises(ValueError):
		subset.parse_str('-old1:new1')

	with raises(ValueError):
		subset.parse_str('old1,old2:new2,-,old4')

	with raises(ValueError):
		subset.parse_str('')


def test_main_errors():
	df = pd.DataFrame({'col1': range(5), 'col2': range(10, 15), 'col3': range(20, 25)})
	for v in [123456, None, True, False, df]:
		with raises(TypeError):
			subset.main(df, v)

	with raises(KeyError):
		subset.main(df, 'missing_column')