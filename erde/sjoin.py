import geopandas as gpd


def sgroup(left_df, right_df, agg, left_on='geometry', right_on='geometry', suffixes=('', '_right'), join='left', op='intersects'):
    if len(agg) == 0:
        raise ValueError('agg argument can\'t be empty')

    left_tmp = df_on(left_df, left_on)
    right_tmp = df_on(right_df, right_on)

    m = gpd.sjoin(left_tmp, right_tmp, op=op, how=join)
    for k in agg.keys():  # we put the data columns here, because they may contain `geometry` (_right), which gets lost after sjoin.
        m[k] = m.index_right.map(right_df[k])

    m2 = m.groupby(m.index).agg(agg)
    return left_df.join(m2, lsuffix=suffixes[0], rsuffix=suffixes[1], how=join)


def slookup(left_df, right_df, columns, left_on='geometry', right_on='geometry', suffixes=('', '_right'), join='left', op='intersects'):
    return sgroup(left_df, right_df, {k: 'first' for k in columns}, left_on, right_on, suffixes, join, op)


def sfilter(left_df, right_df, left_on='geometry', right_on='geometry', negative=False, op='intersects'):

    left_tmp = df_on(left_df, left_on)
    right_tmp = df_on(right_df, right_on)
       
    m = gpd.sjoin(left_tmp, right_tmp, op=op)
    isin = left_df.index.isin(m.index)
    if negative: isin = ~isin
    return left_df[isin]


def df_on(df, on):
    if isinstance(on, gpd.GeoSeries):
        return gpd.GeoDataFrame({'geometry': on}, index=df.index)
    
    if isinstance(on, str):
        return df[[on]]
    
    raise TypeError('*_on argument must be either string, or GeoSeries')
