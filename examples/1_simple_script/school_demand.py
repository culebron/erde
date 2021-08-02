from erde import read_df, write_df
from erde.op import sjoin, buffer

houses_df = read_df('houses.csv')
schools_df = read_df('schools.csv')

schools_df['school'] = schools_df.index.tolist()
school_buf = buffer.main(schools_df.geometry, 1000, default_crs=4326)
demand = sjoin.sgroup(houses_df, schools_df, {'school': 'count'}, right_on=school_buf)
demand['apts_demand'] = demand.apartments / demand.school

write_df(demand, '/tmp/demand.csv')

result = sjoin.sgroup(schools_df, demand, {'apts_demand': 'sum'}, left_on=school_buf)

write_df(result, '/tmp/school_demand.csv')
