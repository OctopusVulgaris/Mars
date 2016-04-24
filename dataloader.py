import pandas as pd
import sqlalchemy as sa





_DAY = '_day'
code = '600000'
start_date = '1990-01-01'
end_date = '2050-01-01'
fuquan = 'qfq'
table = code + _DAY + '_' + fuquan
engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/test', echo=True)
#read example
#dayK1 = pd.read_sql_query(text('SELECT open, high FROM "dayk_table" WHERE date =:date1;'), engine, params={'date1':'2016-01-04'})
dayK1 = pd.read_sql_table(table, engine, index_col = 'date')
dayK2 = dayK1.copy()

#dayK1.to_sql('test', engine, if_exists='append')
#itr = dayK1.itertuples()
#row = next(itr)
#while row:
#    print row;
#    row = next(itr)

#print dayK1[dayK1.date == '2016-04-21'].high

