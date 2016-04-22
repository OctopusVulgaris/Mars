# -*- coding:utf-8 -*- 

import tushare as ts
from sqlalchemy import create_engine, Date, text
import pandas


_DAY = '_day'
code = '600000'
start_date = '1990-01-01'
end_date = '2050-01-01'
fuquan = 'qfq'
table = code + _DAY + '_' + fuquan
dayK = ts.get_h_data(code, start_date, end_date, fuquan)
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/test', echo=True)
dayK.to_sql(table, engine, if_exists='replace', dtype={'date': Date})


#read example
#dayK1 = pandas.read_sql_query(text('SELECT open, high FROM "dayk_table" WHERE date =:date1;'), engine, params={'date1':'2016-01-04'})
#dayK1 = pandas.read_sql_table(table, engine)







