# -*- coding:utf-8 -*- 

import tushare as ts
import datetime as dt
from sqlalchemy import create_engine, Date, text
import pandas

dd = Date()
dd= '2016-01-04'
print dd

dayK = ts.get_h_data('600705', '2016-01-01', '2100-01-01', 'qfq')
#dayK.to_csv('d:\\a.csv')


#tickH = ts.get_tick_data('600008', '2004-01-05')
#tickH.to_csv('d:\\b.csv')

#dayK = pandas.read_csv('d:\\a.csv')

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/test', echo=True)
dayK.to_sql('dayk_table', engine, index=True, index_label='date', if_exists='replace', dtype={'date': Date})
dayK1 = pandas.read_sql_query(text('SELECT open, high FROM dayk_table where date =: date1;'), engine, params={'date1': dd})
print dayK1






