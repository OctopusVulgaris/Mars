# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
import downloader
import matplotlib.pyplot as plt
import matplotlib

import numpy as np

def to_date(x):
    return  x.date()
engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

code = '000869'
#downloader.request_history_tick(code, engine, '2014-01-01', '2016-05-01')
#downloader.request_dayk('dayk', code, engine, '2014-01-01', '2016-05-01')

t1 = datetime.datetime.now()
delta = datetime.timedelta(days=1)
cur_day = datetime.datetime.strptime('2015-01-01', '%Y-%m-%d')
next_day = cur_day + delta
last_day = datetime.datetime.strptime('2016-01-01', '%Y-%m-%d')


sql = "SELECT * FROM tick_tbl_000869 where time > DATE '" + cur_day.strftime('%Y-%m-%d') + "' and time < DATE '" + last_day.strftime('%Y-%m-%d') +"'"
one_year_tick = pd.read_sql(sql, engine, index_col='time')
t2 = datetime.datetime.now()
print t2-t1
gg = one_year_tick.groupby(to_date)
print gg
mm = gg.mean()
sm = gg.sum()

mm['VWAP'] = sm.amount / sm.volume / 100
del mm['amount']
del mm['index']
del mm['volume']
del mm['type']

mm.plot()
plt.show()
#print mm





t1 = datetime.datetime.now()-t1
print t1

