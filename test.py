# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
import mydownloader
import matplotlib.pyplot as plt
from sqlalchemy import Date, text, DateTime, Integer

outstanding = 4543607000

def to_date(x):
    return  x.date()
engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

code = '000869'
downloader.request_history_tick(code, engine, '2016-04-01', '2016-05-01')
downloader.request_dayk('dayk', code, engine, '2014-01-01', '2016-05-01')

t1 = datetime.datetime.now()
delta = datetime.timedelta(days=1)
cur_day = datetime.datetime.strptime('2016-04-29', '%Y-%m-%d')
next_day = cur_day + delta
last_day = datetime.datetime.strptime('2016-04-30', '%Y-%m-%d')


sql = "SELECT time, price, change, volume, amount, type FROM tick_tbl_000869 where time > DATE '" + cur_day.strftime('%Y-%m-%d') + "' and time < DATE '" + last_day.strftime('%Y-%m-%d') +"'"
one_year_tick = pd.read_sql(sql, engine, index_col='time')

print datetime.datetime.now()-t1

one_year_tick['volume'] = one_year_tick['amount'] / one_year_tick['price']
one_year_tick['volume'] = one_year_tick['volume'].round()
one_year_tick['amount'] = one_year_tick['price'] * one_year_tick['volume']
#print one_year_tick
#one_year_tick.to_sql('tick_tbl_' + code, engine, if_exists='replace', dtype={'time': DateTime})

gg = one_year_tick.groupby(to_date)

#mm = gg.mean()
sm = gg.sum()
print sm

#mm['VWAP'] = sm.amount / sm.volume / 100
#del mm['amount']
#del mm['index']
#del mm['volume']
#del mm['type']

#mm.plot()
#plt.show()

print datetime.datetime.now()-t1


