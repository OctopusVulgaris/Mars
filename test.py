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

def filter_group(x):
    x = abs(x)
    if x > 5000000:
        return 500
    elif x > 1000000:
        return 100
    elif x > 500000:
        return 50
    else:
        return 0


engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

code = '000700'
#downloader.request_history_tick(code, engine, '2016-04-01', '2016-05-01')
#mydownloader.request_dayk('dayk', code, engine, '2014-01-01', '2016-05-01')

t1 = datetime.datetime.now()
delta = datetime.timedelta(days=1)
cur_day = datetime.datetime.strptime('2016-01-01', '%Y-%m-%d')
next_day = cur_day + delta
last_day = datetime.datetime.strptime('2016-03-31', '%Y-%m-%d')


sql = "SELECT time, price, change, volume, amount, type FROM tick_tbl_000700 where time > DATE '" + cur_day.strftime('%Y-%m-%d') + "' and time < DATE '" + last_day.strftime('%Y-%m-%d') +"'"
one_year_tick = pd.read_sql(sql, engine, index_col='time', parse_dates={'time':'%Y-%m-%d'})

#sql = "SELECT date, close FROM dayk where date > DATE '" + cur_day.strftime('%Y-%m-%d') + "' and date < DATE '" + last_day.strftime('%Y-%m-%d') +"'"
#dayk = pd.read_sql(sql, engine, index_col='date', parse_dates={'date':'%Y-%m-%d'})


print datetime.datetime.now()-t1

one_year_tick['volume'] = one_year_tick['amount'] / one_year_tick['price']
one_year_tick['volume'] = one_year_tick['volume'].round()
one_year_tick['amount'] = one_year_tick['amount'] * one_year_tick['type']
one_year_tick['type'] = one_year_tick['amount'].apply(filter_group)
#one_year_tick['500'] = one_year_tick['amount'].apply(filter_group_1, args=(500000,))

#print one_year_tick
#one_year_tick.to_sql('tick_tbl_' + code, engine, if_exists='replace', dtype={'time': DateTime})

gg = one_year_tick.groupby([to_date, 'type']).sum()
kk = one_year_tick.groupby([to_date, 'type']).mean()

pd.set_option('display.multi_sparse', False)

zz = pd.DataFrame()

zz[0] = gg.loc(axis=0)[:,0]['amount'].reset_index(1)['amount']
zz[50] = gg.loc(axis=0)[:,50]['amount'].reset_index(1)['amount']
zz[100] = gg.loc(axis=0)[:,100]['amount'].reset_index(1)['amount']
zz[500] = gg.loc(axis=0)[:,500]['amount'].reset_index(1)['amount']
zz['price'] = one_year_tick.groupby(to_date).last()['price']


#print zz

ax = zz.plot(rot= 70, grid=True, kind='bar', subplots=True)
#bx = dayk.plot(rot = 70, grid=True)



#mm['VWAP'] = sm.amount / sm.volume / 100
#del mm['amount']
#del mm['index']
#del mm['volume']
#del mm['type']

#mm.plot()
plt.show()

print datetime.datetime.now()-t1



