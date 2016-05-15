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

def change(x):
    if float(x) > 0:
        return 1
    elif float(x) < 0:
        return -1
    else:
        return 0

def filter_group(x):
    x = abs(x)
    if x > 3000000:
        return 300
    elif x > 1000000:
        return 100
    elif x > 200000:
        return 20
    else:
        return 0


engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

code = '000998'
t1 = datetime.datetime.now()
delta = datetime.timedelta(days=1460)
cur_day = datetime.datetime.strptime('2005-01-01', '%Y-%m-%d')
next_day = cur_day + delta
last_day = datetime.datetime.strptime('2009-01-01', '%Y-%m-%d')

one_year_tick = pd.DataFrame()
while last_day < datetime.datetime(2016,12,31):

    sql = "SELECT time, price, change, volume, amount, type FROM tick_tbl_"+code+" where time > DATE '" + cur_day.strftime('%Y-%m-%d') + "' and time < DATE '" + last_day.strftime('%Y-%m-%d') +"'"
    one_year_tick = one_year_tick.append(pd.read_sql(sql, engine, index_col='time', parse_dates={'time':'%Y-%m-%d'}))
    cur_day = cur_day + delta
    last_day = last_day + delta
    print datetime.datetime.now()-t1
#sql = "SELECT date, close FROM dayk where date > DATE '" + cur_day.strftime('%Y-%m-%d') + "' and date < DATE '" + last_day.strftime('%Y-%m-%d') +"'"
#dayk = pd.read_sql(sql, engine, index_col='date', parse_dates={'date':'%Y-%m-%d'})


print datetime.datetime.now()-t1

#one_year_tick['volume'] = one_year_tick['amount'] / one_year_tick['price']
#one_year_tick['volume'] = one_year_tick['volume'].round()
one_year_tick['change'] = one_year_tick['change'].apply(change)
one_year_tick['amount2'] = one_year_tick['amount'] * one_year_tick['change']
one_year_tick['amount'] = one_year_tick['amount'] * one_year_tick['type']
one_year_tick['type'] = one_year_tick['amount'].apply(filter_group)

def money_flow():


    #print one_year_tick

    #one_year_tick['500'] = one_year_tick['amount'].apply(filter_group_1, args=(500000,))

    #print one_year_tick
    #one_year_tick.to_sql('tick_tbl_' + code, engine, if_exists='replace', dtype={'time': DateTime})

    gg = one_year_tick.groupby([to_date, 'type']).sum()

    pd.set_option('display.multi_sparse', False)

    zz = pd.DataFrame()
    yy = pd.DataFrame()

    zz[0] = gg.loc(axis=0)[:,0]['amount'].reset_index(1)['amount']
    zz[20] = gg.loc(axis=0)[:,20]['amount'].reset_index(1)['amount']
    zz[100] = gg.loc(axis=0)[:,100]['amount'].reset_index(1)['amount']
    zz[300] = gg.loc(axis=0)[:,300]['amount'].reset_index(1)['amount']
    zz['amount'] = one_year_tick.groupby(to_date).sum()['amount']

    yy[0] = gg.loc(axis=0)[:,0]['amount2'].reset_index(1)['amount2']
    yy[20] = gg.loc(axis=0)[:,20]['amount2'].reset_index(1)['amount2']
    yy[100] = gg.loc(axis=0)[:,100]['amount2'].reset_index(1)['amount2']
    yy[300] = gg.loc(axis=0)[:,300]['amount2'].reset_index(1)['amount2']
    yy['amount'] = one_year_tick.groupby(to_date).sum()['amount2']

    zz.fillna(0, inplace=True)
    yy.fillna(0, inplace=True)


    mm = zz.cumsum()
    nn = yy.cumsum()
    print datetime.datetime.now()-t1

    #zz.plot(rot= 70, grid=True, kind='bar', title = code)

    #bx = dayk.plot(rot = 70, grid=True)
    mm.plot(rot= 70, grid=True, kind='line', title = code+'buysell')
    nn.plot(rot= 70, grid=True, kind='line', title = code+'change')


    plt.show()

    print datetime.datetime.now()-t1

if __name__=="__main__":

    money_flow()


