# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
import logging
from dataloader import get_code_list

def s(x):
    return x.head(300)

def get5min():
    df = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day')
    gg = df.groupby(level=0)
    df = gg.apply(s)
    df = df[df.buyflag== True]
    df = df.reset_index(level=1)
    open = df.open
    open = open.reset_index()
    open = open.set_index('date')
    open = open.loc[:'2016-5-15']
    open = open.reset_index()
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

    tick5min = pd.HDFStore('D:\\HDF5_Data\\tick5min.h5', complib='blosc', mode='w')
    size = len(open)
    cnt = 0
    itr = open.itertuples()
    try:
        row = next(itr)
        while row:
            t1 = datetime.datetime.now()
            cnt += 1
            date = row[1].date()
            code = row[2]
            open = row[3]
            try:
                sql = 'select price, time, volume, amount from tick_tbl_' + code + ' where time > \'' + str(date) + ' 00:00:00\' and time < \'' + str(date) + ' 09:35:00\''
                r = pd.read_sql(sql, engine, index_col='price')
                r['code'] = code
                tick5min.append('tick', r)
                row = next(itr)
            except Exception:
                row = next(itr)
            print 'finish ' + str(cnt) + ' of ' + str(size) + ' in ' + str(datetime.datetime.now() - t1)
    except StopIteration:
        pass
