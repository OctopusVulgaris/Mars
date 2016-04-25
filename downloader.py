# -*- coding:utf-8 -*- 

import tushare as ts
import sqlalchemy as sa
from sqlalchemy import Date, text
import psycopg2

def create_dayk_talbe():
    conn = psycopg2.connect("dbname=test user=postgres password=postgres")
    cur = conn.cursor()
    cur.execute("create table if not exists dayk_qfq (date date, open numeric, high numeric, close numeric, low numeric, volume numeric, amount numeric, code integer)")
    conn.commit()
    cur.close()
    conn.close()

def request_dayk(table, code, start_date = '1990-01-01', end_date = '2050-01-01', fuquan = 'qfq'):
    dayK = ts.get_h_data(code, start_date, end_date, fuquan)
    dayK['code'] = code
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/test', echo=True)
    dayK.to_sql(table, engine, if_exists='append', dtype={'date': Date})

def create_tick_talbe(code):
    conn = psycopg2.connect("dbname=test user=postgres password=postgres")
    cur = conn.cursor()
    sql = "create table if not exists " + code + "_tick_tbl (time timestamp primary key, price numeric, change numeric, volume numeric, amount numeric, type string, amount numeric, code integer)"
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def request_history_tick(table, code, start_date='1990-01-01', end_date='2050-01-01'):
    create_tick_talbe(code)
    tick = ts.get_h_data(code, start_date, end_date)

    a = ts.get_tick_data('600848', date='2016-04-24')
    a['type'] = a['type'].apply(lambda x: trade_type_dic[x])
    a['time'] = '2014-01-09 ' + a['time']

    print a[0:1].time
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/test', echo=True)
    dayK.to_sql(table, engine, if_exists='append', dtype={'date': Date})










