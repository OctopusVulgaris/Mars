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

def request_instrument(table, code, start_date = '1990-01-01', end_date = '2050-01-01', fuquan = 'qfq'):
    dayK = ts.get_h_data(code, start_date, end_date, fuquan)
    dayK['code'] = code
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/test', echo=True)
    dayK.to_sql(table, engine, if_exists='append', dtype={'date': Date})










