# -*- coding:utf-8 -*- 

import tushare as ts
import sqlalchemy as sa

from sqlalchemy import Date, text, DateTime, Integer
import psycopg2
import datetime
import logging
import threading

logging.basicConfig(filename='log.txt', level=logging.DEBUG)


def create_dayk_talbe():
    conn = psycopg2.connect("dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()
    cur.execute("create table if not exists dayk_qfq (date date, open numeric, high numeric, close numeric, low numeric, volume numeric, amount numeric, code integer)")
    conn.commit()
    cur.close()
    conn.close()

def request_dayk(table, code, start_date = '1990-01-01', end_date = '2050-01-01', fuquan = 'qfq'):
    dayK = ts.get_h_data(code, start_date, end_date, fuquan)
    dayK['code'] = code
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=True)
    dayK.to_sql(table, engine, if_exists='append', dtype={'date': Date})

def create_tick_talbe(code):
    conn = psycopg2.connect("dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()
    sql = "create table if not exists tick_tbl_" + code + " (index integer, time timestamp, price numeric, change text, volume numeric, amount numeric, type integer)"
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


trade_type_dic = {
    '买盘' : 1,
    '卖盘' : -1,
    '中性盘' : 0
}

def change_dic(x):
    if x == '--':
        return 0
    else:
        return x

def request_history_tick(code, start_date='1995-01-01', end_date='2050-01-01'):
    create_tick_talbe(code)

    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
    cur_day = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    last_day = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    logging.info('requesting code: ' + code + str(threading.currentThread()))


    while cur_day != last_day:
        try:
            logging.info('cur_day: ' + str(cur_day) + str(threading.currentThread()))
            tick = ts.get_tick_data(code, date=cur_day.date(), retry_count=500)
            if not tick.empty:
                if tick.time[0] != 'alert("当天没有数据");':
                    tick['type'] = tick['type'].apply(lambda x: trade_type_dic[x])
                    tick['change'] = tick['change'].apply(change_dic)
                    tick['time'] = str(cur_day.date()) + ' '+ tick['time']
                    tick.to_sql('tick_tbl_' + code, engine, if_exists='append', dtype={'time': DateTime})
                    logging.info('save to tick_tbl_' + code + ' on '+ str(cur_day) + ' thread ' + str(threading.currentThread()))


        except Exception:
            logging.error(str(code) + ' prcessing failed on ' + str(cur_day) + str(threading.currentThread()))

        delta = datetime.timedelta(days=1)
        cur_day = cur_day + delta


def get_stock_basics():
    list = ts.get_stock_basics()
    engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=True)
    list.sort_index(inplace=True)
    list.to_sql('stock_list', engine, if_exists='replace')
    return  list











