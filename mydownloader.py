# -*- coding:utf-8 -*- 

import tushare as ts
import sqlalchemy as sa
import pandas as pd


from sqlalchemy import Date, text, DateTime, Integer
import psycopg2
import datetime
import logging
import threading

logging.basicConfig(filename='log.txt', level=logging.DEBUG)

def create_dayk_talbe():
    conn = psycopg2.connect("dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()
    cur.execute("create table if not exists dayk (date date, open numeric, high numeric, close numeric, low numeric, volume numeric, amount numeric, open_hfq numeric, high_hfq numeric, close_hfq numeric, low_hfq numeric,code text)")
    conn.commit()
    cur.close()
    conn.close()

def request_dayk(table, code, engine, start_date = '1990-01-01', end_date = '2050-01-01'):
    try:
        dayK_bfq = ts.get_h_data(code, start_date, end_date, None, retry_count=500)
        dayK_hfq = ts.get_h_data(code, start_date, end_date, 'hfq', retry_count=500)
        dayK_bfq['open_hfq'] = dayK_hfq['open']
        dayK_bfq['high_hfq'] = dayK_hfq['high']
        dayK_bfq['low_hfq'] = dayK_hfq['low']
        dayK_bfq['close_hfq'] = dayK_hfq['close']
        dayK_bfq['code'] = code
        dayK_bfq.to_sql(table, engine, if_exists='append', dtype={'date': Date})
        logging.info(str(code) + ', request_dayk success')
    except Exception:
        logging.error(str(code) + ' request_dayk failed on ' + str(threading.currentThread()))


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

def request_history_tick(code, engine, start_date, end_date):
    create_tick_talbe(code)

    cur_day = start_date
    logging.info('requesting tick, code: ' + code + str(threading.currentThread()))


    while cur_day != end_date:
        try:
            #logging.info('cur_day: ' + str(cur_day) + str(threading.currentThread()))
            tick = ts.get_tick_data(code, date=cur_day.date(), retry_count=500)
            if not tick.empty:
                if tick.time[0] != 'alert("当天没有数据");':
                    tick['type'] = tick['type'].apply(lambda x: trade_type_dic[x])
                    tick['change'] = tick['change'].apply(change_dic)
                    tick['time'] = str(cur_day.date()) + ' '+ tick['time']
                    tick.to_sql('tick_tbl_' + code, engine, if_exists='append', dtype={'time': DateTime})
                    logging.info('save to tick_tbl_' + code + ' on '+ str(cur_day) + ' thread ' + str(threading.currentThread()))


        except Exception:
            logging.error(str(code) + ' request tick failed on ' + str(cur_day) + str(threading.currentThread()))

        delta = datetime.timedelta(days=1)
        cur_day = cur_day + delta


def update_stock_basics(engine):
    oldlist = pd.read_sql_table('stock_list', engine)
    oldlist.to_csv('./conf/stock_list_old.csv', encoding='utf-8')
    newlist = ts.get_stock_basics()
    newlist.sort_index(inplace=True)
    newlist.to_sql('stock_list', engine, if_exists='replace')
    newlist = pd.read_sql_table('stock_list', engine)
    old_code_list = oldlist['code']
    new_code_list = newlist['code']
    posA = 0
    posB = 0
    lenA = len(old_code_list)
    lenB = len(new_code_list)
    newAdd = open('.\\conf\\stock_list_new_add.txt', 'w')
    removed = open('.\\conf\\stock_list_removed.txt', 'w')
    while posA < lenA and posB < lenB:
        if old_code_list[posA] == new_code_list[posB]:
            posA += 1
            posB += 1
        elif old_code_list[posA] > new_code_list[posB]:
            newAdd.write(str(new_code_list[posB]) + '\n')
            posB += 1
        else:
            removed.write(str(old_code_list[posA]) + '\n')
            posA += 1
    while posA < lenA:
        removed.write(str(old_code_list[posA]) + '\n')
        posA += 1
    while posB < lenB:
        newAdd.write(str(new_code_list[posB]) + '\n')
        posB += 1

    newAdd.close()
    removed.close()

    return newlist











