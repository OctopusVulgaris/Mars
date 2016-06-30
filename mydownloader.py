# -*- coding:utf-8 -*- 

import requests
import time
import os
import tushare as ts
import pandas as pd
import dataloader
import json
import re
from lxml import etree
from StringIO import StringIO
from dataloader import engine
import pandas.io.sql as psql
import tushare as ts
import argparse
import utility

from sqlalchemy import Date, text, DateTime, Integer
import psycopg2
import datetime
import logging
import threading

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='log.txt'
                    )
conn = psycopg2.connect(database="postgres", user="postgres", password="Wcp181114", host="localhost", port="5432")
cur = conn.cursor()
proxies = {
    'http': 'http://10.23.31.130:8080',
    'https': 'http://10.23.31.130:8080',
}

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

def create_test_talbe(code):
    conn = psycopg2.connect("dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()
    sql = "create table if not exists test_" + code + " (index integer, time timestamp, price numeric, change text, volume numeric, amount numeric, type integer)"
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def request_test_tick(code, engine, start_date, end_date):
    create_test_talbe(code)

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
                    tick.to_sql('test_' + code, engine, if_exists='append', dtype={'time': DateTime})
                    logging.info('save to test_' + code + ' on '+ str(cur_day) + ' thread ' + str(threading.currentThread()))


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

def write_bonus_to_db(binfo):
    order = "INSERT INTO public.bonus_ri_sc(type, code, adate, give, transfer, paydiv, rdate, xdate)\
          VALUES ('bonus',\'%s\', \'%s\', %s, %s, %s, \'%s\', \'%s\')" % (
            binfo['code'], binfo['adate'], binfo['give'], binfo['trans'], binfo['divpay'], binfo['rdate'], binfo['xdate'])
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if (err.find('duplicate key value') > 0 or err.find('违反唯一约束') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
    conn.commit()

def write_ri_to_db(rinfo):
    order = "INSERT INTO public.bonus_ri_sc(type, code, adate, ri, riprice, basecap, rdate, xdate)\
          VALUES ('rightsissue',\'%s\', \'%s\', %s, %s, %s, \'%s\', \'%s\')" % (
            rinfo['code'], rinfo['adate'], rinfo['ri'], rinfo['riprice'], rinfo['basecap'], rinfo['rdate'], rinfo['xdate'])
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if (err.find('duplicate key value') > 0 or err.find('违反唯一约束') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
    conn.commit()

def write_stockchange_to_db(sinfo):
    order = "INSERT INTO public.bonus_ri_sc(type,code, adate, xdate, reason, totalshare, tradeshare, limitshare, prevts)\
          VALUES ('stockchange',\'%s\', \'%s\', \'%s\', \'%s\', %s, %s, %s, %s)" % (
        sinfo['code'], sinfo['adate'], sinfo['xdate'], sinfo['reason'], sinfo['totalshare'], sinfo['tradeshare'], sinfo['limitshare'], sinfo['prevts'])
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if (err.find('duplicate key value') > 0 or err.find('违反唯一约束') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
    conn.commit()

def get_bonus_and_ri(code, timeout=5):
    url = r'http://money.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/'+ code + r'.phtml'
    content = requests.get(url, timeout=timeout).content
    selector = etree.HTML(content)
    bitems = selector.xpath('//*[@id="sharebonus_1"]/tbody/tr')
    dfs = []
    for item in bitems:
        binfo = {}

        binfo['adate'] = ''.join(item.xpath('td[1]/text()'))
        if(binfo['adate'].find(u'没有数据') > 0):
            break
        if(binfo['adate'] == '--'):
            binfo['adate'] = '1900-1-1'
        binfo['code'] = code
        binfo['give'] = ''.join(item.xpath('td[2]/text()'))
        binfo['trans'] = ''.join(item.xpath('td[3]/text()'))
        binfo['divpay'] = ''.join(item.xpath('td[4]/text()'))
        binfo['xdate'] = ''.join(item.xpath('td[6]/text()'))
        if(binfo['xdate'] == '--'):
            binfo['xdate'] = '1900-1-1'
        binfo['rdate'] = ''.join(item.xpath('td[7]/text()'))
        if (binfo['rdate'] == '--'):
            binfo['rdate'] = '1900-1-1'
        write_bonus_to_db(binfo)
    #     df = pd.DataFrame()
    #     df = df.from_dict(binfo, orient='index')
    #     dfs.append(df.T)
    # df = pd.DataFrame()
    # df = pd.concat(dfs)
    # df.adate = pd.to_datetime(df.adate)
    # df.xdate = pd.to_datetime(df.xdate)
    # df.rdate = pd.to_datetime(df.rdate)
    # df.give = pd.to_numeric(df.give)
    # df.trans = pd.to_numeric(df.trans)
    # df.divpay = pd.to_numeric(df.divpay)
    # print df.dtypes
    # print 'b'
    # print df
    # df.to_hdf('d:\\HDF5_Data\\binfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)


    dfs1 = []
    ritems = selector.xpath('//*[@id="sharebonus_2"]/tbody/tr')
    for item in ritems:
        rinfo = {}

        rinfo['adate'] = ''.join(item.xpath('td[1]/text()'))
        if (rinfo['adate'].find(u'没有数据') > 0):
            break
        if(rinfo['adate'] == '--'):
            rinfo['adate'] = '1900-1-1'
        rinfo['ri'] = ''.join(item.xpath('td[2]/text()'))
        rinfo['riprice'] = ''.join(item.xpath('td[3]/text()'))
        rinfo['basecap'] = ''.join(item.xpath('td[4]/text()'))
        rinfo['xdate'] = ''.join(item.xpath('td[5]/text()'))
        rinfo['rdate'] = ''.join(item.xpath('td[6]/text()'))
        if(rinfo['xdate'] == '--'):
            rinfo['xdate'] = '1900-1-1'
        if (rinfo['rdate'] == '--'):
            rinfo['rdate'] = '1900-1-1'
        rinfo['code'] = code
        write_ri_to_db(rinfo)
    #     df = pd.DataFrame()
    #     df = df.from_dict(rinfo, orient='index')
    #     dfs1.append(df.T)
    # df = pd.concat(dfs1)
    # df.adate = pd.to_datetime(df.adate)
    # df.xdate = pd.to_datetime(df.xdate)
    # df.rdate = pd.to_datetime(df.rdate)
    # df.ri = pd.to_numeric(df.ri)
    # df.riprice = pd.to_numeric(df.riprice)
    # df.basecap = pd.to_numeric(df.basecap)
    # print df.dtypes
    # print 'r'
    # print df
    #
    # df.to_hdf('d:\\HDF5_Data\\rinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)


def is_digit_or_point(c):
    if(str.isdigit(c)):
        return True
    elif(c == '.'):
        return True
    else:
        return False

def get_stock_change(code, timeout=60):
    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/' + code + r'.phtml'
    content = requests.get(url, timeout=timeout).content
    selector = etree.HTML(content)
    tables = selector.xpath('//*[@id="con02-1"]/table')
    prev_tradeable_share = 0.0
    dfs = []
    for table in tables[::-1]:
        cols = table.xpath('tbody/tr[1]/td')
        for col in range(len(cols), 1, -1):
            sinfo = {}
            sinfo['code'] = code
            sinfo['xdate'] = ''.join(table.xpath('tbody/tr[1]/td[%d]/text()'%col))
            if (sinfo['xdate'] == ''):
                sinfo['xdate'] = '1900-1-1'
            sinfo['adate'] = ''.join(table.xpath('tbody/tr[2]/td[%d]/text()'%col))
            if (sinfo['adate'] == ''):
                sinfo['adate'] = '1900-1-1'
            sinfo['reason'] = ''.join(table.xpath('tbody/tr[4]/td[%d]/text()'%col))
            sinfo['totalshare'] = ''.join(table.xpath('tbody/tr[5]/td[%d]/text()'%col))
            sinfo['totalshare'] = filter(is_digit_or_point, sinfo['totalshare'].encode('utf-8'))
            sinfo['tradeshare'] = ''.join(table.xpath('tbody/tr[7]/td[%d]/text()'%col))
            sinfo['tradeshare'] = filter(is_digit_or_point, sinfo['tradeshare'].encode('utf-8'))
            sinfo['limitshare'] = ''.join(table.xpath('tbody/tr[9]/td[%d]/text()'%col))
            sinfo['limitshare'] = filter(is_digit_or_point, sinfo['limitshare'].encode('utf-8'))
            sinfo['prevts'] = prev_tradeable_share
            prev_tradeable_share = sinfo['tradeshare']
            write_stockchange_to_db(sinfo)
    #         df = pd.DataFrame()
    #         df = df.from_dict(sinfo, orient='index')
    #         dfs.append(df.T)
    # df = pd.DataFrame()
    # df = pd.concat(dfs)
    # df.adate = pd.to_datetime(df.adate)
    # df.xdate = pd.to_datetime(df.xdate)
    # df.totalshare = pd.to_numeric(df.totalshare)
    # df.tradeshare = pd.to_numeric(df.tradeshare)
    # df.limitshare = pd.to_numeric(df.limitshare)
    # df.prevts = pd.to_numeric(df.prevts)
    # df.reason = df.reason.str.encode('utf-8')
    # print df.dtypes
    # print 's'
    # print df
    # df.to_hdf('d:\\HDF5_Data\\sinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)

def convertNone(c):
    if(c == 'None' or c == 'null' or c== 'NULL'):
        return 0.00
    else:
        return c

def calc_hfqratio(data, bsr, initHfqRatio=1):
    lastestHfqRatio = initHfqRatio
    index = 0
    for row in bsr.itertuples():
        exdate_valid_flag = 1
        try:
            if data.ix[row[10]]['close'] - 0.0 < 0.0000001:
                exdate_valid_flag = 0
        except KeyError, e:
            exdate_valid_flag = 0

        delta = datetime.timedelta(days=1)
        enddate = row[10]
        vdate = row[10]
        prev_valid_date = row[10]
        if not exdate_valid_flag:
            while True:
                rr = enddate
                rr += delta
                rdata = data[:rr].tail(1)
                if rdata.empty:
                    break
                else:
                    cls = rdata.ix[0]['close']
                    enddate = rdata.index[0]
                    if cls != 0.0:
                        try:
                            itr = rdata.itertuples()
                            drow = next(itr)
                            #print drow
                            while drow:
                                if drow[3] == 0.0:
                                    break
                                prev_valid_date = drow[0]
                                drow = next(itr)
                            break
                        except StopIteration, e:
                            break
                    else:
                        continue
            try:
                bsr.loc[index,'xdate'] = datetime.datetime.strptime(str(prev_valid_date), "%Y-%m-%d %H:%M:%S").date()
            except ValueError, e:
                bsr.loc[index, 'xdate'] = datetime.datetime.strptime(str(prev_valid_date), "%Y-%m-%d").date()
        #get prev valid close
        cls = 0.0
        enddate = row[10]
        while True:
            rr = enddate
            rr -= delta
            rdata = data[ rr:].head(1)
            if rdata.empty:
                cls = 0.0
                break
            else:
                cls = rdata.ix[-1]['close']
                enddate = rdata.index[-1]
                if cls != 0.0:
                    itr = rdata.itertuples()
                    drow = next(itr)
                    while drow:
                        if drow[3] != 0.0:
                            cls = drow[3]
                            break
                        drow = next(itr)
                    break
                else:
                    continue
        bsr.loc[index,'preclose'] = cls
        index += 1

    bsr.sort(['xdate','totalshare'], ascending=[1, 0], inplace=True)
    bsr.reset_index(inplace=True)
    #print bsr
    index = 0
    prev_hfqratio = lastestHfqRatio
    prev_type = ''
    prev_xdate = datetime.date(1900, 1, 1)
    for row in bsr.itertuples():
        if(row[11] == prev_xdate and prev_type == 'stockchange'):
            bsr.loc[index, 'hfqratio'] = round(prev_hfqratio, 6)
            index += 1
            continue

        if row[16] != 'stockchange':
            if bsr.loc[index,'preclose'] != 0.0:
                adjcls = (bsr.loc[index,'preclose'] - bsr.loc[index,'paydiv'] / 10 + bsr.loc[index,'riprice'] * bsr.loc[index,'ri'] / 10) / (1 + bsr.loc[index,'give'] / 10 + bsr.loc[index,'transfer'] / 10 + bsr.loc[index,'ri'] / 10)
                #adjcls = (cls - row[5] / 10 + row[7] * row[6] / 10) / (1 + row[3] / 10 + row[4] / 10 + row[6] / 10)
                adjcls = round(adjcls * 100) / 100
                hfqratio = bsr.loc[index,'preclose'] / adjcls
            else:
                hfqratio = lastestHfqRatio
        else:
            if bsr.loc[index,'tradeshare'] != 0 and bsr.loc[index,'prevts'] != 0:
                hfqratio = bsr.loc[index,'tradeshare'] / bsr.loc[index,'prevts']
            else:
                hfqratio = lastestHfqRatio
        hfqratio = round(hfqratio, 6)
        bsr.loc[index, 'hfqratio'] = round(prev_hfqratio * hfqratio, 6)
        prev_hfqratio = bsr.loc[index, 'hfqratio']
        prev_xdate = bsr.loc[index,'xdate']
        prev_type = bsr.loc[index,'type']

        index += 1
    #data = pd.read_csv(StringIO(r.content), encoding='gbk', index_col=u'日期',parse_dates=True)
    data['hfqratio'] = pd.Series(lastestHfqRatio, index=data.index)
    logging.info(bsr)
    print bsr

    prevdate = datetime.date(1900,1,1)
    prevratio= lastestHfqRatio
    for row in bsr.itertuples():
        currentdate=row[11]
        data.loc[currentdate:prevdate,'hfqratio'] = prevratio
        prevdate=currentdate
        prevratio=row[19]
    if not bsr.empty:
        today = datetime.datetime.now().date()
        data.loc[today:prevdate, 'hfqratio'] = prevratio
        #print data.to_csv('test.csv', encoding='utf-8')

def get_stock_full_daily_data(code, timeout=60):
    if code[0] == '6':
        url = r'http://quotes.money.163.com/service/chddata.html?code=0' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
    else:
        url = r'http://quotes.money.163.com/service/chddata.html?code=1' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content), encoding='gbk', index_col=u'日期', parse_dates=True)
    data.index.names = ['date']
    if not data.empty:
        data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','turnoverrate','vol','amo','totalcap','tradeablecap']
        data['code'] = pd.Series(code, index=data.index)
        data['netchng'] = data['netchng'].apply(convertNone)
        data['pctchng'] = data['pctchng'].apply(convertNone)
        data['turnoverrate'] = data['turnoverrate'].apply(convertNone)
        #data.to_sql('dailydata', engine, if_exists='append', index=True)
        #name=code+'.csv'
        #data.to_csv(name, encoding='utf-8')

    sql = "select * from bonus_ri_sc where code=\'" + code + "\' and xdate!='1900-1-1' and (type='bonus' or type='rightsissue' or (type='stockchange' and (reason='股权分置' or reason='拆细'))) order by xdate asc, totalshare desc"
    bsr = pd.read_sql(sql, engine)

    bsr['preclose'] = pd.Series(0.0, index=bsr.index)
    bsr['hfqratio'] = pd.Series(0.0, index=bsr.index)

    calc_hfqratio(data, bsr)

    if not data.empty:
        #data.drop(u'前收盘',1, inplace=True)
        #data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','turnoverrate','vol','amo','totalcap','tradeablecap','hfqratio']
        #data['code'] = pd.Series(code, index=data.index)
        #data['netchng'] = data['netchng'].apply(convertNone)
        #data['pctchng'] = data['pctchng'].apply(convertNone)
        data.to_sql('dailydata', engine, if_exists='append', index=True)
        # data['codeutf'] = 'utf8'
        # data['nameutf'] = 'utf8'
        # data.nameutf = data.name.str.encode('utf-8')
        # data.codeutf = data.code.str.encode('utf-8')
        # del data['code']
        # del data['name']
        #
        # data.columns = ['close','high','low','open','prevclose','netchng','pctchng','turnoverrate','vol','amo',
        #                 'totalcap','tradeablecap', 'code','name']
        # data.to_hdf('d:\\HDF5_Data\\dailydata.hdf', 'day', mode='a', format='t', complib='blosc', append=True)

def get_all_full_daily_data(retry=50, pause=10):
    target_list = dataloader.get_code_list('', '', engine)
    #llen = len(target_list)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_bonus_and_ri(row.code.encode("utf-8"))
                    get_stock_change(row.code.encode("utf-8"))
                    get_stock_full_daily_data(row.code.encode("utf-8"))
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get daily data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

def update_weekly_data():
    full_df = pd.DataFrame()
    chunk_size = 100000
    offset = 0
    dfs = []
    while True:
        sql = "SELECT * FROM dailydata where code = '000001' order by code, date limit %d offset %d" % (chunk_size, offset)
        dfs.append(psql.read_sql(sql, engine, index_col=['code', 'date'], parse_dates=True))
        offset += chunk_size
        if len(dfs[-1]) < chunk_size:
            break
    full_df = pd.concat(dfs)
    del dfs
    logging.info("Loading daily data fininshed")

    period_type = 'W'
    ma_list = [5, 10, 20, 30, 60, 120]
    for i in range(len(full_df.index.levels[0])):
        #logging.info("iloc starts:" + str(datetime.datetime.now()))
        tdf = full_df.iloc[full_df.index.get_level_values('code') == full_df.index.levels[0][i]]
        tdf.reset_index('code', inplace=True)
        logging.info("ma starts:" + str(datetime.datetime.now()))
        wtdf = tdf.resample(period_type, how='last')
        wtdf['pctchng'] = tdf.resample(period_type, how=lambda x:(x+1.0).prod() - 1.0)
        wtdf = wtdf.dropna(axis=0, how='all')  # 删除全空的行和列
        wtdf.to_sql('weeklydata', engine, if_exists='append', index=True)
    #logging.info("MA Done")
        print wtdf


def get_index_full_daily_data(code, timeout=60):
    if code[0] == '0':
        url = r'http://quotes.money.163.com/service/chddata.html?code=0' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'
    else:
        url = r'http://quotes.money.163.com/service/chddata.html?code=1' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content), encoding='gbk', index_col=u'日期', parse_dates=True)
    data.index.names = ['date']
    if not data.empty:
        data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','vol','amo']
        data['code'] = pd.Series(code, index=data.index)
        data['prevclose'] = data['prevclose'].apply(convertNone)
        data['netchng'] = data['netchng'].apply(convertNone)
        data['pctchng'] = data['pctchng'].apply(convertNone)
        data['vol'] = data['vol'].apply(convertNone)
        data['amo'] = data['amo'].apply(convertNone)

        data.to_sql('indexdaily', engine, if_exists='append', index=True)

def get_all_full_index_daily(retry=50, pause=10):
    target_list = dataloader.get_index_list('', '', engine)
    itr = target_list.itertuples()
    row = next(itr)
    while row:
        for _ in range(retry):
            try:
                get_index_full_daily_data(row.code.encode("utf-8"))
            except Exception as e:
                err = 'Error %s' % e
                logging.info('Error %s' % e)
                time.sleep(pause)
            else:
                logging.info('get index data for %s successfully' % row.code.encode("utf-8"))
                break
        row = next(itr)

def close_check(row):
    if row.open - 0.0 < 0.000001:
        row.close = 0.0
    row.totalcap *= 10000
    row.tradeablecap *= 10000
    return row

def get_today_all_from_sina_realtime(retry=50, pause=10):
    df = utility.get_realtime_all()
    df1 = df[['Date','Code','Open','Pre_close','high','low','Price','Name']]
    df1.columns = ['date','code','open','prevclose','high','low','close','name']
    df1.to_sql('dailydata', engine, if_exists='append', index=False)

def get_today_all_from_sina(retry=50, pause=10):
    #please take note::
    #change tushare to add pricechange
    #added by andy
    df = utility.get_today_all()
    #print df

    #df.columns = ['code', 'name', 'netchng','pctchng', 'close', 'open', 'high', 'low', 'prevclose', 'vol','turnoverrate', 'amo', 'per','pb','totalcap', 'tradeablecap']
    df.columns = ['amo', 'buy', 'pctchng', 'code','high', 'low', 'totalcap','name', 'tradeablecap','open','pb', 'per', 'netchng', 'sell', 'prevclose','symbol','ticktime','close', 'turnoverrate','vol']
    df.drop('per', 1, inplace=True)
    df.drop('pb', 1, inplace=True)
    df.drop('buy', 1, inplace=True)
    df.drop('sell', 1, inplace=True)
    df.drop('ticktime', 1, inplace=True)
    df.drop('symbol', 1, inplace=True)

    df.to_csv('t1.csv', encoding='utf-8')
    df = df.apply(close_check, axis=1)
    df.to_csv('t2.csv', encoding='utf-8')
    #print df
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    df['date'] = pd.Series(today, index=df.index)
    #print df
    df.to_sql('dailydata', engine, if_exists='append', index=False)

    target_list = dataloader.get_code_list('', '', engine)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    #get_bonus_and_ri(row.code.encode("utf-8"))
                    #get_stock_change(row.code.encode("utf-8"))
                    #get_stock_full_daily_data(row.code.encode("utf-8"))
                    update_today_data(row.code.encode("utf-8"))
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

def get_bonus_ri_sc(retry=50, pause=1):
    target_list = dataloader.get_code_list('', '', engine)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_bonus_and_ri(row.code.encode("utf-8"))
                    get_stock_change(row.code.encode("utf-8"))
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s bonus data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

def get_today_all_from_163(retry=50, pause=10):
    target_list = dataloader.get_code_list('', '', engine)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    #get_bonus_and_ri(row.code.encode("utf-8"))
                    #get_stock_change(row.code.encode("utf-8"))
                    get_delta_daily_data(row.code.encode("utf-8"))
                    # update_today_data(row.code.encode("utf-8"))
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

def get_delta_daily_data(code, timeout=60):
    sql = "select * from dailydata where code=\'" + code + "\' and open != 0.0 and hfqratio > 1.0 order by date desc limit 1"
    ddata = pd.read_sql(sql, engine, index_col=['date'], parse_dates=True)
    if ddata.empty:
        sql = "select * from dailydata where code=\'" + code + "\' and open != 0.0 and hfqratio = 1.0 order by date desc limit 1"
        ddata = pd.read_sql(sql, engine, index_col=['date'], parse_dates=True)
        if ddata.empty:
            return

    date = ddata.index[0]
    lastestHfqRatio = ddata.ix[0]['hfqratio']

    sdate = date.strftime('%Y%m%d')
    edate = datetime.datetime.now().strftime('%Y%m%d')
    if code[0] == '6':
        url = r'http://quotes.money.163.com/service/chddata.html?code=0' + code + r'&start=' + sdate + r'&end=' + edate + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
    else:
        url = r'http://quotes.money.163.com/service/chddata.html?code=1' + code + r'&start=' + sdate + r'&end=' + edate + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content), encoding='gbk', index_col=u'日期', parse_dates=True)
    data.index.names = ['date']
    if not data.empty:
        data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','turnoverrate','vol','amo','totalcap','tradeablecap']
        data['code'] = pd.Series(code, index=data.index)
        data['netchng'] = data['netchng'].apply(convertNone)
        data['pctchng'] = data['pctchng'].apply(convertNone)
        data['turnoverrate'] = data['turnoverrate'].apply(convertNone)
        data['hfqratio'] = pd.Series(lastestHfqRatio, index=data.index)

    sql = "select * from bonus_ri_sc where code=\'" + code + "\' and xdate> \'"+ str(sdate) + "\' and xdate <= \'" + edate + "\' and (type='bonus' or type='rightsissue' or (type='stockchange' and (reason='股权分置' or reason='拆细'))) order by xdate asc, totalshare desc"
    bsr = pd.read_sql(sql, engine)

    if not bsr.empty:
        date1 = data.index[0].date()
        date2 = bsr.xdate.iloc[-1]
        if date2 > date1:
            if not data.empty:
                print data
                data.to_sql('dailydata', engine, if_exists='append', index=True)
                return

    bsr['preclose'] = pd.Series(0.0, index=bsr.index)
    bsr['hfqratio'] = pd.Series(lastestHfqRatio, index=bsr.index)
    calc_hfqratio(data, bsr, lastestHfqRatio)

    if not data.empty:
        # data.drop(u'前收盘',1, inplace=True)
        # data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','turnoverrate','vol','amo','totalcap','tradeablecap','hfqratio']
        # data['code'] = pd.Series(code, index=data.index)
        # data['netchng'] = data['netchng'].apply(convertNone)
        # data['pctchng'] = data['pctchng'].apply(convertNone)
        data.to_sql('dailydata', engine, if_exists='append', index=True)
        #pass

def update_today_data(code, timeout=60):
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    sql = "select * from dailydata where date=\'" + today + "\' and code=\'" + code + "\' and hfqratio = 1.0"
    data1 = pd.read_sql(sql, engine, index_col=['date'], parse_dates=True)
    #if no data in db for today, return
    if data1.empty:
        return

    #if data1.index[0] == data2.index[0]:
    #    return

    #if data1.open.iloc[0] - 0.0 < 0.0000001:
    #    sql = "select * from dailydata where code=\'" + code + "\' and ( date=\'" + today + "\' or( date<\'" + today + "\' and  hfqratio != 0)) order by date desc limit 1"

    sql = "select * from dailydata where code=\'" + code + "\' and ( date=\'" + today + "\' or( date<\'" + today + "\' and  open != 0)) order by date desc limit 2"
    #sql = "select * from dailydata where code=\'" + code + "\' and (date<\'" + today + "\' and  open != 0) order by date desc limit 1"
    data = pd.read_sql(sql, engine, index_col=['date'], parse_dates=True)

    #data = data1.append(data2)

    #if all hfqration in db is 1, no change, return
    lastestHfqRatio = data.hfqratio.iloc[-1]
    if  lastestHfqRatio - 1.0 < 0.0000001:
        return

    #if data1.open.iloc[0] - 0.0 < 0.0000001:
    #    data1.hfqratio = lastestHfqRatio
    #    data1.to_sql('dailydata', engine, if_exists='append', index=True)
    #    return

    data.hfqratio=lastestHfqRatio
    print data
    date = data.index[1]
    sdate = date.strftime('%Y%m%d')
    edate = datetime.datetime.now().strftime('%Y%m%d')

    sql = "select * from bonus_ri_sc where code=\'" + code + "\' and xdate> \'"+ str(sdate) + "\' and xdate <= \'" + edate + "\' and (type='bonus' or type='rightsissue' or (type='stockchange' and (reason='股权分置' or reason='拆细'))) order by xdate asc, totalshare desc"
    bsr = pd.read_sql(sql, engine)

    if not bsr.empty:
        date1 = data.index[0]
        date2 = bsr.xdate.iloc[-1]
        if date2 > date1:
            if not data.empty:
                print data
                data.to_sql('dailydata', engine, if_exists='append', index=True)
                return
    else:
        if not data.empty:
            print data
            data.to_sql('dailydata', engine, if_exists='append', index=True)
            return

    bsr['preclose'] = pd.Series(0.0, index=bsr.index)
    bsr['hfqratio'] = pd.Series(lastestHfqRatio, index=bsr.index)
    calc_hfqratio(data, bsr, lastestHfqRatio)

    if not data.empty:
        data.to_sql('dailydata', engine, if_exists='append', index=True)
        #pass

def postdelta():
    sql = "update dailydata set totalcap=close * (select totalshare from bonus_ri_sc where code=dailydata.code and totalshare > 0 order by xdate desc limit 1) where date='20160630' and totalcap =0 and open !=0"
    pd.read_sql(sql, engine)

def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['full', 'delta','bonus','bnd'], default='full', help='download type')

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']

    if (type == 'full'):
        get_all_full_daily_data()
    elif(type == 'delta'):
        get_today_all_from_sina_realtime()
        get_today_all_from_163()
        get_today_all_from_sina()
        postdelta()
    elif (type == 'bonus'):
        get_bonus_ri_sc()
    elif (type == 'bnd'):
        get_bonus_ri_sc()
        get_today_all_from_sina_realtime()
        get_today_all_from_163()
        get_today_all_from_sina()
        postdelta()

    #update_weekly_data()
    # get_bonus_and_ri('000001')
    # get_stock_change('000001')
    #get_stock_full_daily_data('603519')
    #get_index_full_daily_data('000001')
    #get_bonus_ri_sc()
    #get_today_all_from_163()
    #get_today_all_from_sina()
    #get_delta_daily_data('002352')
    #update_today_data('603519')




