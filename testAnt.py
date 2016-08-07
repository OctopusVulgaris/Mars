# -*- coding:utf-8 -*-

import pandas as pd
import tushare as ts
import numpy as np
import json
import re
import requests
from lxml import etree
from StringIO import StringIO
from utility import round_series, getcodelist, getindexlist

import argparse
import utility
import datetime
import time
import logging
import sys

ashare_pattern = r'^0|^3|^6'

def convertNone(c):
    if(c == 'None' or c == 'null' or c== 'NULL'):
        return float(0.00)
    else:
        return float(c)

def get_one_index_full(code, idxStore, timeout=60):
    if code[0] == '0':
        url = r'http://quotes.money.163.com/service/chddata.html?code=0' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'
    else:
        url = r'http://quotes.money.163.com/service/chddata.html?code=1' + code + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content), encoding='gbk', index_col=u'日期', parse_dates=True)
    data.index.names = ['date']
    if not data.empty:
        data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','vol','amo']
        data['code'] = code
        data.name = data.name.str.encode('utf-8')
        data['close'] = data['close'].apply(convertNone)
        data['high'] = data['high'].apply(convertNone)
        data['low'] = data['low'].apply(convertNone)
        data['open'] = data['open'].apply(convertNone)
        data['prevclose'] = data['prevclose'].apply(convertNone)
        data['netchng'] = data['netchng'].apply(convertNone)
        data['pctchng'] = data['pctchng'].apply(convertNone)
        data['vol'] = data['vol'].apply(convertNone)
        data['amo'] = data['amo'].apply(convertNone)

        idxStore.append('idx', data, min_itemsize={'name': 30})

def get_all_full_index_daily(retry=50, pause=10):
    idxStore = pd.HDFStore('D:/HDF5_Data/indexdaily.h5', complib='blosc', mode='w')
    target_list = getindexlist()
    target_list.sort_values('code', inplace=True)
    for code in target_list.code.values:
        for _ in range(retry):
            try:
                get_one_index_full(code, idxStore)
                pass
            except Exception as e:
                logging.info('Error %s' % e)
                time.sleep(pause)
            else:
                logging.info('get history index data for %s successfully' % code)
                break
    idxStore.close()

def updateindexlist():
    sh_index_url = 'http://quotes.money.163.com/hs/service/hsindexrank.php?host=/hs/service/hsindexrank.php&query=IS_INDEX:true;EXCHANGE:CNSESH&fields=SYMBOL,NAME,PRICE,UPDOWN,PERCENT,zhenfu,VOLUME,TURNOVER,YESTCLOSE,OPEN,HIGH,LOW&sort=SYMBOL&order=asc&count=1000'
    sz_index_url = 'http://quotes.money.163.com/hs/service/hsindexrank.php?host=/hs/service/hsindexrank.php&query=IS_INDEX:true;EXCHANGE:CNSESZ&fields=SYMBOL,NAME,PRICE,UPDOWN,PERCENT,zhenfu,VOLUME,TURNOVER,YESTCLOSE,OPEN,HIGH,LOW&sort=SYMBOL&order=asc&count=1000'

    contentdf = pd.read_json(sh_index_url)
    sh = pd.read_json(contentdf.list.to_json()).T
    contentdf = pd.read_json(sz_index_url)
    sz = pd.read_json(contentdf.list.to_json()).T
    all = pd.concat([sh, sz])

    if not all.empty:
        all.CODE = all.CODE.str.encode('utf-8')
        all.NAME = all.NAME.str.encode('utf-8')
        all.CODE = all.CODE.str.slice(1)
        all = all[['CODE', 'NAME']].reset_index(drop=True)
        all.columns = ['code', 'name']
        all.to_hdf('d:/hdf5_data/indexlist.hdf', 'day')
        logging.info('finished to get index list...' + str(datetime.datetime.now()))
    else:
        logging.info('failed to get list...' + str(datetime.datetime.now()))


def updatestocklist(retry_count, pause):
    """
    get shanghai and shengkai stock list from their official website.
    Note: A rule has been set in db which will trigger update whenever any duplicate insert

    :param retry_count:
    :param pause: in sec
    :return:no
    """
    sz_onboard_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab2PAGENUM=1&ENCODE=1&TABKEY=tab2'
    sz_quit_onhold_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1793_ssgs&ENCODE=1&TABKEY=tab1'
    sz_quit_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1793_ssgs&ENCODE=1&TABKEY=tab2'

    sh_onboard_url = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1'
    sh_quit_onhold_url = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=4'
    sh_quit_url = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=5'

    header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36',
        'Upgrade-Insecure-Request': '1'
    }
    logging.info('start retrieving stock list...' + str(datetime.datetime.now()))
    for _ in range(retry_count):
        dfs = []
        print time.clock()
        try:
            # get info of shen zheng
            r = requests.get(sz_onboard_url)  # , proxies=proxies)
            sz_on = pd.read_html(r.content)
            if sz_on:
                df2 = sz_on[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                df2.code = df2.code.str.encode('utf-8')
                df2.name = df2.name.str.encode('utf-8')
                df2['status'] = 1
                dfs.append(df2)

            r = requests.get(sz_quit_onhold_url)  # , proxies=proxies)
            sz_quit_onhold = pd.read_html(r.content)
            if sz_quit_onhold:
                df2 = sz_quit_onhold[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                df2.code = df2.code.str.encode('utf-8')
                df2.name = df2.name.str.encode('utf-8')
                df2['status'] = 0
                dfs.append(df2)

            r = requests.get(sz_quit_url)  # , proxies=proxies)
            sz_quit = pd.read_html(r.content)
            if sz_quit:
                df2 = sz_quit[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                df2.code = df2.code.str.encode('utf-8')
                df2.name = df2.name.str.encode('utf-8')
                df2['status'] = -1
                dfs.append(df2)

            # get info of shang hai
            r = requests.get(sh_onboard_url, headers=header)  # , proxies=proxies,)
            # with open("sh_onboard.xls", "wb") as code:
            #    code.write(r.content)
            sh_on = pd.read_table(StringIO(r.content), encoding='gbk')
            if not sh_on.empty:
                df1 = sh_on.iloc[0:, [2, 3]]
                df1.columns = ['code', 'name']
                df1.code = df1.code.astype(str)
                df1.name = df1.name.str.encode('utf-8')
                df1['status'] = 1
                dfs.append(df1)

            r = requests.get(sh_quit_onhold_url, headers=header)  # , proxies=proxies,)
            # with open("sh_quit_onhold.xls", "wb") as code:
            #    code.write(r.content)
            sh_onhold = pd.read_table(StringIO(r.content), encoding='gbk')
            if not sh_onhold.empty:
                df1 = sh_onhold.iloc[0:, [0, 1]]
                df1.columns = ['code', 'name']
                df1.code = df1.code.astype(str)
                df1.name = df1.name.str.encode('utf-8')
                df1['status'] = 0
                dfs.append(df1)

            r = requests.get(sh_quit_url, headers=header)  # , proxies=proxies,)
            # with open("sh_quit.xls", "wb") as code:
            #    code.write(r.content)
            sh_quit = pd.read_table(StringIO(r.content), encoding='gbk')
            if not sh_quit.empty:
                df1 = sh_quit.iloc[0:, [0, 1]]
                df1.columns = ['code', 'name']
                df1.code = df1.code.astype(str)
                df1.name = df1.name.str.encode('utf-8')
                df1['status'] = -1
                dfs.append(df1)
        except Exception as e:
            err = 'Error %s' % e
            logging.info(err)
            time.sleep(pause)
        else:
            print time.clock()
            df = pd.concat(dfs)
            df = df.drop_duplicates(subset='code', keep='last')
            df = df.set_index('code')
            df = df[df.index.get_level_values(0).str.contains(ashare_pattern)]
            df = df.sort_index()
            df.to_hdf('d:/HDF5_Data/stocklist.hdf', 'list', mode='w', format='t', complib='blosc')
            logging.info('finished retrieving ' + str(len(df)) + ' successfully...' + str(datetime.datetime.now()))
            return
    logging.info('get_stock_list failed...' + str(datetime.datetime.now()))

def get_bonus_and_ri(code, brsStore, timeout=5):
    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/'+ code + r'.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    bitems = selector.xpath('//*[@id="sharebonus_1"]/tbody/tr')
    ritems = selector.xpath('//*[@id="sharebonus_2"]/tbody/tr')
    retry = 0
    while (len(bitems) < 1 or len(ritems) < 1) and retry < 10:
        content = requests.get(url, timeout=timeout).content
        selector = etree.HTML(content)
        bitems = selector.xpath('//*[@id="sharebonus_1"]/tbody/tr')
        ritems = selector.xpath('//*[@id="sharebonus_2"]/tbody/tr')
        retry += 1
        logging.info('Info retrying %d" bonus' % retry)
    dfs = []
    dfs1 = []
    for item in bitems:
        binfo = {}

        binfo['adate'] = ''.join(item.xpath('td[1]/text()'))
        if(binfo['adate'].find(u'没有数据') > 0):
            #logging.info('Info %s "没有数据" bonus' % code)
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
        #write_bonus_to_db(binfo)
        df = pd.DataFrame()
        df = df.from_dict(binfo, orient='index')
        dfs.append(df.T)

    for item in ritems:
        rinfo = {}

        rinfo['adate'] = ''.join(item.xpath('td[1]/text()'))
        if (rinfo['adate'].find(u'没有数据') > 0):
            #logging.info('Info %s "没有数据" rightsissue' % code)
            break
        if (rinfo['adate'] == '--'):
            rinfo['adate'] = '1900-1-1'
        rinfo['ri'] = ''.join(item.xpath('td[2]/text()'))
        rinfo['riprice'] = ''.join(item.xpath('td[3]/text()'))
        rinfo['basecap'] = ''.join(item.xpath('td[4]/text()'))
        rinfo['xdate'] = ''.join(item.xpath('td[5]/text()'))
        rinfo['rdate'] = ''.join(item.xpath('td[6]/text()'))
        if (rinfo['xdate'] == '--'):
            rinfo['xdate'] = '1900-1-1'
        if (rinfo['rdate'] == '--'):
            rinfo['rdate'] = '1900-1-1'
        rinfo['code'] = code
        # write_ri_to_db(rinfo)
        df = pd.DataFrame()
        df = df.from_dict(rinfo, orient='index')
        dfs1.append(df.T)

    if len(dfs) > 0:
        df = pd.concat(dfs)
        df.adate = pd.to_datetime(df.adate)
        df.xdate = pd.to_datetime(df.xdate)
        df.rdate = pd.to_datetime(df.rdate)
        df.give = df.give.astype(np.float64)
        df.trans = df.trans.astype(np.float64)
        df.divpay = df.divpay.astype(np.float64)
        df['type'] = 'bonus'
        #print df
        key = 'b' + code
        brsStore.append('bonus', df, min_itemsize={'values': 50})
        logging.info('Info %s has saved bonus' % code)
        #df.to_hdf('d:\\HDF5_Data\\binfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty bonus' % code)
        #logging.info('Info len bitems %d' % len(bitems))

    if len(dfs1) > 0:
        df1 = pd.concat(dfs1)
        df1.adate = pd.to_datetime(df1.adate)
        df1.xdate = pd.to_datetime(df1.xdate)
        df1.rdate = pd.to_datetime(df1.rdate)
        df1.ri = df1.ri.astype(np.float64)
        df1.riprice = df1.riprice.astype(np.float64)
        df1.basecap = df1.basecap.astype(np.float64)
        df1['type'] = 'rightsissue'
        #print df
        key = 'r' + code
        brsStore.append('rightsissue', df1, min_itemsize = {'values': 50})
        #df.to_hdf('d:\\HDF5_Data\\rinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty righsissue' % code)
        #logging.info('Info len ritems %d' % len(ritems))


def is_digit_or_point(c):
    if(str.isdigit(c)):
        return True
    elif(c == '.'):
        return True
    else:
        return False

def get_stock_change(code, brsStore, timeout=5):
    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/' + code + r'.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode('gbk')
    selector = etree.HTML(ct)
    tables = selector.xpath('//*[@id="con02-1"]/table')
    retry = 0
    while len(tables) < 1 and retry < 10:
        content = requests.get(url, timeout=timeout).content
        selector = etree.HTML(content)
        tables = selector.xpath('//*[@id="con02-1"]/table')
        retry += 1
        logging.info('Info retrying %d" stock change' % retry)

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
            #write_stockchange_to_db(sinfo)
            df = pd.DataFrame()
            df = df.from_dict(sinfo, orient='index')
            dfs.append(df.T)
    if len(dfs) > 0:
        df = pd.DataFrame()
        df = pd.concat(dfs)
        df.adate = pd.to_datetime(df.adate)
        df.xdate = pd.to_datetime(df.xdate)
        df.totalshare = df.totalshare.astype(np.float64)
        df.tradeshare = df.tradeshare.astype(np.float64)
        df.limitshare = df.limitshare.astype(np.float64)
        df.prevts = df.prevts.astype(np.float64)
        df.reason = df.reason.str.encode('utf-8')
        df['type'] = 'stockchange'
        #print df
        key = 's' + code
        brsStore.append('stockchange', df, min_itemsize={'values': 50})
        #df.to_hdf('d:\\HDF5_Data\\sinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty stock change' % code)
        logging.info('Info len sitems %d' % len(tables))

def get_bonus_ri_sc(retry=50, pause=1):
    brsStore = pd.HDFStore('D:\\HDF5_Data\\brsInfo.h5', complib='blosc', mode='w')
    target_list = getcodelist()
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    print 'retrieving bonus_and_ri' + row.code
                    get_bonus_and_ri(row.code, brsStore)
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s bonus_and_ri data for %s successfully' % row.code)
                    break
            row = next(itr)
    except StopIteration as e:
        pass

    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    print 'retrieving stock change' + row.code
                    get_stock_change(row.code, brsStore)
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s stock change data for %s successfully' % row.code)
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    brsStore.close()

def get_stock_daily_data_163(code, daykStore, startdate = datetime.date(1997,1,2), timeout=3):
    sdate = startdate.strftime('%Y%m%d')
    enddate = datetime.date.today() - datetime.timedelta(days=1)
    edate = enddate.strftime('%Y%m%d')
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
        data['nameutf'] = 'utf8'
        data.nameutf = data.name.str.encode('utf-8')
        del data['name']
        data.columns = ['code', 'close', 'high', 'low', 'open', 'prevclose', 'netchng', 'pctchng',
                        'turnoverrate', 'vol', 'amo', 'totalcap', 'tradeablecap', 'name']
        data['hfqratio'] = 1.0
        data = data.reset_index()
        data = data.set_index(['code', 'date'])
#        data.to_hdf('d:\\HDF5_Data\\dailydata.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
        data = data.sort_index()
        daykStore.append('dayk', data, min_itemsize={'values': 30})


def get_full_daily_data_163(retry=50, pause=1):
    daykStore = pd.HDFStore('D:\\HDF5_Data\\dailydata.h5', complib='blosc', mode='w')
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_stock_daily_data_163(row.code, daykStore)
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get daily data for %s successfully' % row.code)
                    cnt += 1
                    print 'retrieved ' + row.code + ', ' + str(cnt) + ' of ' + str(llen)
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    daykStore.close()

def get_delta_daily_data_163(retry=50, pause=1):
    daykStore = pd.HDFStore('D:\\HDF5_Data\\dailydata.h5', complib='blosc', mode='a')
    tmpdf = daykStore.select('dayk', start=-10000)
    if tmpdf.empty:
        print 'error, empty dayk'
        logging.info('error, empty dayk')
        return
    tmpdf = tmpdf.sort_index(level=1, ascending=True)
    startdate = tmpdf.index.get_level_values(1)[-1] + datetime.timedelta(days=1)
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_stock_daily_data_163(row.code, daykStore, startdate)
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get delta daily data for %s successfully' % row.code)
                    cnt += 1
                    print 'retrieved delta ' + row.code + ', ' + str(cnt) + ' of ' + str(llen)
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    daykStore.close()

def close_check(row):
    if row.open - 0.0 < 0.000001:
        row.close = 0.0
    row.totalcap *= 10000
    row.tradeablecap *= 10000
    return row

#not used, still lack some total cap
def get_today_all_from_sina(retry=50, pause=10):
    df = utility.get_today_all()

    df.drop('per', 1, inplace=True)
    df.drop('pb', 1, inplace=True)
    df.drop('buy', 1, inplace=True)
    df.drop('sell', 1, inplace=True)
    df.drop('ticktime', 1, inplace=True)
    df.drop('symbol', 1, inplace=True)

    #df[['amount', 'changepercent', 'code', 'high', 'low', 'mktcap', 'name', 'nmc', 'open', 'pricechange', 'settlement', 'trade', 'turnoverratio', 'volume']] = df[['trade', 'high', 'low', 'open', 'settlement', 'pricechange', 'changepercent', 'turnoverratio', 'volume', 'amount', 'mktcap', 'nmc', 'name', 'code']]
    #df.columns = ['close', 'high', 'low', 'open', 'prevclose', 'netchng', 'pctchng', 'turnoverrate', 'vol', 'amo', 'totalcap', 'tradeablecap', 'name', 'code']
    df.columns = ['amo', 'pctchng', 'code', 'high', 'low', 'totalcap', 'name', 'tradeablecap', 'open', 'netchng',
                  'prevclose', 'close', 'turnoverrate', 'vol']
    df = df.apply(close_check, axis=1)
    df['hfqratio'] = 1
    df.code = df.code.str.encode('utf-8')
    df.name = df.name.str.encode('utf-8')

    #get what missed in sina today all
    target_list = getcodelist()
    target_list = target_list.set_index('code')
    diff = target_list.index.difference(df.code).str.encode('utf-8')
    missed = utility.get_realtime_all_st(diff.values)
    missed = missed[['name', 'open', 'pre_close', 'price', 'high', 'low', 'volume', 'amount', 'date', 'code']]
    missed.columns = ['name', 'open', 'prevclose', 'close', 'high', 'low', 'vol', 'amo', 'date', 'code']
    missed.amo = missed.amo.astype(np.int64)
    missed.code = missed.code.str.encode('utf-8')
    missed.name = missed.name.str.encode('utf-8')
    missed = missed.set_index(['code', 'date'])

    date = datetime.date.today()
    df['date'] = date
    df = df.set_index(['code', 'date'])
    df = df.combine_first(missed)
    df = df.fillna(0)

    df.to_hdf('d:\\HDF5_Data\\today.hdf', 'tmp', mode='w', format='t', complib='blosc')


def close2PrevClose(x):
    r = pd.Series(0, x.index)
    x.close.replace('0', inplace=True)
    r[1:] = x.ix[:len(x) - 1].close.values
    return r.reset_index(level=0, drop=True)

def cumprod(x):
    return x.cumprod()

def calcFullRatio():
    t1 = datetime.datetime.now()
    dayk = pd.HDFStore('d:\\HDF5_Data\\dailydata.h5', mode='a', complib='blosc')
    brs = pd.HDFStore('d:\\HDF5_Data\\brsInfo.h5', mode='r', complib='blosc')
    df = dayk.select('dayk')
    bi = brs.select('bonus')
    ri = brs.select('rightsissue')
    si = brs.select('stockchange')
    brs.close()

    #df = df[df.index.get_level_values(0).str.contains(ashare_pattern)]
    df = df.sort_index()
    bi = bi[bi.xdate > '1997-1-1']
    bi = bi.reset_index(drop=True).groupby(['code', 'xdate']).sum()
    if not bi.index.is_unique:
        raise IndexError('bonus index is not unique')
    ri = ri[ri.xdate > '1997-1-1']
    ri = ri.set_index(['code', 'xdate'])
    if not ri.index.is_unique:
        raise IndexError('rightsissue index is not unique')
    si = si[si.xdate > '1997-1-1']
    si = si[(si.reason == '股权分置') | (si.reason == '拆细')].set_index(['code', 'xdate'])
    si = si[(si.tradeshare > si.prevts) & (si.prevts > 0)]
    if not si.index.is_unique:
        raise IndexError('stockchange index is not unique')
    #merge bi, ri
    bi = bi.combine_first(ri[['riprice', 'ri']])
    bi = bi.fillna(0)
    sPclose = df.groupby(level=0).apply(close2PrevClose)
    bi['pclose'] = sPclose.reindex(bi.index, method='pad')
    bi['b'] = (bi.pclose - bi.riprice) / bi.riprice
    #bi = bi[bi.b > 0.05]
    print datetime.datetime.now() - t1

    t2 = datetime.datetime.now()

    adjpclose = (bi.pclose - (bi.divpay / 10) + bi.riprice * bi.ri / 10) / (1 + (bi.give + bi.trans) / 10 + bi.ri / 10)
    adjpclose = round_series(adjpclose)
    factor = bi.pclose / adjpclose
    #factor = (bi.give + bi.trans) / 10 + bi.divpay / 10 / (bi.pclose - (bi.divpay / 10)) + bi.pclose * (1 + bi.ri / 10) / (bi.pclose + bi.riprice * bi.ri / 10)

    all = si.tradeshare / si.prevts
    all = all.combine_first(factor)
    all.to_hdf('d:\\HDF5_Data\\hfqfactor.hdf', 'factor', mode='w', format='t', complib='blosc')
    combinedIndex = all.index.union(df.index)
    all = all.reindex(combinedIndex, fill_value=1)
    all = all.groupby(level=0).apply(cumprod)
    df.hfqratio = all
    #df.hfqratio.fillna(1, inplace=True)

    dayk.put('dayk', df, format='t')

    dayk.close()
    print datetime.datetime.now() - t2
def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['full', 'delta'], default='full', help='download type')

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/testAnt.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    if (type == 'full'):
        updateindexlist()
        get_all_full_index_daily()
        updatestocklist(5, 5)
        get_bonus_ri_sc()
        get_full_daily_data_163()
        calcFullRatio()
    elif (type == 'delta'):
        updateindexlist()
        get_all_full_index_daily()
        updatestocklist(5, 5)
        get_bonus_ri_sc()
        get_delta_daily_data_163()
        calcFullRatio()


    #get_bonus_ri_sc()
    #()

    #get_today_all_from_sina()




