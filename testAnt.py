# -*- coding:utf-8 -*-
import zipfile
import pandas as pd
import numpy as np
import requests
from lxml import etree
from io import StringIO, BytesIO
from utility import round_series, getcodelist, getindexlist, reconnect,st_pattern
import random, string
import argparse
import utility
import datetime as dt
import time
import logging
import sys
import tushare as ts
import os
from shutil import copyfile
import pymongo


ashare_pattern = r'^0|^3|^6'


report_date = ['2004-06-30', '2004-09-30', '2004-12-31',
               '2005-03-31', '2005-06-30', '2005-09-30', '2005-12-31',
               '2006-03-31', '2006-06-30', '2006-09-30', '2006-12-31',
               '2007-03-31', '2007-06-30', '2007-09-30', '2007-12-31',
               '2008-03-31', '2008-06-30', '2008-09-30', '2008-12-31',
               '2009-03-31', '2009-06-30', '2009-09-30', '2009-12-31',
               '2010-03-31', '2010-06-30', '2010-09-30', '2010-12-31',
               '2011-03-31', '2011-06-30', '2011-09-30', '2011-12-31',
               '2012-03-31', '2012-06-30', '2012-09-30', '2012-12-31',
               '2013-03-31', '2013-06-30', '2013-09-30', '2013-12-31',
               '2014-03-31', '2014-06-30', '2014-09-30', '2014-12-31',
               '2015-03-31', '2015-06-30', '2015-09-30', '2015-12-31',
               '2016-03-31', '2016-06-30', '2016-09-30', '2016-12-31',
               '2017-03-31', '2017-06-30', '2017-09-30', '2017-12-31',
               '2018-03-31', '2018-06-30', '2018-09-30', '2018-12-31',
               '2019-03-31', '2019-06-30', '2019-09-30', '2019-12-31',
               '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31',
               '2021-03-31', '2021-06-30', '2021-09-30', '2021-12-31',
               '2022-03-31', '2022-06-30', '2022-09-30', '2022-12-31',
               '2023-03-31', '2023-06-30', '2023-09-30', '2023-12-31',
               '2024-03-31', '2024-06-30', '2024-09-30', '2024-12-31',
               '2025-03-31', '2025-06-30', '2025-09-30', '2025-12-31',
               '2026-03-31', '2026-06-30', '2026-09-30', '2026-12-31',
               '2027-03-31', '2027-06-30', '2027-09-30', '2027-12-31',
               '2028-03-31', '2028-06-30', '2028-09-30', '2028-12-31',
               '2029-03-31', '2029-06-30', '2029-09-30', '2029-12-31',
               '2030-03-31', '2030-06-30', '2030-09-30', '2030-12-31',
               '2031-03-31', '2031-06-30', '2031-09-30', '2031-12-31',
               '2032-03-31', '2032-06-30', '2032-09-30', '2032-12-31',
               '2033-03-31', '2033-06-30', '2033-09-30', '2033-12-31',
               '2034-03-31', '2034-06-30', '2034-09-30', '2034-12-31',
               '2035-03-31', '2035-06-30', '2035-09-30', '2035-12-31',
               '2036-03-31', '2036-06-30', '2036-09-30', '2036-12-31',
               '2037-03-31', '2037-06-30', '2037-09-30', '2037-12-31',
               '2038-03-31', '2038-06-30', '2038-09-30', '2038-12-31',
               '2039-03-31', '2039-06-30', '2039-09-30', '2039-12-31',
               '2040-03-31', '2040-06-30', '2040-09-30', '2040-12-31',
               '2041-03-31', '2041-06-30', '2041-09-30', '2041-12-31',
               '2042-03-31', '2042-06-30', '2042-09-30', '2042-12-31',
               '2043-03-31', '2043-06-30', '2043-09-30', '2043-12-31',
               '2044-03-31', '2044-06-30', '2044-09-30', '2044-12-31',
               '2045-03-31', '2045-06-30', '2045-09-30', '2045-12-31',
               '2046-03-31', '2046-06-30', '2046-09-30', '2046-12-31',
               '2047-03-31', '2047-06-30', '2047-09-30', '2047-12-31',
               '2048-03-31', '2048-06-30', '2048-09-30', '2048-12-31',
               '2049-03-31', '2049-06-30', '2049-09-30', '2049-12-31',
               '2050-03-31', '2050-06-30', '2050-09-30', '2050-12-31',
               ]
rd = pd.DataFrame(index=report_date)
rd['end'] = report_date
rd['start'] = ['1989-12-31'] + report_date[:-1]

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
    data = pd.read_csv(StringIO(r.content.decode(encoding='gbk')), index_col=u'日期', parse_dates=True)
    data.index.names = ['date']
    if not data.empty:
        data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','vol','amo']
        data['code'] = code
        #data.name = data.name.str.encode('utf-8')
        data['close'] = data['close'].apply(convertNone)
        data['high'] = data['high'].apply(convertNone)
        data['low'] = data['low'].apply(convertNone)
        data['open'] = data['open'].apply(convertNone)
        data['prevclose'] = data['prevclose'].apply(convertNone)
        data['netchng'] = data['netchng'].apply(convertNone)
        data['pctchng'] = data['pctchng'].apply(convertNone)
        data['vol'] = data['vol'].apply(convertNone)
        data['amo'] = data['amo'].apply(convertNone)

        idxStore.append('idx', data, min_itemsize={'name': 30}, data_columns=True)

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
        #all.CODE = all.CODE.str.encode('utf-8')
        #all.NAME = all.NAME.str.encode('utf-8')
        all.CODE = all.CODE.str.slice(1)
        all = all[['CODE', 'NAME']].reset_index(drop=True)
        all.columns = ['code', 'name']
        all.to_hdf('d:/hdf5_data/indexlist.hdf', 'day', mode='w', format='t', complib='blosc', data_columns=True)
        logging.info('finished to get index list...' + str(dt.datetime.now()))
    else:
        logging.info('failed to get list...' + str(dt.datetime.now()))

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
    logging.info('start retrieving stock list...' + str(dt.datetime.now()))
    for _ in range(retry_count):
        dfs = []
        print (time.clock())
        try:
            # get info of shen zheng
            r = requests.get(sz_onboard_url)  # , proxies=proxies)
            sz_on = pd.read_html(r.content)
            if sz_on:
                df2 = sz_on[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                #df2.code = df2.code.str.encode('utf-8')
                #df2.name = df2.name.str.encode('utf-8')
                df2['status'] = 1
                dfs.append(df2)

            r = requests.get(sz_quit_onhold_url)  # , proxies=proxies)
            sz_quit_onhold = pd.read_html(r.content)
            if sz_quit_onhold:
                df2 = sz_quit_onhold[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                #df2.code = df2.code.str.encode('utf-8')
                #df2.name = df2.name.str.encode('utf-8')
                df2['status'] = 0
                dfs.append(df2)

            r = requests.get(sz_quit_url)  # , proxies=proxies)
            sz_quit = pd.read_html(r.content)
            if sz_quit:
                df2 = sz_quit[0].iloc[1:, [0, 1]]
                df2.columns = ['code', 'name']
                #df2.code = df2.code.str.encode('utf-8')
                #df2.name = df2.name.str.encode('utf-8')
                df2['status'] = -1
                dfs.append(df2)

            # get info of shang hai
            r = requests.get(sh_onboard_url, headers=header)  # , proxies=proxies,)
            # with open("sh_onboard.xls", "wb") as code:
            #    code.write(r.content)
            sh_on = pd.read_table(StringIO(r.content.decode(encoding='gbk', errors='ignore')))
            if not sh_on.empty:
                df1 = sh_on.iloc[0:, [2, 3]]
                df1.columns = ['code', 'name']
                df1.code = df1.code.astype(str)
                #df1.name = df1.name.str.encode('utf-8')
                df1['status'] = 1
                dfs.append(df1)

            r = requests.get(sh_quit_onhold_url, headers=header)  # , proxies=proxies,)
            # with open("sh_quit_onhold.xls", "wb") as code:
            #    code.write(r.content)
            sh_onhold = pd.read_table(StringIO(r.content.decode(encoding='gbk', errors='ignore')))
            if not sh_onhold.empty:
                df1 = sh_onhold.iloc[0:, [0, 1]]
                df1.columns = ['code', 'name']
                df1.code = df1.code.astype(str)
                #df1.name = df1.name.str.encode('utf-8')
                df1['status'] = 0
                dfs.append(df1)

            r = requests.get(sh_quit_url, headers=header)  # , proxies=proxies,)
            # with open("sh_quit.xls", "wb") as code:
            #    code.write(r.content)
            sh_quit = pd.read_table(StringIO(r.content.decode(encoding='gbk', errors='ignore')))
            if not sh_quit.empty:
                df1 = sh_quit.iloc[0:, [0, 1]]
                df1.columns = ['code', 'name']
                df1.code = df1.code.astype(str)
                #df1.name = df1.name.str.encode('utf-8')
                df1['status'] = -1
                dfs.append(df1)
        except Exception as e:
            err = 'Error %s' % e
            logging.info(err)
            time.sleep(pause)
        else:
            print (time.clock())
            df = pd.concat(dfs)
            df = df.drop_duplicates(subset='code', keep='last')
            df = df.set_index('code')
            df = df[df.index.get_level_values(0).str.contains(ashare_pattern)]
            df = df.sort_index()
            df.to_hdf('d:/HDF5_Data/stocklist.hdf', 'list', mode='w', format='t', complib='blosc', data_columns=True)
            logging.info('finished retrieving ' + str(len(df)) + ' successfully...' + str(dt.datetime.now()))
            return
    logging.info('get_stock_list failed...' + str(dt.datetime.now()))

def get_bonus_and_ri(code, brsStore, timeout=5):
    url = r'http://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/'+ code + r'.phtml'
    content = requests.get(url, timeout=timeout).content
    ct = content.decode(encoding='gbk', errors='ignore')
    selector = etree.HTML(ct)
    bitems = selector.xpath('//*[@id="sharebonus_1"]/tbody/tr')
    ritems = selector.xpath('//*[@id="sharebonus_2"]/tbody/tr')
    retry = 0
    while (len(bitems) < 1 or len(ritems) < 1) and retry < 20:
        content = requests.get(url, timeout=timeout).content
        selector = etree.HTML(content)
        bitems = selector.xpath('//*[@id="sharebonus_1"]/tbody/tr')
        ritems = selector.xpath('//*[@id="sharebonus_2"]/tbody/tr')
        retry += 1
        logging.info('Info retrying %d" bonus' % retry)
        reconnect()
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
        df.replace(to_replace='', value=0, inplace=True)
        df.adate = pd.to_datetime(df.adate)
        df.xdate = pd.to_datetime(df.xdate)
        df.rdate = pd.to_datetime(df.rdate)
        df.give = df.give.astype(np.float64)
        df.trans = df.trans.astype(np.float64)
        df.divpay = df.divpay.astype(np.float64)
        df['type'] = 'bonus'
        #print df
        key = 'b' + code
        brsStore.append('bonus', df, min_itemsize={'values': 50}, data_columns=True)
        #logging.info('Info %s has saved bonus' % code)
        #df.to_hdf('d:\\HDF5_Data\\binfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True, data_columns=True)
    else:
        logging.info('Info %s has empty bonus' % code)
        #logging.info('Info len bitems %d' % len(bitems))

    if len(dfs1) > 0:
        df1 = pd.concat(dfs1)
        df1.replace(to_replace='', value=0, inplace=True)
        df1.adate = pd.to_datetime(df1.adate)
        df1.xdate = pd.to_datetime(df1.xdate)
        df1.rdate = pd.to_datetime(df1.rdate)
        df1.ri = df1.ri.astype(np.float64)
        df1.riprice = df1.riprice.astype(np.float64)
        df1.basecap = df1.basecap.astype(np.float64)
        df1['type'] = 'rightsissue'
        #print df
        key = 'r' + code
        brsStore.append('rightsissue', df1, min_itemsize = {'values': 50}, data_columns=True)
        #df.to_hdf('d:\\HDF5_Data\\rinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True, data_columns=True)
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
    ct = content.decode(encoding='gbk', errors='ignore')
    selector = etree.HTML(ct)
    tables = selector.xpath('//*[@id="con02-1"]/table')
    retry = 0
    while len(tables) < 1 and retry < 20:
        content = requests.get(url, timeout=timeout).content
        selector = etree.HTML(content)
        tables = selector.xpath('//*[@id="con02-1"]/table')
        retry += 1
        logging.info('Info retrying %d" stock change' % retry)
        reconnect()

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
            sinfo['totalshare'] = sinfo['totalshare'].strip(' -万股')
            sinfo['tradeshare'] = ''.join(table.xpath('tbody/tr[7]/td[%d]/text()'%col))
            sinfo['tradeshare'] = sinfo['tradeshare'].strip(' -万股')
            sinfo['limitshare'] = ''.join(table.xpath('tbody/tr[9]/td[%d]/text()'%col))
            sinfo['limitshare'] = sinfo['limitshare'].strip(' -万股')
            sinfo['prevts'] = prev_tradeable_share
            prev_tradeable_share = sinfo['tradeshare']
            #write_stockchange_to_db(sinfo)
            df = pd.DataFrame()
            df = df.from_dict(sinfo, orient='index')
            dfs.append(df.T)
    if len(dfs) > 0:
        df = pd.DataFrame()
        df = pd.concat(dfs)
        df.replace(to_replace='', value=0, inplace=True)
        df.adate = pd.to_datetime(df.adate)
        df.xdate = pd.to_datetime(df.xdate)
        df.totalshare = df.totalshare.astype(np.float64)
        df.tradeshare = df.tradeshare.astype(np.float64)
        df.limitshare = df.limitshare.astype(np.float64)
        df.prevts = df.prevts.astype(np.float64)
        df.reason = df.reason.astype(str)
        df['type'] = 'stockchange'
        #print df
        key = 's' + code
        brsStore.append('stockchange', df, min_itemsize={'values': 50}, data_columns=True)
        #df.to_hdf('d:\\HDF5_Data\\sinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True, data_columns=True)
    else:
        logging.info('Info %s has empty stock change' % code)
        logging.info('Info len sitems %d' % len(tables))

def get_bonus_ri_sc(retry=50, pause=1):
    brsStore = pd.HDFStore('D:\\HDF5_Data\\brsInfo.h5', complib='blosc', mode='w')
    target_list = getcodelist()

    size = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    print ('retrieving bonus_and_ri' + row.code)
                    get_bonus_and_ri(row.code, brsStore)
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    reconnect()
                else:
                    cnt+=1
                    logging.info('get today\'s bonus_and_ri data for %s successfully, %d of %d' % (row.code, cnt, size))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    print ('retrieving stock change' + row.code)
                    get_stock_change(row.code, brsStore)
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    reconnect()
                else:
                    cnt+=1
                    logging.info('get today\'s stock change data for %s successfully, %d of %d' % (row.code, cnt, size))
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    brsStore.close()

def get_stock_daily_data_163(db, code, daykStore, startdate = dt.date(1997,1,2), timeout=3):
    sdate = startdate.strftime('%Y%m%d')
    enddate = dt.date.today()# - dt.timedelta(days=1)
    edate = enddate.strftime('%Y%m%d')
    if code[0] == '6':
        url = r'http://quotes.money.163.com/service/chddata.html?code=0' + code + r'&start=' + sdate + r'&end=' + edate + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
    else:
        url = r'http://quotes.money.163.com/service/chddata.html?code=1' + code + r'&start=' + sdate + r'&end=' + edate + r'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content.decode(encoding='gbk')), index_col=u'日期', parse_dates=True)
    data.index.names = ['date']
    logging.info('get daily data for %s ' % (code))
    if not data.empty:
        data.columns = ['code','name','close','high','low','open','prevclose','netchng','pctchng','turnoverrate','vol','amo','totalcap','tradeablecap']
        data['code'] = pd.Series(code, index=data.index)
        data['netchng'] = data['netchng'].apply(convertNone)
        data['pctchng'] = data['pctchng'].apply(convertNone)
        data['turnoverrate'] = data['turnoverrate'].apply(convertNone)
        #data['nameutf'] = 'utf8'
        #data.nameutf = data.name.str.encode('utf-8')
        #del data['name']
        #data.columns = ['code', 'close', 'high', 'low', 'open', 'prevclose', 'netchng', 'pctchng', 'turnoverrate', 'vol', 'amo', 'totalcap', 'tradeablecap', 'name']
        #data['hfqratio'] = 1.0
        #data['stflag'] = 0
        data = data.reset_index()
        try:
            dd = data.T.to_dict().values()
            db.day.insert_many(dd, ordered=False)
        except Exception as e:
            pass

        data = data.set_index(['code', 'date'])
        data = data.sort_index()
        #data.to_hdf('d:/hdf5_data/dailydatadelta.hdf', 'day', mode='a', format='t', append=True, data_columns=True)
        daykStore.append('day', data, min_itemsize={'values': 30}, data_columns=True)

        logging.info('get daily data for %s successfully' % (code))

def get_fundmental_data_163(code, timeout=3):
    url = 'http://quotes.money.163.com/service/zycwzb_' + code + '.html?type=report'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content.decode(encoding='gbk'))).T.dropna()
    data = data.replace('--', np.NaN)
    data.to_csv('D:/HDF5_Data/fundamental/163/cwzb/'+code+'.csv', header=False, encoding='gbk')

    url = 'http://quotes.money.163.com/service/zycwzb_' + code + '.html?type=report&part=ylnl'

    r = requests.get(url, timeout=timeout)
    data = pd.read_csv(StringIO(r.content.decode(encoding='gbk'))).T.dropna()
    data = data.replace('--', np.NaN)
    data.to_csv('D:/HDF5_Data/fundamental/163/ylnl/' + code + '.csv', header=False, encoding='gbk')

def get_full_daily_data_163(conn, retry=50, pause=1):
    daykStore = pd.HDFStore('D:\\HDF5_Data\\dailydata.h5', complib='blosc', mode='w')
    db = conn['day']
    target_list = getcodelist()
    size = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_stock_daily_data_163(db, row.code, daykStore)
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    reconnect()
                else:
                    cnt += 1
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    daykStore.close()

def get_delta_daily_data_163(retry=50, pause=1):
    daykStore = pd.HDFStore('D:/HDF5_Data/dailydatadelta.h5', complib='blosc', mode='a')
    tmpdf = daykStore.select('day')
    if tmpdf.empty:
        print ('error, empty day')
        logging.info('error, empty day')
        return
    tmpdf = tmpdf.sort_index(level=1, ascending=True)
    startdate = tmpdf.index.get_level_values(1)[-1] + dt.timedelta(days=1)
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
                    cnt += 1
                    print ('retrieved delta ' + row.code + ', ' + str(cnt) + ' of ' + str(llen))
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    daykStore.close()

def checkandappendaily(retry=50, pause=1):
    c = pd.bdate_range(start='2017-12-31', end='2018-12-31')
    holidays = [dt.date(2018, 1, 1), dt.date(2018, 2, 15), dt.date(2018, 2, 16), dt.date(2018, 2, 19),
                dt.date(2018, 2, 20), dt.date(2018, 2, 21), dt.date(2018, 4, 5), dt.date(2018, 4, 6),
                dt.date(2018, 4, 30), dt.date(2018, 5, 1), dt.date(2018, 6, 18), dt.date(2018, 9, 24),
                dt.date(2018, 10, 1), dt.date(2018, 10, 2), dt.date(2018, 10, 3), dt.date(2018, 10, 4),
                dt.date(2018, 10, 5)]
    c = c.difference(holidays)
    c = c[c.slice_indexer(end=dt.date.today() - dt.timedelta(days=1))]
    lasttradeday = str(c[-1].date())

    daykStore = pd.HDFStore('D:/HDF5_Data/dailydatadelta.h5', complib='blosc', mode='a')
    tmpdf = daykStore.select('day', where='date=\'%s\'' % lasttradeday)
    if tmpdf.empty:
        data = tmpdf.append(pd.read_hdf('d:/hdf5_data/lastday.hdf'))
        daykStore.append('day', data, min_itemsize={'values': 30}, data_columns=True)
        logging.warning('163 missed last trade date data.')

    daykStore.close()

def get_full_daily_data_sina(retry=50, pause=1):
    daykStore = pd.HDFStore('D:/HDF5_Data/dailydata_sina.h5', complib='blosc', mode='w')
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    data = ts.get_h_data(row.code, start='1990-01-01', autype='hfq', drop_factor=False)
                    if data is None:
                        data
                    elif data.empty:
                        logging.info('Error, empty day for code: %s' % (row.code))
                    else:
                        data = data.sort_index()
                        data['code'] = row.code
                        data = data.set_index(['code', data.index])
                        daykStore.append('day', data, data_columns=True)
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get daily data for %s successfully' % row.code)
                    cnt += 1
                    print ('retrieved ' + row.code + ', ' + str(cnt) + ' of ' + str(llen))
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    daykStore.close()

def get_delta_daily_data_sina(retry=50, pause=1):
    daykStore = pd.HDFStore('D:\\HDF5_Data\\dailydata_sina.h5', complib='blosc', mode='a')
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    startdate = '1997-01-01'
                    df = daykStore.select('day', where='code==\'%s\'' %(row.code))
                    if not df.empty:
                        t = df.index.get_level_values(1)[-1] + dt.timedelta(days=1)
                        startdate = t.strftime('%Y-%m-%d')

                    data = ts.get_h_data(row.code, start=startdate, autype='hfq', drop_factor=False)
                    if data is None:
                        data
                    elif data.empty:
                        logging.info('Error, empty dayk for code: %s' % (row.code))
                    else:
                        data = data.sort_index()
                        data['code'] = row.code
                        data = data.set_index(['code', data.index])
                        daykStore.append('day', data, data_columns=True)
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get daily data for %s successfully' % row.code)
                    cnt += 1
                    print ('retrieved ' + row.code + ', ' + str(cnt) + ' of ' + str(llen))
                    break
            row = next(itr)
    except StopIteration as e:
        pass
    daykStore.close()

def getadate(astrdate):
    try:
        adate = dt.datetime.strptime(astrdate, '%Y-%m-%d')
    except ValueError:
        return dt.datetime(1900, 1, 1)
    else:
        return adate

def getreportdate(astrdate, type):
    if(astrdate == ''):
        astrdate = '1900-1-1'
    try:
        adate = dt.datetime.strptime(astrdate, '%Y-%m-%d')
    except ValueError:
        return dt.datetime(1900, 1, 1)
    year = adate.year
    month = adate.month
    day = adate.day
    ryear = year
    rmonth = 0
    rday  = 0
    if (type == '一季度报告'):
        rmonth = 3
        rday = 31
    elif (type == '中期报告'):
        rmonth = 6
        rday = 30
    elif (type == '三季度报告'):
        rmonth = 9
        rday = 30
    elif (type == '年度报告'):
        rmonth = 12
        rday = 31
        if(month == 12 and day == 31):
            pass
        else:
            ryear -= 1
    else:
        return dt.datetime(1900, 1, 1)
    return dt.datetime(ryear, rmonth, rday)

report_type = {
    '一季度报告' : 'yjdbg',
    '中期报告'  : 'zqbg',
    '三季度报告' : 'sjdbg',
    '年度报告': 'ndbg'
}
def getadate163(code, maxseq = 100):

    print(code)
    codes = []
    adates = []
    rdates = []

    for seq in range(0, maxseq):
        for _ in range(10):
            try:
                url = 'http://quotes.money.163.com/f10/gsgg_' + code + ',dqbg,' + str(seq) + '.html'
                content = requests.get(url, timeout=5).content
                ct = content.decode(encoding='utf-8', errors='ignore')
                selector = etree.HTML(ct)
                nodatatxt = ''.join(selector.xpath('//*[@id="newsTabs"]/div/table/tr/td/text()'))
                break
            except:
                reconnect()
                continue
            else:
                break

        if (nodatatxt.find("暂无数据") != -1):
            break
        rows = selector.xpath('//*[@id="newsTabs"]/div/table/tr')
        for row in rows:
            stradate = ''.join(row.xpath('td[2]/text()'))
            rtype = ''.join(row.xpath('td[3]/text()'))
            rdate = getreportdate(stradate, rtype)
            adate = getadate(stradate)
            codes.append(code)
            adates.append(adate)
            rdates.append(rdate)


    df = pd.DataFrame(data={'code':codes, '报告日期':rdates, '公告日期':adates})
    df = df.drop_duplicates(['code', '报告日期'], keep='last')
    df = df.set_index(['code', '报告日期']).sort_index()
    if not df.empty:
        df = df.loc(axis=0)[:, '1990-1-1':]
    #print(df)
    return df

def getadate163all():
    t1 = time.clock()
    target_list = getcodelist()
    dfs = []
    for code in target_list.code.values:
        dfs.append(getadate163(code, 100))

    all = pd.concat(dfs)
    #print(all.to_string())
    all.to_hdf('d:/hdf5_data/adate163.hdf', 'fundamental', complib='blosc', data_columns=True)
    logging.info('getadate163all done ' + str(time.clock() - t1))

def getadatesina(code, type):

    print(code)
    codes = []
    adates = []
    rdates = []

    url = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vCB_BulletinYi/stockid/' + code + '/page_type/' + report_type[type] + '.phtml'

    for _ in range(10):
        try:
            content = requests.get(url, timeout=5).content
            ct = content.decode(encoding='gbk', errors='ignore')
            selector = etree.HTML(ct)

            nodatatxt = ''.join(selector.xpath('//*[@id="con02-7"]/table/tr/td/text()'))
            if (nodatatxt.find("暂时没有数据") != -1):
                return pd.DataFrame()

            itr = selector.xpath('//*[@class="datelist"]/ul')[0].itertext()
        except:
            reconnect()
        else:
            break

    try:
        while True:
            txt = next(itr).strip()
            rdate = getreportdate(txt, type)
            adate = getadate(txt)
            codes.append(code)
            adates.append(adate)
            rdates.append(rdate)
    except StopIteration:
        pass

    df = pd.DataFrame(data={'code':codes, '报告日期':rdates, '公告日期':adates})
    df = df.drop_duplicates(['code', '报告日期'], keep='last')
    df = df.set_index(['code', '报告日期']).sort_index()
    if not df.empty:
        df = df.loc(axis=0)[:, '1990-1-1':]

    return df

def getadatesinaall():
    t1 = time.clock()
    target_list = getcodelist()
    dfs = []
    for code in target_list.code.values:
        dfs.append(getadatesina(code, '一季度报告'))
        dfs.append(getadatesina(code, '中期报告'))
        dfs.append(getadatesina(code, '三季度报告'))
        dfs.append(getadatesina(code, '年度报告'))

    all = pd.concat(dfs)
    all.to_hdf('d:/hdf5_data/adatesina.hdf', 'fundamental', format='t', data_columns=True)
    logging.info('getadatesinaall done ' + str(time.clock() - t1))

def readsinapage(url, match='.+', att=None):
    for _ in range(10):
        try:
            pages = pd.read_html(url, encoding='gbk', match=match, attrs=att)
            if len(pages) < 1:
                reconnect()
            else:
                return pages
        except Exception:
            reconnect()

def getfundamentalsinaric(code, delta=False):
    print(code)
    yr = dt.date.today().year - 1
    url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/' + code + '/ctrl/' + str(yr) + '/displaytype/4.phtml'
    page = readsinapage(url, match='历年数据')

    if delta:
        years = page[0].iloc[0].values[0].strip('历年数据: ').split(' ')[:2]
    else:
        years = page[0].iloc[0].values[0].strip('历年数据: ').split(' ')

    for year in years:
        url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/' + code + '/ctrl/' + str(year) + '/displaytype/4.phtml'
        page = readsinapage(url, att={'id':'BalanceSheetNewTable0'})
        page[0].dropna().T.replace('--', np.nan).to_csv('D:/HDF5_Data/fundamental/sina/cwzb/'+code+'_'+str(year)+'.csv', header=False, index=False, encoding='gbk')


def getfundamentalsinaall():
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    for code in target_list.code.values:
        getfundamentalsinaric(code)

def getfundamentalsinadelta():
    t1 = time.clock()
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    for code in target_list.code.values:
        getfundamentalsinaric(code, delta=True)

    logging.info('getfundamentalsinadelta done ' + str(time.clock() - t1))


def csvtohdf(source, type):
    t1 = time.clock()
    inputdir = 'd:/hdf5_data/fundamental/%s/%s/' % (source, type)
    outputfile = 'd:/hdf5_data/fundamental%s_%s.hdf'% (source, type)
    dfs = []
    all = os.listdir(inputdir)
    for file in all:
        print(file + str())
        try:
            csv = pd.read_csv(inputdir+file, encoding='gbk', parse_dates=['报告日期'], header=0, dtype=float)
            year = csv['报告日期'].dt.year.max()
            csv = csv.set_index('报告日期')
            idx = pd.DatetimeIndex([dt.datetime(year, 3, 31, 0, 0, 0), dt.datetime(year, 6, 30, 0, 0, 0), dt.datetime(year, 9, 30, 0,0,0), dt.datetime(year,12,31,0,0,0)])
            csv = csv.reindex(idx)
            #csv['年度报告'] = np.nan
            #csv.loc[dt.datetime(year, 12, 31, 0, 0, 0), '年度报告'] = csv['每股收益_调整后(元)'][-1]
            csv['code'] = file[:6]
            csv = csv.set_index(['code', csv.index])
            csv = csv.sort_index().rolling(window=2).apply(lambda x: x[1] - x[0]).combine_first(csv[:1])
        except ValueError:
            pass
        else:
            dfs.append(csv)


    df = pd.concat(dfs)
    df = df.sort_index()
    df.to_hdf(outputfile, 'fundamental',mode='w', format='t', data_columns=True)

    logging.info('csvtohdf done ' + str(time.clock() - t1))

def processfundamental():
    t1 = time.clock()
    fdsina = pd.read_hdf('d:/hdf5_data/fundamentalsina_cwzb.hdf')
    eps = fdsina[['每股收益_调整后(元)', '主营业务利润(元)', '主营业务利润率(%)', '每股经营性现金流(元)','销售净利率(%)','销售毛利率(%)','加权净资产收益率(%)']]
    epsttm = eps.groupby(level=0, group_keys=False).rolling(window=4).sum()
    epsyoy = eps.groupby(level=0).resample('Y', level=1).sum()
    epsall = epsttm.combine_first(epsyoy)
    epsall = epsall.loc(axis=0)[:, '2007-1-1':].fillna(0)
    epsall = pd.DataFrame(epsall)
    adate = pd.read_hdf('d:/hdf5_data/adatesina.hdf')
    adate163 = pd.read_hdf('d:/hdf5_data/adate163.hdf')
    adate = adate.combine_first(adate163)
    adate = pd.DataFrame(adate)
    epsall['adate'] = adate
    epsall = epsall.sort_index(ascending=True)
    epstmp = epsall.groupby(level=0, group_keys=False).apply(lambda x: x.drop_duplicates('adate', keep='last'))
    epstmp = epstmp.dropna().reset_index().set_index(['code', 'adate']).sort_index()

    day = pd.read_hdf('d:/hdf5_data/dailydata.hdf', columns=['open'])
    #epstmp = epstmp.reindex(day.index, method='ffill')
    combinedIndex = epstmp.index.union(day.index)
    epstmp = epstmp.reindex(combinedIndex)
    epstmp = epstmp.groupby(level=0).fillna(method='ffill')

    combinedIndex = epsall.index.union(day.index)
    epsall = epsall.reindex(combinedIndex)
    epsall = epsall.groupby(level=0).fillna(method='ffill')


    #epsall = epstmp['每股收益_调整后(元)'].combine_first(epsall['每股收益_调整后(元)'])
    epsall = epstmp.combine_first(epsall)
    epsall = epsall.reindex(day.index)
    epsall['pe'] = day.open / epsall['每股收益_调整后(元)']

    epsall.to_hdf('d:/hdf5_data/fundamental.hdf', 'fundamental', mode='w', format='t', data_columns=True)
    logging.info('processfundamention done '+ str(time.clock()-t1))

def random_str(randomlength=8):
    a = list(string.ascii_letters)
    random.shuffle(a)
    return ''.join(a[:randomlength])

def getcninfooneric(code, startyear, endyear, type):
    if code[0] == '6':
        exchange = 'sh'
    else:
        exchange = 'sz'

    BOUNDARY = "----WebKitFormBoundary" + random_str(16)
    fields = [['K_code', ''],
         ['market', exchange],
         ['type', type],
         ['code', code],
         ['orgid', 'gs' + exchange + code],
         ['minYear', startyear],
         ['maxYear', endyear],
         ['hq_code', code],
         ['hq_k_code', ''],
         ['cw_code', code],
         ['cw_k_code', '']]
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '1107',
        'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY,
        # 'Cookie':'JSESSIONID=EDCC00C060A5E9338635CE524C7305FC',
        'Host': 'www.cninfo.com.cn',
        'Origin': 'http://www.cninfo.com.cn',
        'Proxy-Connection':'keep-alive',
        'Referer': 'http://www.cninfo.com.cn/cninfo-new/index',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    }
    r = requests.post("http://www.cninfo.com.cn/cninfo-new/data/download", data=body, headers=headers)

    df = pd.DataFrame()
    if zipfile.is_zipfile(BytesIO(r.content)):
        data = zipfile.ZipFile(BytesIO(r.content))
        data.extractall('d:/cninfo')
        return len(data.namelist())
    return 0
    '''
        for name in data.namelist():
            csv = pd.read_csv(data.open(StringIO(name).read()), parse_dates=[2], encoding='gbk', dtype={0: str})
            if len(csv) == 1:
                continue
            csv.fillna(0, inplace=True)
            csv.columns = ['code', 'name', 'date', 'exchange', 'pclose', 'open', 'volume', 'high', 'low', 'close', 'tradecnt', 'pctchange', 'amount']

            #csv.code = csv.code.str.strip().str.encode('utf-8')
            #csv.name = csv.name.str.encode('utf-8')
            #csv.exchange = csv.exchange.str.encode('utf-8')
            csv.amount = csv.amount.astype(float)
            csv.volume = csv.volume.astype(float)
            csv.tradecnt = csv.tradecnt.astype(float)

            df = df.append(csv)
    return df
    '''

def getcninfodaily(type='delta', retry=10, pause=2):
    target_list = getcodelist()
    if type == 'delta':
        mode = 'a'
        start = str(dt.date.today().year)
        end = start
        target_list = target_list[target_list.status > 0]
    else:
        mode = 'w'
        start = '1990'
        end = str(dt.date.today().year)

    #daykStore = pd.HDFStore('D:/HDF5_Data/dailydata_cninfo.h5', complib='blosc', mode=mode)

    llen = len(target_list)
    cnt = 0
    for code in target_list.code.values:
        for _ in range(retry):
            #time.sleep(pause)
            try:

                size = getcninfooneric(code, start, end, 'hq')
                if size < 1:
                    logging.info('retry hq for %s' % code)
                    reconnect()
                    continue
                else:
                    cnt;
                    #daykStore.append('day', df, min_itemsize={'name': 20, 'exchange' : 15})
            except Exception as e:
                logging.error('Error, %s, %s' % (code, e))
                reconnect()
            else:
                cnt += 1
                logging.info('get cninfo hq for %s successfully' % code + ', %d of %d' %(cnt, llen))
                break
            logging.error('failed to get hq data for %s' % code)

    cnt = 0
    for code in target_list.code.values:
        for _ in range(retry):
            # time.sleep(pause)
            try:
                size = getcninfooneric(code, start, end, 'lrb')
                if size < 1:
                    logging.info('retry lrb for %s' % code)
                    reconnect()
                    continue
                else:
                    cnt;
                    # daykStore.append('day', df, min_itemsize={'name': 20, 'exchange' : 15})
            except Exception as e:
                logging.error('Error, %s, %s' % (code, e))
                reconnect()
            else:
                cnt += 1
                logging.info('get cninfo lrb for %s successfully' % code + ', %d of %d' % (cnt, llen))
                break
            logging.error('failed to get lrb data for %s' % code)

    cnt = 0
    for code in target_list.code.values:
        for _ in range(retry):
            # time.sleep(pause)
            try:
                size = getcninfooneric(code, start, end, 'fzb')
                if size < 1:
                    logging.info('retry fzb for %s' % code)
                    reconnect()
                    continue
                else:
                    cnt;
                    # daykStore.append('day', df, min_itemsize={'name': 20, 'exchange' : 15})
            except Exception as e:
                logging.error('Error, %s, %s' % (code, e))
                reconnect()
            else:
                cnt += 1
                logging.info('get cninfo fzb for %s successfully' % code + ', %d of %d' % (cnt, llen))
                break
            logging.error('failed to get fzb data for %s' % code)

    cnt = 0
    for code in target_list.code.values:
        for _ in range(retry):
            # time.sleep(pause)
            try:
                size = getcninfooneric(code, start, end, 'llb')
                if size < 1:
                    logging.info('retry llb for %s' % code)
                    reconnect()
                    continue
                else:
                    cnt;
                    # daykStore.append('day', df, min_itemsize={'name': 20, 'exchange' : 15})
            except Exception as e:
                logging.error('Error, %s, %s' % (code, e))
                reconnect()
            else:
                cnt += 1
                logging.info('get cninfo llb for %s successfully' % code + ', %d of %d' % (cnt, llen))
                break
            logging.error('failed to get llb data for %s' % code)
    #daykStore.close()


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
    #df.code = df.code.str.encode('utf-8')
    #df.name = df.name.str.encode('utf-8')

    #get what missed in sina today all
    target_list = getcodelist()
    target_list = target_list.set_index('code')
    diff = target_list.index.difference(df.code).str#.encode('utf-8')
    missed = utility.get_realtime_all_st(diff.values)
    missed = missed[['name', 'open', 'pre_close', 'price', 'high', 'low', 'volume', 'amount', 'date', 'code']]
    missed.columns = ['name', 'open', 'prevclose', 'close', 'high', 'low', 'vol', 'amo', 'date', 'code']
    missed.amo = missed.amo.astype(np.int64)
    #missed.code = missed.code.str.encode('utf-8')
    #missed.name = missed.name.str.encode('utf-8')
    missed = missed.set_index(['code', 'date'])

    date = dt.date.today()
    df['date'] = date
    df = df.set_index(['code', 'date'])
    df = df.combine_first(missed)
    df = df.fillna(0)

    df.to_hdf('d:\\HDF5_Data\\today.hdf', 'tmp', mode='w', format='t', complib='blosc', data_columns=True)


def close2PrevClose(x):
    r = pd.Series(0, x.index)
    x.close.replace(0, inplace=True)
    r[1:] = x.iloc[:len(x) - 1].close.values
    return r.reset_index(level=0, drop=True)

def getFundmental163(retry=50, pause=1):
    target_list = getcodelist()
    llen = len(target_list)
    cnt = 0
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    get_fundmental_data_163(row.code)
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get fundmental for %s successfully' % row.code)
                    cnt += 1
                    print ('retrieved ' + row.code + ', ' + str(cnt) + ' of ' + str(llen))
                    break
            row = next(itr)
    except StopIteration as e:
        pass

def cumprod(x):
    return x.cumprod()

def addstflag():
    t1 = time.clock()
    #dayk = pd.HDFStore('d:/hdf5_data/dailydata.h5', mode='a', complib='blosc')
    #day = dayk.select('day')
    day = pd.read_hdf('d:/hdf5_data/dailydata.hdf')

    day['stflag'] = 0
    day.loc[day.name.str.contains(st_pattern), 'stflag'] = 1

    codelist = getcodelist()
    idx = day.loc[codelist[codelist.status < 0].code].groupby(level=0, group_keys=False).apply(lambda x: x[x.open>0][-5:]).index
    day.loc[idx, 'stflag'] = 1
    #dayk.put('day', day, format='t')
    day.to_hdf('d:/hdf5_data/dailydata.hdf', 'day', mode='w', format='t', data_columns=True)
    #dayk.close()
    logging.info('addstflag done' + str(time.clock() - t1))

def updateTodayDailyData():
    t1 = time.clock()

    get_today_all().sort_index().to_hdf('d:/hdf5_data/lastday.hdf', 'day', mode='w', format='t', complib='blosc', data_columns=True)

    logging.info('updateTodayDailyData done' + str(time.clock() - t1))

def calcFullRatio(daydata):
    t1 = time.clock()
    dayk = pd.HDFStore(daydata, mode='r', complib='blosc')
    brs = pd.HDFStore('d:\\HDF5_Data\\brsInfo.h5', mode='r', complib='blosc')
    df = dayk.select('day')
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
    #bi['b'] = (bi.pclose - bi.riprice) / bi.riprice
    #bi = bi[bi.b > 0.05]


    adjpclose = (bi.pclose - (bi.divpay / 10) + bi.riprice * bi.ri / 10) / (1 + (bi.give + bi.trans) / 10 + bi.ri / 10)
    adjpclose = round_series(adjpclose)
    factor = bi.pclose / adjpclose
    #factor = (bi.give + bi.trans) / 10 + bi.divpay / 10 / (bi.pclose - (bi.divpay / 10)) + bi.pclose * (1 + bi.ri / 10) / (bi.pclose + bi.riprice * bi.ri / 10)

    all = si.tradeshare / si.prevts
    all = all.combine_first(factor)
    all.to_hdf('d:\\HDF5_Data\\hfqfactor.hdf', 'factor', mode='w', format='t', complib='blosc', data_columns=True)
    combinedIndex = all.index.union(df.index)
    all = all.reindex(combinedIndex, fill_value=1)
    all = all.groupby(level=0).apply(cumprod)
    df['hfqratio'] = all
    #df.hfqratio.fillna(1, inplace=True)
    df = df.sort_index()
    #dayk.put('day', df, format='t', min_itemsize={'values': 30})
    df.to_hdf('d:/hdf5_data/dailydata.hdf', 'day', mode='w', format='t', data_columns=True)
    dayk.close()
    logging.info('callfullratio done' + str(time.clock() - t1))

def getMax10holder(code, ipodate, tradeable=True, retry=10):
    datelist = rd[ipodate:dt.datetime.today().strftime('%Y-%m-%d')]

    url=''
    rt = []
    itr = datelist.itertuples()
    try:
        row = next(itr)
        while row:
            if tradeable:
                url = 'http://quotes.money.163.com/service/gdfx.html?ltdate='+ row.end + '%2C' + row.start + '&symbol=' + code
            else:
                url = 'http://quotes.money.163.com/service/gdfx.html?date=' + row.end + '%2C' + row.start + '&symbol=' + code

            for _ in range(retry):
                try:
                    r = requests.get(url, timeout=3)
                    #print ('requesting ' + row.end + ' of ' + code)
                    #print (url)
                    df = pd.read_html(r.content.decode(), flavor='html5lib')[0]
                    df.columns = ['name', 'ratio', 'holding', 'delta']
                    df['code'] = code
                    df['date'] = row.end
                    #print (df)
                    if len(df) != 10:
                        logging.info(err)
                        break
                    df.ratio = df.ratio.replace('%', '', regex=True).astype('float') / 100
                    rt.append([code, row.end, df.ratio.sum(), df.holding.sum()])
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info(err)
                    #print (err)
                    #reconnect()
                else:
                    break

            row = next(itr)
    except StopIteration as e:
        pass

    if len(rt) > 0:
        return pd.DataFrame(rt, columns=['code', 'date', 'ratio', 'holding'])
    else:
        return pd.DataFrame()

def getHolder163():
    all = pd.read_hdf('d:\\HDF5_Data\\dailydata.hdf', 'day', columns=['open'])
    all = all.reset_index(level=1)
    all = all.groupby(level=0).apply(lambda x: x.iloc[0])
    all = all.reset_index(level=0)

    total = len(all)
    cnt = 0
    result = pd.DataFrame()
    itr = all.itertuples()
    try:
        row = next(itr)
        while row:
            df = getMax10holder(row.code, row.date.strftime('%Y-%m-%d'), True)
            cnt += 1
            logging.info('finished %s, %d of %d' % (row.code, cnt, total))
            if len(df) > 0:
                result += df
            row = next(itr)
    except StopIteration as e:
        pass

    result.to_hdf('d:/HDF5_Data/Max10TradeableHoldings.hdf', 'hold', mode='w', format='f', complib='blosc', data_columns=True)

    cnt = 0
    result = pd.DataFrame()
    itr = all.itertuples()
    try:
        row = next(itr)
        while row:
            df = getMax10holder(row.code, row.date.strftime('%Y-%m-%d'), False)
            cnt += 1
            logging.info('finished %s, %d of %d' % (row.code, cnt, total))
            if len(df) > 0:
                result += df
            row = next(itr)
    except StopIteration as e:
        pass

    result.to_hdf('d:/HDF5_Data/Max10Holdings.hdf', 'hold', mode='w', format='t', complib='blosc', data_columns=True)

def get_today_all(symbols=[], retry=10):
    t1 = time.clock()
    if len(symbols) < 1:
        riclist = getcodelist(True)
        symbols = riclist['code']
    full_df = pd.DataFrame()
    length = len(symbols)
    lenpertime = 50
    loops = int(length / lenpertime + 1)
    for idx in range(0, loops, 1):
        sublist = symbols[idx * lenpertime:(idx + 1) * lenpertime]
        url = 'http://qt.gtimg.cn/q=' + ','.join(sublist.apply(lambda x: 'sh'+ x if x.startswith('6') else 'sz'+ x))
        for _ in range(retry):
            try:
                a = pd.read_table(url, encoding='gbk', delimiter='~',
                                  names=['', 'name', 'code', 'close', 'prevclose', 'open', 'volume', 'invol', 'outvol','buy1', 'bsize1', 'buy2', 'bsize2', 'buy3', 'bsize3', 'buy4', 'bsize4', 'buy5', 'bsize5', 'sell1', 'ssize1', 'sell2', 'ssize2', 'sell3', 'ssize3', 'sell4', 'ssize4', 'sell5', 'ssize5', 'trades', 'date', 'netchng', 'pctchng', 'high', 'low', 'trade', 'vol', 'amo', 'turnoverrate', 'pe', 'a', 'h', 'l', 'm', 'tradeablecap', 'totalcap', 'pb', 'highlimit', 'lowlimit', 'b', 'c', 'd', 'e', 'f'], usecols=['name', 'code', 'close', 'high', 'low', 'open', 'prevclose', 'netchng', 'pctchng', 'turnoverrate', 'date', 'vol', 'amo', 'totalcap', 'tradeablecap'],lineterminator='"', engine='c', dtype={'name': str, 'code': str, 'close': float, 'prevclose': float, 'open': float, 'high': float, 'low': float, 'vol': float, 'amo': float, 'tradeablecap': float, 'totalcap': float, 'turnoverrate': float, 'netchng': float, 'pctchng': float}, parse_dates=['date']).dropna()
                a = a[['code', 'date', 'name', 'close', 'high', 'low', 'open', 'prevclose', 'netchng', 'pctchng', 'turnoverrate','vol', 'amo', 'totalcap', 'tradeablecap']]
            except Exception as e:
                err = 'Error %s' % e
                print('Error %s' % e)
                time.sleep(1)
            else:
                # print('get daily data for %s successfully' % row.code.encode("utf-8"))
                break
        full_df = full_df.append(a)

    full_df['date'] = full_df.date.apply(lambda x: x.date())
    full_df = full_df.set_index(['code', 'date'])
    full_df['vol'] = full_df.vol.astype(np.int64) * 100
    full_df['amo'] = full_df.amo * 10000
    full_df['totalcap'] = full_df.totalcap * 100000000
    full_df['tradeablecap'] = full_df.tradeablecap * 100000000
    full_df['stflag'] = 0
    full_df.loc[full_df.name.str.contains(st_pattern), 'stflag'] = 1
    full_df['hfqratio'] = 1.0
    logging.info('get today all done ' + str(time.clock() - t1))
    return full_df.sort_index()

def backupfile():
    copyfile('D:/hdf5_data/dailydata.h5', 'D:/hdf5_data/backup/dailydata.h5')
    copyfile('D:/hdf5_data/week.hdf', 'D:/hdf5_data/backup/week.hdf')
    copyfile('D:/hdf5_data/brsInfo.h5', 'D:/hdf5_data/backup/brsInfo.h5')
    copyfile('D:/hdf5_data/stocklist.hdf', 'D:/hdf5_data/backup/stocklist.hdf')
    copyfile('D:/hdf5_data/hfqfactor.hdf', 'D:/hdf5_data/backup/hfqfactor.hdf')


def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str)

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

    conn = pymongo.MongoClient('localhost', 27017)
    if (type == 'full'):
        backupfile()
        updatestocklist(5, 5)
        getfundamentalsinadelta()
        getadatesinaall()
        getadate163all()
        get_bonus_ri_sc()
        get_full_daily_data_163(conn['day'])
        calcFullRatio('d:\\HDF5_Data\\dailydata.h5')
        addstflag()
    elif (type == 'fundamental'):
        csvtohdf('sina', 'cwzb')
        processfundamental()
    elif (type == 'delta'):
        #backupfile()
        #updatestocklist(5, 5)
        #getfundamentalsinadelta()
        #getadatesinaall()
        #getadate163all()
        #get_bonus_ri_sc()
        get_delta_daily_data_163()
        checkandappendaily()
        #calcFullRatio('d:\\HDF5_Data\\dailydata.h5')
    elif (type == 'sinafull'):
        updatestocklist(5, 5)
        get_full_daily_data_sina()
    elif (type == 'sinadelta'):
        #updatestocklist(5, 5)
        get_delta_daily_data_sina()
    elif (type == 'index'):
        updateindexlist()
        get_all_full_index_daily()
    elif (type == 'cninfofull'):
        updatestocklist(5, 5)
        getcninfodaily('full')
    elif (type == '10maxhold'):
        getHolder163()
    elif (type == 'updatetoday'):
        updateTodayDailyData()
    elif (type == 'test'):
        get_full_daily_data_163(conn)

    conn.close()




