# -*- coding:utf-8 -*-




import pandas as pd
import tushare as ts
import numpy as np
import json
import re
import requests
from lxml import etree
from StringIO import StringIO

import argparse
import utility
import datetime
import time
import logging
import threading

from dataloader import engine, get_code_list


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='testAnt.log'
                    )
hdfStore = pd.HDFStore('D:\\HDF5_Data\\brsInfo.h5', complib='blosc', mode='w')

def get_bonus_and_ri(code, timeout=5):
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
            logging.info('Info %s "没有数据" bonus' % code)
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
            logging.info('Info %s "没有数据" rightsissue' % code)
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
        hdfStore.append('bonus', df, min_itemsize={'values': 50})
        logging.info('Info %s has saved bonus' % code)
        #df.to_hdf('d:\\HDF5_Data\\binfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty bonus' % code)
        logging.info('Info len bitems %d' % len(bitems))

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
        hdfStore.append('rightsissue', df1, min_itemsize = {'values': 50})
        #df.to_hdf('d:\\HDF5_Data\\rinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty righsissue' % code)
        logging.info('Info len ritems %d' % len(ritems))


def is_digit_or_point(c):
    if(str.isdigit(c)):
        return True
    elif(c == '.'):
        return True
    else:
        return False

def get_stock_change(code, timeout=5):
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
        hdfStore.append('stockchange', df, min_itemsize={'values': 50})
        #df.to_hdf('d:\\HDF5_Data\\sinfo.hdf', 'day', mode='a', format='t', complib='blosc', append=True)
    else:
        logging.info('Info %s has empty stock change' % code)
        logging.info('Info len sitems %d' % len(tables))

def get_bonus_ri_sc(retry=50, pause=1):
    target_list = get_code_list('', '', engine)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            for _ in range(retry):
                try:
                    print 'retrieving bonus_and_ri' + row.code.encode("utf-8")
                    get_bonus_and_ri(row.code.encode("utf-8"))
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s bonus_and_ri data for %s successfully' % row.code.encode("utf-8"))
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
                    print 'retrieving stock change' + row.code.encode("utf-8")
                    get_stock_change(row.code.encode("utf-8"))
                    pass
                except Exception as e:
                    err = 'Error %s' % e
                    logging.info('Error %s' % e)
                    time.sleep(pause)
                else:
                    logging.info('get today\'s stock change data for %s successfully' % row.code.encode("utf-8"))
                    break
            row = next(itr)
    except StopIteration as e:
        pass


def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-t', type=str, choices=['full', 'delta','bonus','bnd'], default='bonus', help='download type')

    args=parse.parse_args()
    return vars(args)

if __name__=="__main__":
    args = getArgs()
    type = args['t']
    get_bonus_and_ri('300208')
    if (type == 'bonus'):
        get_bonus_ri_sc()




