# -*- coding:utf-8 -*-

import pandas as pd
import json
import re
import tushare as ts
import dataloader
import threading
import time
from dataloader import engine
from urllib2 import urlopen, Request

full_df = pd.DataFrame()
mutex = threading.Lock()

def get_today_all():
    text = urlopen('http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=8000&sort=mktcap&asc=0&node=hs_a&symbol=&_s_r_a=page&page=0').read()
    if text == 'null':
        return None
    reg = re.compile(r'\,(.*?)\:')
    text = reg.sub(r',"\1":', text.decode('gbk'))
    text = text.replace('"{symbol', '{"symbol')
    text = text.replace('{symbol', '{"symbol"')
    jstr = json.dumps(text, encoding='GBK')
    js = json.loads(jstr)
    return pd.DataFrame(pd.read_json(js, dtype={'code': object}))

def gop_worker(symbols, idx, retry=60):
    #length = len(symbols)
    #if(idx*200 > length):
    #    return
    sublist = symbols[idx*300:(idx+1)*300]
    for _ in range(retry):
        try:
            df = ts.get_realtime_quotes(sublist)
        except Exception as e:
            err = 'Error %s' % e
            # print('Error %s' % e)
            time.sleep(1)
        else:
            # print('get daily data for %s successfully' % row.code.encode("utf-8"))
            break
    global full_df
    if mutex.acquire(60):
        full_df = full_df.append(df)
        mutex.release()

def get_realtime_all():
    start = time.time()
    riclist = dataloader.get_code_list('', '', engine)
    length = len(riclist)
    symbols =  riclist['code']
    threads = []
    totalthreads = length / 300 + 1
    for i in range(0, totalthreads, 1):
        t = threading.Thread(target=gop_worker, args=(symbols, i,))
        threads.append(t)

    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()
    finish = time.time()
    print finish - start
    full_df[['name','open','pre_close','price','date','code']] = full_df[['date','code','open','pre_close','price','name']]
    names = full_df.columns.tolist()
    names[names.index('name')] = 'Date'
    names[names.index('open')] = 'Code'
    names[names.index('pre_close')] = 'Open'
    names[names.index('price')] = 'Pre_close'
    names[names.index('date')] = 'Price'
    names[names.index('code')] = 'Name'
    full_df.columns = names
    #full_df.to_csv('d:\\ut.csv',encoding='utf-8',index=False)
    return full_df

def get_realtime_all_st(retry=60):
    start = time.time()
    riclist = dataloader.get_code_list('', '', engine)
    full_df = pd.DataFrame()
    length = len(riclist)
    symbols =  riclist['code']
    threads = []
    loops = length / 300 + 1
    for idx in range(0,loops,1):
        sublist = symbols[idx * 300:(idx + 1) * 300]
        for _ in range(retry):
            try:
                df = ts.get_realtime_quotes(sublist)
            except Exception as e:
                err = 'Error %s' % e
                print('Error %s' % e)
                time.sleep(1)
            else:
                # print('get daily data for %s successfully' % row.code.encode("utf-8"))
                break
        full_df = full_df.append(df)

    full_df[['name','open','pre_close','price','date','code']] = full_df[['date','code','open','pre_close','price','name']]
    names = full_df.columns.tolist()
    names[names.index('name')] = 'date'
    names[names.index('open')] = 'code'
    names[names.index('pre_close')] = 'open'
    names[names.index('price')] = 'pre_close'
    names[names.index('date')] = 'price'
    names[names.index('code')] = 'name'
    full_df.columns = names
    #full_df.to_csv('d:\\ut.csv',encoding='utf-8',index=False)
    finish = time.time()
    print finish - start
    return full_df


if __name__=="__main__":
    df = get_realtime_all_st()
    df.to_csv('d:\\ut.csv', encoding='utf-8', index=False)
    #df = get_today_all()
    #df.to_csv('d:\\ut_all.csv',encoding='utf-8',index=False)