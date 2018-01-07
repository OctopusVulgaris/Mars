# -*- coding:utf-8 -*-

import threading

import datetime
import tushare as ts
import Queue
import time
import logging
import pandas as pd
import sys
import os
from testAnt import getcodelist



g_flag = 0



def request_history_dayk(code, startdate):
    logging.info('start requesting dayk, code: ' + code)

    df = pd.DataFrame()
    retry = 50
    pause = 1
    for _ in range(retry):
        try:
            df = ts.get_h_data(code, start=startdate, autype=None, drop_factor=False)
            if df.empty:
                logging.info('Error, empty dayk for code: ' % str(code))
            else:
                df['hfqratio'] = 1.0
                df['code'] = code
                df = df.set_index(['code', df.index])
        except Exception as e:
            err = 'Error %s' % e
            logging.info('Error %s' % e)
            time.sleep(pause)
        else:
            logging.info('get daily data for %s successfully' % code)
            break
    return df

def IO(q1, q2):
    mode = 'a'
    path = 'D:/HDF5_Data/dailydata_sina.h5'
    if not os.path.exists(path):
        mode = 'w'

    daykStore = pd.HDFStore(path, complib='blosc', mode=mode)

    target_list = getcodelist()
    cnt = 0

    for code in target_list.code.values:
        startdate = '1997-01-01'
        if mode == 'a':
            df = daykStore.select('day', where='code==\'%s\'' % (code))
            if not df.empty:
                startdate = df.index.get_level_values(1)[-1].strftime('%Y-%m-%d')

        q1.put([code, startdate])
        cnt = cnt + 1

        while not q2.empty():
            df = q2.get()
            cnt = cnt - 1
            if not df.empty:
                c = df.index.get_level_values(0)[0]
                daykStore.append('day', df)
                logging.info('finished save dayk, code: ' + c)

    while cnt > 0:
        if not q2.empty():
            df = q2.get()
            cnt = cnt - 1
            if not df.empty:
                c = df.index.get_level_values(0)[0]
                daykStore.append('day', df)
                logging.info('finished save dayk, code: ' + c)

    daykStore.close()
    global g_flag
    g_flag += 2
    logging.info('finish io. ')

def requester(q1, q2):
    while True:
        if not q1.empty():
            s = q1.get()
            df = request_history_dayk(s[0], s[1])
            q2.put(df)

        else:
            if g_flag >= 1:
                logging.info('finish request. '+ str(threading.currentThread()))
                return

if __name__=="__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/updateDayKdata.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)


    backbone1 = Queue.Queue()
    backbone2 = Queue.Queue()
    threads = []
    t1 = threading.Thread(target=IO, args=(backbone1, backbone2))
    threads.append(t1)

    t2 = threading.Thread(target=requester, args=(backbone1, backbone2))
    threads.append(t2)
    t3 = threading.Thread(target=requester, args=(backbone1, backbone2))
    threads.append(t3)
    t4 = threading.Thread(target=requester, args=(backbone1, backbone2))
    threads.append(t4)
    t5 = threading.Thread(target=requester, args=(backbone1, backbone2))
    threads.append(t5)
    t6 = threading.Thread(target=requester, args=(backbone1, backbone2))
    threads.append(t6)
    t7 = threading.Thread(target=requester, args=(backbone1, backbone2))
    threads.append(t7)

    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()

    logging.info('all done. ')











