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



g_flag = 0

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

def request_history_tick(code, datelist):
    logging.info('start requesting tick, code: ' + code)

    df = pd.DataFrame()

    for cur_day in datelist:
        succeeded = False
        retry = 0
        try:
            while (succeeded == False) and (retry < 10):
                tick = ts.get_tick_data(code, date=cur_day.date(), retry_count=10)
                if not tick.empty:
                    if tick.time[0] != 'alert("当天没有数据");':
                        tick['type'] = tick['type'].apply(lambda x: trade_type_dic[x])
                        tick['change'] = tick['change'].apply(change_dic)
                        tick['code'] = code
                        tick['date'] = cur_day
                        #tick = tick.set_index(['code', 'date'])
                        tick = tick.sort_values('time')
                        tick.time = pd.to_timedelta(tick.time)
                        tick.change = tick.change.astype(float)
                        df = df.append(tick)
                        #tick['time'] = str(cur_day.date()) + ' '+ tick['time']
                        #tick.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc', append=True)
                        #logging.info('save to tick_tbl_' + code + ' on '+ str(cur_day) + ' thread ' + str(threading.currentThread()))
                succeeded = True

        except Exception:
            retry += 1
            logging.error(str(code) + ' request tick retry ' + str(retry) + ' on ' + str(cur_day))

    logging.info('finished request tick, code: ' + code)
    if not df.empty:
        df = df.set_index(['code', 'date'])
        df.sort_index()
    return df


def IO(codelist, q1, q2):
    a = codelist.index.drop_duplicates()
    requestcnt = 0
    for code in a.values:
        datelist = codelist.loc[code:code].date
        if len(datelist) < 1:
            continue

        if os.path.exists('D:\\HDF5_Data\\tick\\tick_tbl_' + code):
            tmp = pd.read_hdf('D:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', start=-1)
            if not tmp.empty:
                lastday = tmp.reset_index(level=1).date[-1].date()
                datelist = datelist[datelist > lastday]
        if len(datelist) < 1:
            continue

        logging.info('finish get datelist, code: ' + code)
        s = [code, datelist]
        q1.put(s)
        requestcnt += 1

        while not q2.empty():
            df = q2.get()
            requestcnt = requestcnt - 1
            code = df.index.get_level_values(0)[0]
            if len(df) < 100:
                logging.warning('len of df less than 100, code: ' + code)
            df.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc', append=True)
            logging.info('finished save tick, code: ' + code)

    while requestcnt > 0:
        if not q2.empty():
            df = q2.get()
            requestcnt = requestcnt - 1
            code = df.index.get_level_values(0)[0]
            if len(df) < 100:
                logging.warning('len of df less than 100, code: ' + code)
            df.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc', append=True)
            logging.info('finished save tick, code: ' + code)
            print requestcnt

    global g_flag
    g_flag += 2

def requesttick(q1, q2):
    while True:
        if not q1.empty():
            s = q1.get()
            df = request_history_tick(s[0], s[1])
            if not df.empty:
                q2.put(df)
            else:
                logging.info('empty tick, code: ' + s[0])

        else:
            if g_flag >= 1:
                return

if __name__=="__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/updateTickData.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    logging.info('finished save tick, code: ' + code)
    all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'dayk', columns=['open'], where='date > \'2016-6-1\'')
    all = all[all.open > 0]
    all = all.reset_index(level=1)
    backbone1 = Queue.Queue()
    backbone2 = Queue.Queue()
    threads = []
    t1 = threading.Thread(target=IO, args=(all, backbone1, backbone2))
    threads.append(t1)

    t2 = threading.Thread(target=requesttick, args=(backbone1, backbone2))
    threads.append(t2)
    t3 = threading.Thread(target=requesttick, args=(backbone1, backbone2))
    threads.append(t3)
    t4 = threading.Thread(target=requesttick, args=(backbone1, backbone2))
    threads.append(t4)
    t5 = threading.Thread(target=requesttick, args=(backbone1, backbone2))
    threads.append(t5)
    t6 = threading.Thread(target=requesttick, args=(backbone1, backbone2))
    threads.append(t6)

    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()












