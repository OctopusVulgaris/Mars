# -*- coding:utf-8 -*-

import threading
import datetime
import tushare as ts
import queue
import time
import logging
import pandas as pd
import sys
import os
import multiprocessing as mp
from utility import reconnect


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
    path = 'D:\\HDF5_Data\\ticksn\\' + code

    for cur_day in datelist:
        succeeded = False
        retry = 0

        while (succeeded == False) and (retry < 10):
            try:
                tick = ts.get_tick_data(code, date=str(cur_day.date()), retry_count=10, src='tt')
                if tick is not None and not tick.empty:
                    if tick.time[0] != 'alert("当天没有数据");':
                        tick['type'] = tick['type'].apply(lambda x: trade_type_dic[x])
                        tick['change'] = tick['change'].apply(change_dic)
                        tick.change = tick.change.astype(float)
                        daypath = path + '\\' + str(cur_day.date()) + '.csv'
                        tick.to_csv(daypath, index=False)

                        tick['code'] = code
                        tick['date'] = cur_day
                        #tick = tick.set_index(['code', 'date'])
                        tick = tick.sort_values('time')
                        tick.time = pd.to_timedelta(tick.time)

                        df = df.append(tick)
                succeeded = True

            except Exception as e:
                retry += 1
                reconnect()
                logging.error(str(code) + ' request tick; retry ' + str(retry) + ' on ' + str(cur_day.date()) + '%s' % e)

    #logging.info('finished request tick, code: ' + code)
    if not df.empty:
        df = df.set_index(['code', 'date'])
        #df.sort_index()
    return df


def IO(codelist, q1, q2):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/updateTickData.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    a = codelist.index.drop_duplicates()
    requestcnt = 0
    try:

        for code in a.values:
            print(code)
            path = 'D:\\HDF5_Data\\ticksn\\' + code
            if not os.path.exists(path):
                os.makedirs(path)

            datelist = codelist.loc[code:code].date
            if len(datelist) < 1:
                continue

            cachelist = os.listdir(path)
            if len(cachelist) > 0:
                datelist = datelist[datelist > cachelist[-1].rstrip('.csv')]

            if len(datelist) < 1:
                continue
            '''
            if os.path.exists('D:\\HDF5_Data\\tick\\tick_tbl_' + code):
                tmp = pd.read_hdf('D:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', start=-1)
                if not tmp.empty:
                    lastday = tmp.reset_index(level=1).date[-1].date()
                    datelist = datelist[datelist > lastday]
            
            if len(datelist) < 1:
                continue
            '''

            logging.info('finish get datelist, code: ' + code)
            s = [code, datelist]
            q1.put(s)
            requestcnt += 1

            while not q2.empty():
                df = q2.get()
                requestcnt = requestcnt - 1
                if not df.empty:
                    code = df.index.get_level_values(0)[0]
                    if len(df) < 100:
                        logging.warning('len of df less than 100, code: ' + code)
                    df.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc', append=True)
                    logging.info('finished save tick, code: ' + code + ', requestcnt: ' + str(requestcnt))

        while requestcnt > 0:
            if not q2.empty():
                df = q2.get()
                requestcnt = requestcnt - 1
                if not df.empty:
                    code = df.index.get_level_values(0)[0]
                    if len(df) < 100:
                        logging.warning('len of df less than 100, code: ' + code + ', len ' + str(len(df)))
                    df.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc', append=True)
                    logging.info('finished save tick, code: ' + code + ', requestcnt: ' + str(requestcnt))
                    print (requestcnt)
            else:
                time.sleep(10)
    except Exception as e:
        err = 'Error %s' % e
        logging.info('Error %s' % e)
    global g_flag
    g_flag += 2
    logging.info('finish io. ' + str(g_flag))

def requesttick(q1, q2):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/updateTickData.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)
    while True:
        try:
            if not q1.empty():

                s = q1.get()

                df = request_history_tick(s[0], s[1])
                if df.empty:
                    logging.info('empty tick, code: ' + s[0])
                q2.put(df)

            else:
                if g_flag >= 1:
                    logging.info('finish request. ' + str(g_flag) + ' ' + str(threading.currentThread()))
                    return
                else:
                    time.sleep(10)
        except Exception as e:
            logging.info('exception: ' + str(e) + ' ' + str(threading.currentThread()))


if __name__=="__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/updateTickData.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    mp.set_start_method('spawn')


    all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'dayk', columns=['open'], where='date > \'2016-9-2\'')
    all = all[all.open > 0]
    all = all.reset_index(level=1)
    a = all.index.drop_duplicates()

    logging.info('finish read. ')
    backbone1 = mp.Queue()
    backbone2 = mp.Queue()
    processes = []
    t1 = mp.Process(target=IO, args=(all, backbone1, backbone2,))
    #threads.append(t1)


    for i in range(1):
        t = mp.Process(target=requesttick, args=(backbone1, backbone2,))
        t.daemon = True
        t.start()
        processes.append(t)

    t1.start()
    t1.join()

    logging.info('all done. ')











