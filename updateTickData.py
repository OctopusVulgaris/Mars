# -*- coding:utf-8 -*-

import mydownloader
import threading
import dataloader
import datetime
import sqlalchemy as sa
import Queue
import time
import logging
import pandas as pd
import sys
import os


engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
g_flag = 0

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
            df.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc')
            logging.info('finished save tick, code: ' + code)

    while requestcnt > 0:
        if not q2.empty():
            df = q2.get()
            requestcnt = requestcnt - 1
            code = df.index.get_level_values(0)[0]
            if len(df) < 100:
                logging.warning('len of df less than 100, code: ' + code)
            df.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc')
            logging.info('finished save tick, code: ' + code)

    global g_flag
    g_flag += 1

def requesttick(q1, q2):
    while True:
        if not q1.empty():
            s = q1.get()
            df = mydownloader.request_history_tick(s[0], s[1])
            if not df.empty:
                q2.put(df)
            else:
                logging.info('empty tick, code: ' + s[0])

        else:
            if g_flag >= 1:
                return

if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='updateTickData.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'dayk', columns=['open'], where='date > \'2016-1-1\'')
    all = all[all.open > 0]
    all = all.reset_index(level=1)
    backbone1 = Queue.Queue()
    backbone2 = Queue.Queue()
    threads = []
    t1 = threading.Thread(target=IO, args=(all, backbone1, backbone2))
    threads.append(t1)

    # t2 = threading.Thread(target=download_tick, args=(all.loc['002579':'002580'], backbone))
    # threads.append(t2)
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












