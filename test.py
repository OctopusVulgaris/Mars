# -*- coding:utf-8 -*-
import pandas as pd
import Queue
import time
import logging
import sys
import threading

backbone1 = Queue.Queue()
backbone2 = Queue.Queue()
g_flag = 0

def read(q, codelist):
    for code in codelist:
        onedaytick = pd.read_hdf('d:\\HDF5_Data\\tick\\tick_tbl_'+code)

        if onedaytick.empty:
            continue
        q.put(onedaytick)


def setopen(q1, q2, all):
    while True:
        if not q1.empty():
            df = q1.get()
            df['open'] = all.open
            q2.put(df)

def save(q):
    while True:
        if not q.empty():
            df = q.get()
            r = pd.DataFrame()
            r['upperamo'] = df[df.price >= df.open].groupby(level=[0, 1]).amount.sum()
            r['loweramo'] = df[df.price <= df.open].groupby(level=[0, 1]).amount.sum()
            r.to_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', format='t', append=True, complib='blosc', mode='a')
            #logging.info('finished save tick, code: ' + code)
        else:
            time.sleep(1)
            if g_flag >= 6:
                return


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='opensplit.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    all = pd.read_hdf('d:\\HDF5_Data\\buylow_sellhigh_tmp.hdf', 'day', columns=['open'], where='date > \'2006-1-1\'')
    all = all[all.open > 0]
    codelist = all.index.get_level_values(0).to_series().get_values()

    threads = []
    t1 = threading.Thread(target=read, args=(backbone1, codelist))

    t2 = threading.Thread(target=setopen, args=(backbone1, backbone2, all))
    t3 = threading.Thread(target=setopen, args=(backbone1, backbone2, all))
    t4 = threading.Thread(target=setopen, args=(backbone1, backbone2, all))
    t5 = threading.Thread(target=setopen, args=(backbone1, backbone2, all))

    t6 = threading.Thread(target=save, args=(backbone2,))
    threads.append(t1)
    threads.append(t2)
    threads.append(t3)
    threads.append(t4)
    threads.append(t5)
    threads.append(t6)

    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()