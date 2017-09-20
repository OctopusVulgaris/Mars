# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import queue
import time
import logging
import sys
import threading
import os
import multiprocessing as mp

backbone1 = mp.Queue()
backbone2 = mp.Queue()
g_flag = 0

def getlastdate(code):
    lastdate = '1990-01-01'
    if os.path.exists('d:\\HDF5_Data\\OpenSplitAmount.hdf'):
        tmp = pd.read_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', where='code == \'' + code + '\'')
        if not tmp.empty:
            lastdate = str(tmp.iloc[-1].name[1].date())

    return lastdate

def readandsave(q1, q2, codelist):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/opensplit.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    savecnt = 0
    try:
        for code in codelist:
            if not os.path.exists('d:\\HDF5_Data\\tick\\tick_tbl_'+code):
                continue
            onedaytick = pd.read_hdf('d:\\HDF5_Data\\tick\\tick_tbl_'+code, where='date > \'' + getlastdate(code) + '\'')
            onedaytick = onedaytick[onedaytick.time > '09:26:00']
            logging.info('finished read tick, code: ' + code)
            if onedaytick.empty:
                continue
            q1.put(onedaytick)
            savecnt += 1

            while not q2.empty():
                r = q2.get()
                r.to_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', format='t', append=True, complib='blosc', mode='a')

                savecnt = savecnt - 1
                logging.info('finished save tick, savecnt:' + str(savecnt))

        while savecnt > 0:
            if not q2.empty():
                r = q2.get()
                r.to_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', format='t', append=True, complib='blosc', mode='a')

                savecnt = savecnt - 1
                logging.info('finished save tick, savecnt:' + str(savecnt))
    except Exception as e:
        err = 'Error %s' % e
        logging.info('Error %s' % e)
    global g_flag
    g_flag += 2


def setopen(q1, q2, all):
    while True:
        if not q1.empty():
            df = q1.get()
            df['open'] = all.loc[df.index.drop_duplicates()].open

            r = pd.DataFrame()
            r['upperamo'] = df[df.price >= df.open].groupby(level=[0, 1]).amount.sum()
            r['loweramo'] = df[df.price <= df.open].groupby(level=[0, 1]).amount.sum()
            r = r.fillna(0)
            r['loweramo'] = r['loweramo'].astype(np.int64)
            r['upperamo'] = r['upperamo'].astype(np.int64)
            q2.put(r)
        else:
            time.sleep(1)
            if g_flag >=1:
                return


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/opensplit.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'dayk', columns=['open'], where='date > \'2016-9-1\'')
    all = all[all.open > 0]
    all = all.sort_index()
    codelist = all.index.get_level_values(0).to_series().drop_duplicates().get_values()
    logging.info('codelist len: ' + str(len(codelist)))

    processes = []
    t1 = mp.Process(target=readandsave, args=(backbone1, backbone2, codelist))

    for i in range(5):
        t = mp.Process(target=setopen, args=(backbone1, backbone2, all))
        t.daemon = True
        t.start()
    #t.join()
    t1.start()
    t1.join()

    osa = pd.read_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf')
    osa = osa.sort_index()
    osa.to_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', format='t', complib='blosc', mode='w')