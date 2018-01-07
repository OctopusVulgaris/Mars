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



if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/opensplit.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'day', columns=['open'], where='date > \'2006-1-1\'')
    all = all[all.open > 0]
    all = all.sort_index()
    codelist = all.index.get_level_values(0).to_series().drop_duplicates().get_values()
    #logging.info('codelist len: ' + str(len(codelist)))

    total = len(codelist)
    savecnt = 0
    try:
        for code in codelist:
            savecnt += 1
            if not os.path.exists('d:\\HDF5_Data\\tick\\tick_tbl_' + code):
                continue
            tick = pd.read_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code,
                                     where='date > \'' + getlastdate(code) + '\'')
            tick = tick[tick.time > '09:26:00']
            logging.info('finished read tick, code: ' + code)
            if tick.empty:
                continue

            tick['open'] = all.loc[tick.index.drop_duplicates()].open

            r = pd.DataFrame()
            r['upperamo'] = tick[tick.price >= tick.open].groupby(level=[0, 1]).amount.sum()
            r['loweramo'] = tick[tick.price <= tick.open].groupby(level=[0, 1]).amount.sum()
            r = r.fillna(0)
            r['loweramo'] = r['loweramo'].astype(np.int64)
            r['upperamo'] = r['upperamo'].astype(np.int64)

            r.to_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', format='t', append=True, complib='blosc', mode='a')

            logging.info('finished save tick, %d of %d' % (savecnt, total))

    except Exception as e:
        err = 'Error %s' % e
        logging.info('Error %s' % e)

    osa = pd.read_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf')
    osa = osa.sort_index()
    osa.to_hdf('d:\\HDF5_Data\\OpenSplitAmount.hdf', 'day', format='t', complib='blosc', mode='w')