# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import queue
import time
import logging
import sys
import threading
import os

backbone1 = queue.Queue()
backbone2 = queue.Queue()
g_flag = 0

def getlastdate(code):
    lastdate = '1990-01-01'
    if os.path.exists('d:\\HDF5_Data\\filteredtick.hdf'):
        tmp = pd.read_hdf('d:\\HDF5_Data\\filteredtick.hdf', 'day', where='code == \'' + code + '\'')
        if not tmp.empty:
            lastdate = str(tmp.iloc[-1].name[1].date())

    return lastdate


def readandsave(q1, q2, codelist):
    savecnt = 0

    for code in codelist:
        ticktablename = 'd:\\HDF5_Data\\tick\\tick_tbl_'+code
        if os.path.exists(ticktablename):
            onedaytick = pd.read_hdf(ticktablename, where='date > \'' + getlastdate(code) + '\'')

            logging.info('finished read tick, code: ' + code)
            if onedaytick.empty:
                continue
            q1.put(onedaytick)
            savecnt += 1

        while not q2.empty():
            r = q2.get()
            r.to_hdf('d:\\HDF5_Data\\filteredtick.hdf', 'day', format='t', append=True, complib='blosc', mode='a')
            logging.info('finished save tick, code: ' + r.index.get_level_values(0)[0])
            savecnt = savecnt - 1

    while savecnt > 0:
        if not q2.empty():
            r = q2.get()
            r.to_hdf('d:\\HDF5_Data\\filteredtick.hdf', 'day', format='t', append=True, complib='blosc', mode='a')
            savecnt = savecnt - 1
            logging.info('finished save tick, code: ' + r.index.get_level_values(0)[0] + ', savecnt:' + str(savecnt))


    global g_flag
    g_flag += 2


def filter(q1, q2):
    while True:
        if not q1.empty():
            x = q1.get()

            g = x[x.amount <= 40000].groupby(level=1)
            s = pd.DataFrame()
            s['small'] = g.amount.count()
            s['small_amo'] = g.amount.sum()

            g = x[(x.amount <= 200000) & (x.amount > 40000)].groupby(level=1)
            m = pd.DataFrame()
            m['medium'] = g.amount.count()
            m['medium_amo'] = g.amount.sum()

            g = x[(x.amount <= 1000000) & (x.amount > 200000)].groupby(level=1)
            l = pd.DataFrame()
            l['large'] = g.amount.count()
            l['large_amo'] = g.amount.sum()

            g = x[x.amount > 1000000].groupby(level=1)
            h = pd.DataFrame()
            h['huge'] = g.amount.count()
            h['huge_amo'] = g.amount.sum()

            r = pd.concat([s, m, l, h], axis=1)
            r['code'] = x.index[0][0]
            r = r.set_index(['code', r.index])
            r['icode'] = r.index.get_level_values(0).to_series().apply(lambda x: np.int64(x)).values
            r['idate'] = r.index.get_level_values(1).to_series().apply(lambda x: np.int64(time.mktime(x.timetuple()))).values
            r = r.fillna(value=0, downcast='infer')
            q2.put(r)
        else:
            time.sleep(1)
            if g_flag >= 1:
                return


if __name__=="__main__":

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='d:/tradelog/tickfilter.log'
                        )
    log = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stdout_handler)

    all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'day', columns=['open'], where='date > \'2006-1-1\'')
    all = all[all.open > 0]
    all = all.sort_index()
    codelist = all.index.get_level_values(0).to_series().drop_duplicates().get_values()
    logging.info('codelist len: ' + str(len(codelist)))

    threads = []
    t1 = threading.Thread(target=readandsave, args=(backbone1, backbone2, codelist))

    t2 = threading.Thread(target=filter, args=(backbone1, backbone2))
    #t3 = threading.Thread(target=filter, args=(backbone1, backbone2, all))
    #t4 = threading.Thread(target=filter, args=(backbone1, backbone2, all))


    threads.append(t1)
    threads.append(t2)
    #threads.append(t3)
    #threads.append(t4)
    #threads.append(t5)

    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()


