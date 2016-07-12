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

backbone = Queue.Queue()
engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
g_flag = 0

def download_tick(codelist, q):
    a = codelist.index.drop_duplicates()

    for code in a.values:
        df = mydownloader.request_history_tick(code, codelist.loc[code].date)
        if not df.empty:
            q.put(df)

    global g_flag
    g_flag += 1

def serialize(q):
    while True:
        if not q.empty():
            df = q.get()
            code = df.index.get_level_values(0)[1]
            df.to_hdf('d:\\HDF5_Data\\tick\\tick_tbl_' + code, 'tick', mode='a', format='t', complib='blosc')
            logging.info('finished save tick, code: ' + code)
            print 'finished save tick, code: ' + code
        else:
            time.sleep(60)
            if g_flag >= 6:
                return

all = pd.read_hdf('d:\\HDF5_Data\\dailydata.h5', 'dayk', columns=['open'], where='date > \'2006-1-1\'')
all = all[all.open > 0]
all = all.reset_index(level=1)

threads = []
t1 = threading.Thread(target=download_tick, args=(all.loc['000001':'000990'], backbone))
threads.append(t1)
t2 = threading.Thread(target=download_tick, args=(all.loc['000991':'002593'], backbone))
threads.append(t2)
t3 = threading.Thread(target=download_tick, args=(all.loc['002594':'300290'], backbone))
threads.append(t3)
t4 = threading.Thread(target=download_tick, args=(all.loc['300293':'600198'], backbone))
threads.append(t4)
t5 = threading.Thread(target=download_tick, args=(all.loc['600199':'600663'], backbone))
threads.append(t5)
t6 = threading.Thread(target=download_tick, args=(all.loc['600664':'603999'], backbone))
threads.append(t6)


t12 = threading.Thread(target=serialize, args=(backbone,))
threads.append(t12)


if __name__=="__main__":

    #downloader.update_stock_basics()
    #downloader.create_dayk_talbe()

    #for line in list_of_all_the_lines:
    #    print "requesting " + line.strip('\n') + "..."
    #    downloader.request_instrument("dayk_qfq", line.strip('\n'))

    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()












