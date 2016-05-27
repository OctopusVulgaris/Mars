# -*- coding:utf-8 -*-

import mydownloader
import threading
import dataloader
import sqlalchemy as sa
import pandas as pd

engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=False)

def thread_func(start_code, end_code):

    target_list = dataloader.get_code_list(str(start_code), str(end_code), engine)
    itr = target_list.itertuples()
    row = next(itr)
    while row:
        mydownloader.request_dayk('dayk', row.code, engine, start_date='1990-01-01', end_date='2016-03-30')
        row = next(itr)

threads = []
t1 = threading.Thread(target=thread_func, args=('000001','002133'))
threads.append(t1)
t2 = threading.Thread(target=thread_func, args=('002134','002737'))
threads.append(t2)
t3 = threading.Thread(target=thread_func, args=('002738','600060'))
threads.append(t3)
t4 = threading.Thread(target=thread_func, args=('600061','600764'))
threads.append(t4)
t5 = threading.Thread(target=thread_func, args=('600765','604000'))
threads.append(t5)
print

if __name__ == "__main__":
    mydownloader.create_dayk_talbe()

    for t in threads:
        t.setDaemon(True)
        t.start()












