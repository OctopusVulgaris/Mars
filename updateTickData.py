# -*- coding:utf-8 -*-

import mydownloader
import threading
import dataloader
import datetime
import sqlalchemy as sa

engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

def download_tick(start_code, end_code):
    target_list = dataloader.get_code_list(start_code, end_code, engine)
    itr = target_list.itertuples()
    try:
        row = next(itr)
        while row:
            mydownloader.request_history_tick(row.code, engine, start_date=datetime.datetime(2005,01,01), end_date=datetime.datetime(2016,03,31))
            row = next(itr)
    except StopIteration:
        pass

threads = []
t1 = threading.Thread(target=download_tick, args=('000913','000980'))
threads.append(t1)
t2 = threading.Thread(target=download_tick, args=('002733','002737'))
threads.append(t2)
t3 = threading.Thread(target=download_tick, args=('600546','600608'))
threads.append(t3)
t4 = threading.Thread(target=download_tick, args=('002077','002133'))
threads.append(t4)
t5 = threading.Thread(target=download_tick, args=('600714','600764'))
threads.append(t5)
print

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











