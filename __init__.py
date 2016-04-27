# -*- coding:utf-8 -*-

import downloader
import threading
import dataloader

def download_tick(start_code, end_code):
    target_list = dataloader.get_code_list(str(start_code), str(end_code))
    itr = target_list.itertuples()
    row = next(itr)
    while row:
        downloader.request_history_tick(row.code, start_date='2005-01-01', end_date='2016-03-31')
        row = next(itr)

threads = []
t1 = threading.Thread(target=download_tick, args=('000023','002133'))
threads.append(t1)
t2 = threading.Thread(target=download_tick, args=('002155','002737'))
threads.append(t2)
t3 = threading.Thread(target=download_tick, args=('002789','600060'))
threads.append(t3)
t4 = threading.Thread(target=download_tick, args=('600081','600764'))
threads.append(t4)
t5 = threading.Thread(target=download_tick, args=('600787','604000'))
threads.append(t5)
print

if __name__=="__main__":
    file_object = open('.\\conf\\sse_a.txt')
    list_of_all_the_lines = file_object.readlines()
    file_object.close()

    #downloader.create_dayk_talbe()

    #for line in list_of_all_the_lines:
    #    print "requesting " + line.strip('\n') + "..."
    #    downloader.request_instrument("dayk_qfq", line.strip('\n'))

    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()











