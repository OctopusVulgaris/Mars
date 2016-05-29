# -*- coding:utf-8 -*-
import pandas as pd
import sqlalchemy as sa
import datetime
import logging
import threading
import dataloader

logging.basicConfig(filename='test_log.txt', level=logging.DEBUG)
from sqlalchemy import Date, text, DateTime, Integer

outstanding = 4543607000

def to_date(x):
    return  x.date()




#code = '600061'

def test(x):
    open = x.ix[-1].price

    if open < 0.01:
        logging.info('invalid open: ' + str(x.ix[-1]) )
    aa = x[x.price >= open].sum()
    aa['price'] = x[x.price <= open].sum().amount

    return aa

def calc(code):

        #logging.info('cur_day: ' + str(cur_day) + str(threading.currentThread()))
    #sql = "SELECT code, date, open FROM dailydata where date >= DATE '" + str(cur_day) + "' and date <= DATE '" + str(stop_day) + "'" + "order by date"
    #dailyK = dailyK.append(pd.read_sql(sql, engine, index_col=['code', 'date']))
    try:

        engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
        sql = "SELECT time, price, amount FROM tick_tbl_" + code
        t1 = datetime.datetime.now()

        aa = pd.read_sql(sql, engine, index_col='time', parse_dates={'time': '%Y-%m-%d'}, chunksize=500000)

        df = pd.concat(aa)

        gg = df.groupby(to_date)
        zz = gg.apply(test)
        zz.columns = ('less', 'greater')
        zz.to_sql('amount_' + code, engine, if_exists='replace', dtype={'time': DateTime})

        logging.info('finished: ' + str(code) + ' in ' +str( datetime.datetime.now() - t1) + str(threading.currentThread()))
    except Exception:
        logging.info('exception: ' + str(code) + str(threading.currentThread()))





    # while cur_day < end_date:
    #     try:
    #         aa = dailyK[dailyK.index == cur_day]
    #         aa = aa.sort_values(by='totalcap', ascending=True)
    #         aa = aa.head(310)
    #         if aa.empty:
    #             cur_day += oneday
    #             continue
    #         result = pd.DataFrame(columns=['code', 'date', 'g', 'l'])
    #         logging.info(' processing on  ' + str(cur_day))
    #         for i in range(0, len(aa), 1):
    #             row = aa.ix[i]
    #             if row.open < 0.01:
    #                 continue
    #             one
    #             onedayTick = pd.DataFrame()
    #             sql = "SELECT time, price, amount FROM tick_tbl_" + row.code + " where time > DATE '" + str(
    #                 cur_day) + "' and time < DATE '" + str(cur_day + oneday) + "'"
    #             try:
    #                 onedayTick = pd.read_sql(sql, engine, index_col='time', parse_dates={'time': '%Y-%m-%d'})
    #             except Exception:
    #                 continue
    #             print onedayTick
    #             if onedayTick.empty:
    #                 logging.error(str(row.code) + ' tick data missed on  ' + str(cur_day))
    #                 continue
    #             greaterThanOpen = onedayTick[onedayTick.price >= row.open]
    #             lessThanOpen = onedayTick[onedayTick.price <= row.open]
    #             result.loc[i] = (row.code, cur_day, greaterThanOpen['amount'].sum(), lessThanOpen['amount'].sum())
    #         cur_day += oneday
    #         result.to_sql('amount'+str(start_date), engine, index=False, if_exists='append')
    #     except KeyError:
    #         cur_day += oneday



#calc('002271', engine, datetime.date(2005, 01, 01), datetime.date(2005, 02, 01))

def thread_func(start, end):
    target_list = dataloader.get_code_list(str(start), str(end))
    itr = target_list.itertuples()
    row = next(itr)
    while row:
        calc( row.code)
        row = next(itr)



# threads = []
# t1 = threading.Thread(target=thread_func, args=('000001','002133'))
# threads.append(t1)
# t2 = threading.Thread(target=thread_func, args=('002134','002737'))
# threads.append(t2)
# t3 = threading.Thread(target=thread_func, args=('002738','600060'))
# threads.append(t3)
# t4 = threading.Thread(target=thread_func, args=('600061','600764'))
# threads.append(t4)
# t5 = threading.Thread(target=thread_func, args=('600765','604000'))
# threads.append(t5)




if __name__ == "__main__":

    thread_func('300113', '604000')
    # for t in threads:
    #     t.setDaemon(True)
    #     t.start()
    # t.join()





