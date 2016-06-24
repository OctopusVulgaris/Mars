# -*- coding:utf-8 -*-
import os
import datetime
import numpy as np
import pandas as pd
import pandas.io.sql as psql
import sqlalchemy as sa
import logging
import pickle
import threading

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='loader.txt'
                    )

def get_code_list(start_code, end_code, engine):
    sql = ''
    if(start_code == '' and end_code == ''):
        sql = r"select code,name from stock_list where code not like '2_____' and code not like '9_____'"
    else:
        sql = "SELECT code,name FROM stock_list where code not like '2_____' and code not like '9_____' and CAST(code AS Integer) >= " + start_code +" and CAST(code AS Integer) <= " + end_code
#    sql = "SELECT * FROM tick_tbl_000001 where time < DATE '2016-01-01' and time > DATE '2015-01-01'"
    df = pd.read_sql(sql, engine)
    return df

def get_index_list(start_code, end_code, engine):
    sql = ''
    if(start_code == '' and end_code == ''):
        sql = 'SELECT code, name FROM index_list'
    else:
        sql = 'SELECT code, name FROM index_list where CAST(code AS Integer) >= ' + start_code +' and CAST(code AS Integer) <= ' + end_code
    return pd.read_sql(sql, engine,)


_DAY = '_day'
code = '600000'
start_date = '1990-01-01'
end_date = '2050-01-01'
fuquan = 'qfq'
table = code + _DAY + '_' + fuquan
engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=True)
#read example
#dayK1 = pd.read_sql_query(text('SELECT open, high FROM "dayk_table" WHERE date =:date1;'), engine, params={'date1':'2016-01-04'})
#dayK1 = pd.read_sql_table(table, engine, index_col = 'date')
#dayK2 = dayK1.copy()

#dayK1.to_sql('postgres', engine, if_exists='append')
#itr = dayK1.itertuples()
#row = next(itr)
#while row:
#    print row;
#    row = next(itr)

#print dayK1[dayK1.date == '2016-04-21'].high

def load_daily_data(engine):
    full_df = pd.DataFrame()
    #print datetime.datetime.now()
    if os.path.exists(r'fd.csv'):
        full_df = pd.read_csv('fd.csv', encoding='utf-8', index_col=('code','date'), dtype={'code':'|S6'})
        #print full_df
        #full_df.to_csv('fd1.csv', encoding='utf-8')
        #return
    else:
        chunk_size = 300000
        offset = 0
        dfs = []
        while True:
            sql = "SELECT * FROM dailydata order by code, date limit %d offset %d" % (chunk_size,offset)
            dfs.append(psql.read_sql(sql, engine, index_col=['code','date'], parse_dates=True))
            offset += chunk_size
            if len(dfs[-1]) < chunk_size:
                break
        full_df = pd.concat(dfs)
        del dfs
        logging.info("Loading data fininshed")

        #full_df = full_df.set_index([full_df.index, full_df['date']], drop=False)
        
    logging.info("Done")
    ma_list = [5,10,20,30,60,120]
    for i in range(len(full_df.index.levels[0])):
        logging.info("iloc starts:"+str(datetime.datetime.now()))
        tdf = full_df.iloc[full_df.index.get_level_values('code')==full_df.index.levels[0][i]]
        tdf.reset_index('code',inplace=True)
        logging.info("ma starts:"+str(datetime.datetime.now()))
        for ma in ma_list:
            #tdf['MA_' + str(ma)] = pd.rolling_mean(tdf['close'], ma)
            tdf.loc[:,('MA_' + str(ma))] = pd.Series(tdf['close']).rolling(ma).mean()
        #print tdf
        logging.info("ma ends:"+str(datetime.datetime.now()))
    logging.info("MA Done")
    print full_df

            #full_df['MA_' + str(ma)] = pd.rolling_mean(full_df['close'], ma)
    # for ma in ma_list:
    #     full_df['EMA_' + str(ma)] = pd.ewma(full_df['close'], span=ma)
    #f = open("pickle.db", "wb");
    #pickle.dump(full_df, f)
    #f.close()
    #return full_df
    #full_df.to_csv('fd.csv', encoding='utf-8')

def load_dailydata_from_db_to_file():
    chunk_size = 300000
    offset = 0
    dfs = []
    while True:
        sql = "SELECT *,rank() OVER (PARTITION BY date ORDER BY totalcap asc) as rank1, rank() OVER (PARTITION BY date ORDER BY totalcap desc) \
        as rank2 FROM dailydata order by date desc limit %d offset %d" % (chunk_size, offset)
        #sql = "SELECT *,rank() OVER (PARTITION BY date ORDER BY totalcap asc) as rank1, rank() OVER (PARTITION BY date ORDER BY totalcap desc) \
        #as rank2 FROM dailydata where code='000001' order by date desc limit %d offset %d" % (chunk_size, offset)
        dfs.append(psql.read_sql(sql, engine, index_col=['date'], parse_dates=True))
        offset += chunk_size
        if len(dfs[-1]) < chunk_size:
            break
    full_df = pd.concat(dfs)
    del dfs
    logging.info("Loading data fininshed")
    full_df.to_csv('fd.csv', encoding='utf-8')

def generate_quit_stock_from_db_to_file():
    sql = "SELECT code from stock_list where status = -1"
    dfs = pd.read_sql(sql, engine,)
    dfs.to_csv('quit.csv', encoding='utf-8', index=False)

def load_indexdaily_from_db_to_file():
    chunk_size = 100000
    offset = 0
    dfs = []
    while True:
        sql = "SELECT * from indexdaily order by date desc limit %d offset %d" % (chunk_size, offset)
        dfs.append(psql.read_sql(sql, engine, index_col=['date'], parse_dates=True))
        offset += chunk_size
        if len(dfs[-1]) < chunk_size:
            break
    full_df = pd.concat(dfs)
    del dfs
    logging.info("Loading data fininshed")
    full_df.to_csv('index.csv', encoding='utf-8')

if __name__=="__main__":
    #load_daily_data(engine)
    #load_dailydata_from_db_to_file()
    #generate_quit_stock_from_db_to_file()
    load_dailydata_from_db_to_file()