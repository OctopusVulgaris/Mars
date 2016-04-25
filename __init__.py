# -*- coding:utf-8 -*-

import downloader

trade_type_dic = {
    '买盘' : 1,
    '卖盘' : -1,
    '中性盘' : 0
}


if __name__=="__main__":
    file_object = open('.\\conf\\sse_a.txt')
    list_of_all_the_lines = file_object.readlines()
    file_object.close()

    #downloader.create_dayk_talbe()

    #for line in list_of_all_the_lines:
    #    print "requesting " + line.strip('\n') + "..."
    #    downloader.request_instrument("dayk_qfq", line.strip('\n'))

    import tushare as ts
    a = ts.get_tick_data('600848', date='2016-04-24')
    if a[0:1].time == '00:00:00':
        print 'aaa'



