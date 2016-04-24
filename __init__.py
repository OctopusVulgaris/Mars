# -*- coding:utf-8 -*-

import downloader


if __name__=="__main__":
    file_object = open('.\\conf\\sse_a.txt')
    list_of_all_the_lines = file_object.readlines()
    file_object.close()

    downloader.create_dayk_talbe()

    for line in list_of_all_the_lines:
        print "requesting " + line.strip('\n') + "..."
        downloader.request_instrument("dayk_qfq", line.strip('\n'))


