# -*-coding:utf8-*-
import os
import sys
import numpy as np
import pandas as pd
import json
import csv
import platform
from optparse import OptionParser
from multiprocessing import Pool
from lib.util import mkdir,getProvinceSet
from lib.table_data import tableToJson
import logging

LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
PRE_DATA_PATH = DATABASE + 'Pre_data_input/'
SUB_KPOINT_PATH = DATABASE + 'Sub_kpoint_input/'
pre_prov = 'user_json_{}_{}.txt'
skp_file = 'question_sub_kpoint_{}_{}.txt'


def packFind_subkp(prov, PATH, pre_prov, datetime, skp_file):
    if os.path.exists(PATH + skp_file.format(prov, datetime)):
        os.remove(PATH + skp_file.format(prov, datetime))

    pp_file = PRE_DATA_PATH + datetime + '/' + pre_prov.format(prov, datetime)
    if os.path.exists(pp_file):
        with open(pp_file, 'r') as pre_file:
            while True:
                line = pre_file.readline()
                if line:
                    user_qid_dic = eval(line)

                    question_list = tableToJson(
                        table='question_simhash_20171111',
                        question_id=list(user_qid_dic.values())[0]
                    )

                    if len(question_list) > 0:
                        userid = list(user_qid_dic.keys())
                        user_sub = userid.extend(question_list)

                        with open(PATH + skp_file.format(prov, datetime), 'a') as prov_sub_file:
                            prov_sub_file.writelines(json.dumps(userid) + '\n')

                else:
                    break

            pre_file.close()
            prov_sub_file.close()
            logging.info("the prov : {} has been finished !".format(prov))
    else:
        logging.error("此路径{}\t下不存在文件".format(pp_file))


if __name__ == '__main__':
    if 'Windows' in platform.system():
        datetimes = {'09-11'}
        pool = Pool(2)

    elif 'Linux' in platform.system():
        optparser = OptionParser()
        optparser.add_option('-t', '--datetimes',
                             dest='datetimes',
                             help='包含时间区间的集合,若是多个时用 , （英文逗号）分割开',
                             default='09-11')
        (options, args) = optparser.parse_args()

        datetimes = set(options.datetimes.replace(' ','').split(','))
        pool = Pool(4)

    prov_set = getProvinceSet()
    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/find_subkp_{}.log'.format(datetime), filemode='a')
        mkdir(SUB_KPOINT_PATH + datetime)
        PATH = SUB_KPOINT_PATH + datetime + '/'

        for prov in prov_set:
            logging.info("running the prov is: {}".format(prov))

            pool.apply_async(packFind_subkp,kwds={
                "prov":prov,
                "PATH":PATH,
                "datetime":datetime
            })

    pool.close()
    pool.join()

