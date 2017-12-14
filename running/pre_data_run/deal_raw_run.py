# -*-coding:utf8-*-
import os
import sys
import csv
import time
import json
import pandas as pd
import platform
from optparse import OptionParser
from lib.util import getProvinceSet,mkdir
from multiprocessing import Pool


_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
RAW_PATH = DATABASE + 'Raw_input/'
PRE_DATA_PATH = DATABASE + 'Pre_data_input/'
pre_province_txt = 'user_json_{}_{}.txt'


def packRawdeal(datetime, init_file):
    exit_file = RAW_PATH + init_file.format(datetime)
    if os.path.exists(exit_file):
        province_set = {'nan', '0', '1', '2', '3', 'Rize', 'Juzny', 'hobbit', '全国', '台湾', 'NULL'}
        mkdir(PRE_DATA_PATH + datetime)
        PATH = PRE_DATA_PATH + datetime + '/'
        with open(exit_file, 'r', encoding='utf-8') as raw_file:
            readers = csv.DictReader(raw_file)
            for reader in readers:
                prov = reader['province']
                if prov in province_set:
                    prov = '全国'
                elif prov in {'闽'}:
                    prov = '福建'
                else:
                    prov = prov[:2]

                time_question = []
                try:
                    if isinstance(reader['question'], str):
                        if len(eval(reader['question'])) > 0:
                            time_question = [i[1] for i in eval(reader['question'])]
                except Exception as e:
                    print(e)
                finally:
                    rank = {reader['user_id']: time_question}
                    with open(PATH + pre_province_txt.format(prov, datetime), 'a', encoding='utf-8') as prov_file:
                        prov_file.writelines(json.dumps(rank) + '\n')

            raw_file.close()

    else:
        print("该路径{0}下没有找到文件".format(exit_file))

    # os.remove(RAW_PATH + init_file.format(datetime))


if __name__ == '__main__':
    if 'Windows' in platform.system():
        req_user = 'requir_user.csv'
        datetimes = {'09-11'}
        pool = Pool(2)

    elif 'Linux' in platform.system():
        optparser = OptionParser()
        optparser.add_option('-f', '--inputfile',
                             dest='input',
                             help='必须是csv格式的文件, 如user_if_{}.csv',
                             default='user_if_{}.csv')
        optparser.add_option('-t', '--datetimes',
                             dest='datetimes',
                             help='包含时间区间的集合,若是多个时用 , （英文逗号）分割开',
                             default='09-11')
        (options, args) = optparser.parse_args()

        req_user = options.input
        datetimes = set(options.datetimes.replace(' ','').split(','))
        pool = Pool(4)

    datetimes = {'09-11'}
    init_file = 'user_if_{}.csv'

    for datetime in datetimes:
        pool.apply_async(packRawdeal, kwds={
            "datetime":datetime,
            "init_file":init_file
        })

    pool.close()
    pool.join()

