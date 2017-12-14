# -*-coding:utf8-*-
import os
import sys
import numpy as np
import time
import logging
import platform
from optparse import OptionParser
from lib.table_data import tableToJson, getQuestionId
import pandas as pd
import csv
import json
from lib.util import (
    mkdir,
    getRecomFPGth,
    getProvinceSet
)

LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
RECOMMEND = _DIR + '/new_database/Recommend/'
ANALY = _DIR + '/new_database/Analy/'
FPGTH_PATH = ANALY + 'FPGrowth_output/'
USER_PATH = RECOMMEND + 'user/'
RECOM_FPGTH_PATH = RECOMMEND + 'FPGrowth/'

fpgth_output_file = 'fp_growth_{}_{}_{}.txt'
output_file = 'fp_output_{}_{}.csv'

def packageRcomFPGth(req_user, datetime, diff):
    ''' 对推荐试题结果程序进行封装打包
                                @param req_user     原始数据文件(user_id, province， question)
                                @param diff         难度系数，int
                                @param datetime     时间区间
                            '''
    prov_set = getProvinceSet()
    diff = diff or 1

    filename = RECOM_FPGTH_PATH + output_file.format(datetime, time.strftime("%Y%m%d"))
    if os.path.exists(filename):
        os.remove(filename)

    with open(USER_PATH + req_user, 'r') as user_file:
        readers = csv.DictReader(user_file)
        for reader in readers:
            prov = reader['province'][:2]
            if prov not in prov_set:
                prov = '全国'

            user_id = reader['user_id']
            question_id = eval(reader['question'])

            question_list = tableToJson(
                table='question_simhash_20171111',
                question_id=question_id
            )

            if len(question_list) > 0:
                subj_dic = {}
                recom_set = set([])

                for sub_kpoint in question_list:
                    if str(sub_kpoint[1]) not in subj_dic.keys():
                        subj_dic[str(sub_kpoint[1])] = [sub_kpoint[0]]
                    else:
                        subj_dic[str(sub_kpoint[1])].append(sub_kpoint[0])

                for ks, vs in subj_dic.items():
                    fpg_file = FPGTH_PATH + datetime + '/' + prov + '/' + fpgth_output_file.format(prov, ks,
                                                                                                        datetime)
                    if os.path.exists(fpg_file):
                        logging.info(u"正在读取{0}省份下{1}学科的Analy下的FPGrowth数据！".format(prov, ks))
                        with open(fpg_file, 'r') as recom_file:
                            while True:
                                result = recom_file.readline()
                                if result:
                                    recom_set |= getRecomFPGth(result.replace('\n','').split('--'), set(vs), k=1)

                                else:
                                    break

                        recom_list = getQuestionId(
                            table='question_simhash_20171111',
                            question_id=question_id,
                            recom_set=recom_set,
                            diff=diff
                        )

                        with open(filename, 'a') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow([user_id, ks, recom_list])

                        logging.info(u"已经解析完用户{}！".format(user_id))

                    else:
                        print(u"在该{0}路径下没有查询到相关的FPGrowth输出表！".format(fpg_file))

            else:
                print(u"用户{}传入的question_id在表**question_simhash_20171111**里查询不到！".format(user_id))

        user_file.close()


if __name__ == '__main__':
    if 'Windows' in platform.system():
        req_user = 'requir_user.csv'
        datetimes = {'09-11'}
        diff = 1

    elif 'Linux' in platform.system():
        optparser = OptionParser()
        optparser.add_option('-f', '--inputfile',
                             dest='input',
                             help='必须是csv格式的文件, 如requir_user.csv',
                             default='requir_user.csv')
        optparser.add_option('-t', '--datetimes',
                             dest='datetimes',
                             help='包含时间区间的集合,若是多个时用 , （英文逗号）分割开',
                             default='09-11')
        optparser.add_option('-d', '--difficulty',
                             dest='diff',
                             help='整数值0,1,2',
                             default=1,
                             type='int')

        (options, args) = optparser.parse_args()

        req_user = options.input
        if not os.path.exists(USER_PATH + req_user):
            sys.exit("该路径下{0}下没有{1}文件，请将文件放到上面路径下".format(USER_PATH, req_user))

        datetimes = set(options.datetimes.replace(' ','').split(','))
        diff = options.diff

    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                        filename='working/Recom_fpg_{}.log'.format(datetime), filemode='a')

        packageRcomFPGth(
            diff=diff,
            datetime=datetime,
            req_user=req_user
        )

