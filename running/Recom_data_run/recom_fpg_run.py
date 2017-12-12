# -*-coding:utf8-*-
import os
import sys
import numpy as np
import time
import logging
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
                                    recom_set.add(getRecomFPGth(result.replace('\n','').split('--'), set(vs)))

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
                        print(u"没有查询到生成相关的FPGrowth输出表！")

            else:
                print(u"用户{}传入的question_id在表**question_simhash_20171111**里查询不到！".format(user_id))

        user_file.close()


if __name__ == '__main__':
    datetimes = {'09-11'}
    diff = 1
    req_user = 'requir_user.csv'

    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                        filename='working/Recom_fpg_{}.log'.format(datetime), filemode='a')

        packageRcomFPGth(
            diff=diff,
            datetime=datetime,
            req_user=req_user
        )

