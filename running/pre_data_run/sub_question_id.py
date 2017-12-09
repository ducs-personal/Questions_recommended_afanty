# -*-coding:utf8-*-
import os
import sys
import numpy as np
import pandas as pd
import json
import csv
from multiprocessing import Pool
from lib.util import mkdir,getProvinceSet
from lib.table_data import getQidSubj
import logging

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
RECOMMEND = _DIR + '/new_database/Recommend/'

RAW_PATH = RECOMMEND + 'Raw_input/'
PRE_DATA_PATH = DATABASE + 'Pre_data_input/'
SUB_KPOINT_PATH = DATABASE + 'Sub_kpoint_input/'

pre_province_file = 'user_json_{}_{}.txt'
user_file =  'user_qid_subj_{}_{}_{}.txt'


def packFind_subkp(prov, PATH, subj_set, datetime):
    with open(PRE_DATA_PATH + datetime + '/' + pre_province_file.format(prov, datetime), 'r',
              encoding='utf-8') as pre_file:
        while True:
            line = pre_file.readline()
            if line:
                user_qid_dic = eval(line)

                question_list = getQidSubj(
                    table='question_simhash_20171111',
                    question_id=list(user_qid_dic.values())[0]
                )

                if len(question_list) > 0:
                    for subj in subj_set:
                        userid = []
                        for qlist in question_list:
                            if int(subj) in qlist:
                                userid.append(qlist)

                        if len(userid) > 0:
                            with open(PATH + '/' + user_file.format(
                                    prov, subj, datetime), 'a') as user_sub_file:

                                user_sub_file.writelines(json.dumps(
                                    list(user_qid_dic.keys()) + userid) + '\n')

            else:
                break

        pre_file.close()
        user_sub_file.close()
        logging.info("the prov : {} has been finished !".format(prov))


if __name__ == '__main__':
    datetimes = {'09-11'}

    LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
    prov_set = getProvinceSet()
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}
    # subj_set = {'2'}
    pool = Pool(3)

    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/Recom_subqid_{}.log'.format(datetime), filemode='a')

        for prov in prov_set:
            PATH = RAW_PATH + datetime + '/' + prov
            mkdir(PATH)
            logging.info("running the {0} at the time between {1}".format(prov, datetime))
            for subj in subj_set:
                if os.path.exists(PATH + '/' + user_file.format(prov, subj, datetime)):
                    os.remove(PATH + '/' + user_file.format(prov, subj, datetime))

            pool.apply_async(packFind_subkp,kwds={
                "prov":prov,
                "PATH":PATH,
                "subj_set":subj_set,
                "datetime":datetime
            })

    pool.close()
    pool.join()

