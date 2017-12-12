# -*-coding:utf8-*-
import os
import sys
import csv
import logging
import pandas as pd
import numpy as np
import json
from multiprocessing import Pool
from data_mining.FPGrowth import fpGrowth
from lib.util import getProvinceSet,mkdir
from lib.table_evaluat import evalSubKpo

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
EVALUATE = _DIR + '/new_database/Evaluate/'

PRE_DATA_PATH = DATABASE + 'Pre_data_input/'
SUBJ_KPO_PATH = EVALUATE + 'Subj_Kpo_output/'

pre_province_file = 'user_json_{}_{}.txt'
output_file = 'subj_kpo_{}_{}_{}.txt'


def packSubjKpo(prov,  PATH, datetime):
    input_file = PRE_DATA_PATH + datetime + '/' + pre_province_file.format(prov, datetime)
    if os.path.exists(input_file):
        with open(input_file, 'r') as pre_file:

            while True:
                line = pre_file.readline()
                if line:
                    question_list = evalSubKpo(
                        mode=0,
                        data=list(eval(line).values())[0],
                        table='question_simhash_20171111'
                    )

                    if len(question_list) > 1:
                        new_dict = {}
                        for qid in question_list:
                            if qid[0] not in new_dict.keys():
                                new_dict[qid[0]] = qid[1].replace("．", '').replace(";",'；').split("；")
                            else:
                                new_dict[qid[0]].extend(qid[1].replace("．", '').replace(";",'；').split("；"))
                                new_dict[qid[0]] = new_dict[qid[0]]

                        for key in new_dict.keys():
                            with open(PATH + '/' + output_file.format(
                                    prov, key, datetime), 'a') as n_file:
                                n_file.write(str(new_dict[key]) + '\n')

                else:
                    break

            pre_file.close()
            n_file.close()
            logging.info("the prov : {} has been finished !".format(prov))

    else:
        logging.error("在{0}区间，该{1}省的{2}年级科目下，没有文件".format(datetime, prov, subj))


if __name__ == '__main__':
    datetimes = {'09-11'}

    LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
    prov_set = getProvinceSet()
    # prov_set = {"全国"}
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}
    pool = Pool(3)

    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/Recom_subqid_{}.log'.format(datetime), filemode='a')

        for prov in prov_set:
            PATH = SUBJ_KPO_PATH + datetime + '/' + prov
            mkdir(PATH)
            logging.info("running the {0} at the time between {1}".format(prov, datetime))

            for subj in subj_set:
                if os.path.exists(PATH + '/' + output_file.format(prov, subj, datetime)):
                    os.remove(PATH + '/' + output_file.format(prov, subj, datetime))

            pool.apply_async(packSubjKpo, kwds={
                "prov": prov,
                "PATH": PATH,
                "datetime": datetime
            })
            logging.info("在{0}期间的{1}省，完成任务！".format(datetime, prov))
            # packSubjKpo(prov, PATH, datetime)

    pool.close()
    pool.join()
