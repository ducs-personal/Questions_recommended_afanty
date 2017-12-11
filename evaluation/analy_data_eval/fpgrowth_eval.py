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
# from running.analy_data_run.fpgrowth_run import (
#     bigFreqItems,
#     smallFreqItems,
#     middleFreqItems
# )

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
EVALUATE = _DIR + '/new_database/Evaluate/'
SUBJ_KPO_PATH = EVALUATE + 'Subj_Kpo_output/'
FPGROWTH_PATH = EVALUATE + 'FPGrowth_eval/'

subj_kpo_file = 'subj_kpo_{}_{}_{}.txt'
fpgth_output_file = 'fpgrowth_{}_{}_{}.txt'


#修饰数据
def dealJsonData(data_json):
    data_list = []
    for i in range(len(data_json)):
        data = list(set(eval(data_json[i].replace('\n',''))))
        if len(data) > 2:
            data_list.append(data)

    return data_list


def getfreqitem(dataSet, minS, k):
    freqItems = []
    freqlist = fpGrowth(dataSet, minSup=minS)

    for i in freqlist:
        if len(i) >= k:
            freqItems.append(i)

    return freqItems


def bigFreqItems(dataSet, minS_k, getsize):
    door_len = 0
    minSup = int(3 * getsize / 10000)

    while door_len < 4000 and minSup > minS_k:
        freqItems = []
        minSup = int(minSup * 0.95)
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup, k=3)
        door_len = len(freqItems)

    if len(freqItems) > 10000:
        if minSup != int(minSup * 1.06):
            freqItems = getfreqitem(dataSet=dataSet, minS=int(minSup * 1.06), k=3)
        else:
            freqItems = getfreqitem(dataSet=dataSet, minS=minSup + 1, k=3)

    return freqItems


def middleFreqItems(dataSet, minS_k):
    door_len = 0
    minSup = 100
    while door_len < 500 and minSup > minS_k:
        minSup = int(minSup * 0.95)
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup, k=3)
        door_len = len(freqItems)

    # if len(freqItems) < 20:
    #     freqItems = getfreqitem(dataSet=dataSet, minS=2, k=2)

    if len(freqItems) > 2000:
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup + 1, k=3)

    return freqItems


def smallFreqItems(dataSet, minS_k):
    door_len = 0
    minSup = 20
    while door_len < 200 and minSup > minS_k:
        minSup = int(minSup * 0.95)
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup, k=2)
        door_len = len(freqItems)

    if len(freqItems) > 2000:
        freqItems = getfreqitem(dataSet=dataSet, minS=minSup + 1, k=2)

    return freqItems


def packageFPGrowthEval(prov, subj, datetime, FO_PATH):
    ''' 普通过程的fpgrowth打包程序
                                    @param prov         省份
                                    @param subj         年级学科
                                    @param FO_PATH      FPGrowth算法输出路径
                                    @param datetime     日期区间
                                '''
    output_file = FO_PATH + '/' + fpgth_output_file.format(prov, subj, datetime)
    if os.path.exists(output_file):
        os.remove(output_file)

    exist_file = SUBJ_KPO_PATH + datetime + '/' + prov + '/' + subj_kpo_file.format(prov, subj, datetime)
    if os.path.exists(exist_file):
        logging.info(u"正在读取{0}省份下{1}学科的Subj_Kpo_output文件下数据！".format(prov, subj))
        with open(exist_file, 'r', encoding='utf-8') as ps_file:
            data_json = ps_file.readlines()
            data_json = dealJsonData(data_json)
            exist_file_size = os.path.getsize(exist_file)

            if  exist_file_size > 1000000:
                freqItems = bigFreqItems(dataSet=data_json, minS_k=8, getsize=exist_file_size)
            elif exist_file_size < 200000:
                freqItems = smallFreqItems(dataSet=data_json, minS_k=2)
            else:
                freqItems = middleFreqItems(dataSet=data_json, minS_k=5)

            del data_json
            if len(freqItems) > 0:
                with open(output_file, 'wt') as csv_file:
                    for fi in freqItems:
                        csv_file.writelines('--'.join(i for i in fi))
                        csv_file.write('\n')
                    csv_file.close()

            ps_file.close()


if __name__ == '__main__':
    datetimes = {'09-11'}

    LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
    prov_set = getProvinceSet()
    # prov_set = {'山东'}
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)} | {
    '52', '62'}

    pool = Pool(2)
    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/Eval_fpgrowth_{}.log'.format(datetime), filemode='a')
        for prov in prov_set:
            FO_PATH = FPGROWTH_PATH + datetime + '/' + prov
            mkdir(FO_PATH)

            for subj in subj_set:
                pool.apply_async(packageFPGrowthEval, kwds={
                    "prov": prov,
                    "subj": subj,
                    "datetime": datetime,
                    "FO_PATH": FO_PATH
                })
                logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到文件！".format(prov, subj))

    pool.close()
    pool.join()