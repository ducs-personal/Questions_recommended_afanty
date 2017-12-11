# -*-coding:utf8-*-
import os
import sys
import csv
import logging
import pandas as pd
import numpy as np
import json
import math
from simhash import Simhash
from collections import Counter
from operator import itemgetter
from multiprocessing import Pool
from lib.util import getProvinceSet,mkdir

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
EVALUATE = _DIR + '/new_database/Evaluate/'
SUBJ_KPO_PATH = EVALUATE + 'Subj_Kpo_output/'
ITEMCF_PATH = EVALUATE + 'ItemCF_eval/'

subj_kpo_file = 'subj_kpo_{}_{}_{}.txt'
itemCF_output_file = 'itemCF_{}_{}_{}.txt'


#修饰数据
def dealJsonData(data_json):
    data_list = []
    for i in range(len(data_json)):
        data = list(eval(data_json[i].replace('\n','')))
        if len(data) > 2:
            values_counts = Counter(data)
            data_list.append([[k, v/ len(data)] for k,v in values_counts.items()])

    return data_list


def itemSimilarity(data_json):
    item_item_count = dict()
    item_count = dict()

    # 计算每两个item共有的user数目
    for train_list in data_json:
        for item_count1 in train_list:
            if item_count1[0] not in item_count.keys():
                item_count[item_count1[0]] = 0.0
            item_count[item_count1[0]] += item_count1[1]
            for item_count2 in train_list:
                if item_count1 == item_count2:
                    continue
                if item_count1[0] not in item_item_count.keys():
                    item_item_count[item_count1[0]] = dict()
                if item_count2[0] not in item_item_count[item_count1[0]]:
                    item_item_count[item_count1[0]][item_count2[0]] = 0.0
                item_item_count[item_count1[0]][item_count2[0]] += item_count1[1] + item_count2[1]

    UserSimi2arr = dict()
    for i, related_items in item_item_count.items():
        for j, cij in related_items.items():
            if i not in UserSimi2arr:
                UserSimi2arr[i] = dict()

            UserSimi2arr[i][j] = 1000 * cij / (
                math.sqrt(item_count[i] * item_count[j]) * (Simhash(i).distance(Simhash(j))) )

    return UserSimi2arr


def packItemCFEval(prov, subj, datetime, ICF_PATH):
    ''' 普通过程的Apriori打包程序
                                        @param prov         省份
                                        @param subj         年级学科
                                        @param ICF_PATH      ItemCF(基于项目的协同过滤)算法输出路径
                                        @param datetime     日期区间
                                    '''
    exist_file = SUBJ_KPO_PATH + datetime + '/' + prov + '/' + subj_kpo_file.format(prov, subj, datetime)
    if os.path.exists(exist_file):
        logging.info(u"正在读取{0}省份下{1}学科的Prov_Sub_input文件的数据！".format(prov, subj))
        with open(exist_file, 'r', encoding='utf-8') as ps_file:
            data_json = ps_file.readlines()
            data_json = dealJsonData(data_json)
            ps_file.close()

            UserSimi2arr = itemSimilarity(data_json)
            del data_json

            output_file = ICF_PATH + '/' + itemCF_output_file.format(prov, subj, datetime)
            with open(output_file, 'wt') as new_file:
                for k, v in UserSimi2arr.items():
                    new_file.write(str({k: v}))
                    new_file.write('\n')

                new_file.close()

        logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到文件里！".format(prov, subj))



if __name__ == '__main__':
    datetimes = {'09-11'}

    LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
    # prov_set = getProvinceSet()
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}
    # subj_set = {'2'}
    prov_set = {'福建'}

    pool = Pool(3)
    for datetime in datetimes:
        logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                            filename='working/itemCF_Eval_{}.log'.format(datetime), filemode='a')

        for prov in prov_set:
            ICF_PATH = ITEMCF_PATH + datetime + '/' + prov
            mkdir(ICF_PATH)

            for subj in subj_set:
                pool.apply_async(packItemCFEval,kwds={
                    'prov':prov,
                    'subj':subj,
                    'datetime':datetime,
                    'ICF_PATH':ICF_PATH
                })

    pool.close()
    pool.join()