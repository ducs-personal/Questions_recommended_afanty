# -*-coding:utf8-*-
import os
import sys
import logging
import re
import json
import time
from multiprocessing import Pool

LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/knkp_subkp_200w.log', filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\','/')
CONFIG_FILE = os.path.join(_DIR, 'config')
DATABASE = _DIR + '/new_database/Input/'
EVALUATE = _DIR + '/new_database/Evaluate/'

RAW_PATH = DATABASE + 'Raw_input/'
SUBKPT_PATH = EVALUATE + 'Subkpt_Knlkpt_eval/Raw_eval/'
SK_PATH = EVALUATE + 'Subkpt_Knlkpt_eval/Second_eval/'

input_file = 'Subkpt_Knlkpt.txt'
output_file = 'sub_kpt_skp_{}.txt'

def RawData():
    new_dict = {}
    with open(RAW_PATH + input_file, 'r', encoding='utf-8') as new_file:
        while True:
            line = new_file.readline()
            if line:
                result = eval(line)
                if result[0] not in new_dict.keys():
                    new_dict[result[0]] = {}

                if result[1] not in new_dict[result[0]].keys():
                    new_dict[result[0]][result[1]] = set(result[2])

            else:
                break
        new_file.close()

    for ks, vs in new_dict.items():
        with open(SUBKPT_PATH + output_file.format(ks), 'wt', encoding='utf-8') as first_file:
            first_file.write(str(dict(vs)))
            first_file.write('\n')

    first_file.close()
    del new_dict
    time.sleep(100)


def dealFirstData(subj):
    second_dict = {}
    if os.path.exists(SUBKPT_PATH + output_file.format(subj)):

        with open(SUBKPT_PATH + output_file.format(subj), 'r', encoding='utf-8') as second_file:
            while True:
                line = second_file.readline()
                if line:
                    result = eval(line)
                    for ks,vs in result.items():
                        for v in vs:
                            if v not in second_dict.keys():
                                second_dict[v] = set([])
                            second_dict[v].add(ks)

                else:
                    break
            second_file.close()

        with open(SK_PATH + output_file.format(subj), 'wt', encoding='utf-8') as third_file:
            third_file.write(str(second_dict))


if __name__ == '__main__':
    # RawData()

    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)} | {
    '52', '62'}
    # subj_set = {'2'}
    pool = Pool(2)

    for subj in subj_set:
        pool.apply_async(dealFirstData,kwds={
            "subj":subj
        })
        # dealFirstData(subj)

    pool.close()
    pool.join()

