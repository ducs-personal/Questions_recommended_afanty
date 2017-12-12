# -*-coding:utf8-*-
import os
import sys
import pymysql
import pymysql.cursors
import re
import time
import datetime
import logging
import json
import jieba.posseg
from simhash import Simhash

LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/knkp_subkp_200w.log', filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\','/')
CONFIG_FILE = os.path.join(_DIR, 'config')
DATABASE = _DIR + '/new_database/Input/'
RAW_PATH = DATABASE + 'Raw_input/'
output_file = 'Subkpt_Knlkpt.txt'


def getKnkptSubkpt(table, output_file):
    config = json.load(open(CONFIG_FILE))
    first_id = 0
    new_dict = {}
    conn = pymysql.connect(host=config['host'], user=config['user'], passwd=config['password'], db='tiku_cloud',
                           port=3306, charset= "utf8", use_unicode=True, cursorclass = pymysql.cursors.DictCursor)
    cur = conn.cursor()

    while True:
        logging.info('read the data from question_id = {}'.format(first_id))

        sql = 'select knowledge_point,sub_kpoint,subject from {0} limit {1},100000'.format(table, first_id)
        cur.execute(sql)
        data = cur.fetchall()

        if not data:
            break

        Parser(data, output_file)
        first_id += 100000
        del data


def Parser(data, output_file):
    for row in data:
        knowledge_point = row['knowledge_point'].replace("．", '').replace(";", '；').split("；")
        with open(RAW_PATH + output_file, 'a') as new_file:
            new_file.write(str([row['subject'], row['sub_kpoint'], knowledge_point]))
            new_file.write('\n')


if __name__ == '__main__':
    jsonData = getKnkptSubkpt('question_simhash_20171111', output_file)


