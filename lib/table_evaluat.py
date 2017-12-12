# -*-coding:utf8-*-
import os
import sys
import pymysql
import pymysql.cursors
import re
import time
import pandas as pd
import numpy as np
import json
from lib.util import getProvinceSet,mkdir
import requests
import asyncio
import random
import math

_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\','/')
CONFIG_FILE = os.path.join(_DIR, 'config')
DATABASE = _DIR + '/new_database/'

async def qidSubject(cur, table, question_id):
    sql = "select knowledge_point, subject, sub_kpoint from {0} where question_id = '{1}' ".format(table, question_id)
    cur.execute(sql)
    data = cur.fetchall()

    if len(data) != 0:
        return [data[0]['subject'], data[0]['knowledge_point'], data[0]['sub_kpoint']]

async def setKpoSubKpo(cur, table, knowledge_point):
    sql = "select sub_kpoint from {0} where knowledge_point LIKE '%{1}%' ".format(table, knowledge_point)
    cur.execute(sql)
    data = cur.fetchall()

    if len(data) != 0:
        return data[0]['sub_kpoint']


async def main_tasks(tasks):
    return await asyncio.gather(*tasks)


def evalSubKpo(table, data, mode):
    ''' 通过question_id获取字段knowledge_point
                            @param table    able表
                            @param mode     模式选择
                            @param data     包含question_id的set
                        '''
    config = json.load(open(CONFIG_FILE))
    conn = pymysql.connect(host=config['host'], user=config['user'], passwd=config['password'], db='tiku_cloud',
                           port=3306, charset= "utf8", use_unicode=True, cursorclass = pymysql.cursors.DictCursor)
    cur = conn.cursor()
    sub_kpoint_dict = {}
    question_list = []
    tasks = []

    if mode == 0:
        #模式0下是传入question_id，输出subject、knowledge_point、sub_kpoint信息
        for question_id in data:
            tasks.append(asyncio.ensure_future(qidSubject(cur, table, question_id)))
    elif mode == 1:
        # 模式1下是传入knowledge_point，输出sub_kpoint信息
        for knowledge_point in data:
            tasks.append(asyncio.ensure_future(setKpoSubKpo(cur, table, knowledge_point)))

    loop = asyncio.get_event_loop()
    try:
        results = loop.run_until_complete(main_tasks(tasks))
        for result in results:
            if result is not None:
                question_list.append(result)

    except KeyboardInterrupt as e:
        print(asyncio.Task.all_tasks())
        for task in asyncio.Task.all_tasks():
            print(task.cancel())
        loop.stop()
        loop.run_forever()

    return question_list
