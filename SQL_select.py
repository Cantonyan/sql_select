#Coding: utf-8
#Creat by Yan 2018.09.14
#Modified by Yan 2018.09.21 v2.0 #增加hive查询
#Modified by Yan 2018.11.27 v2.1 #增加id_config.yaml储存配置文件
#Modified by Yan 2018.11.29 v2.2 #select_hive使用类实现，实现内部的参数传递
#Modified by Yan 2019.10.31 v2.3 #增加down_excel、cancel方法
#Modified by Yan 2019.12.05 v2.4 #增加判断数据库表名，减少输入参数

import re
import pyodbc
import time
import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from py_tools import get_config
    
##                            sql查询                             ##
#连接数据库
def select_sql():
    try:
        config = get_config(config_path = Path(__file__).parent, config_name = 'id_config.yaml')
        conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' \
                              %(config['sql']['SERVER'], config['sql']['DATABASE'],\
                                config['sql']['UID'], config['sql']['PWD']))
        print('数据库连接成功')
    except Exception as e:
        print('数据库连接失败')
        print(e)
    return conn

	
##                            hive查询                             ##
#定义初始头部信息
class select_hive:
    headers = {'Host': '10.10.1.39', \
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:61.0) Gecko/20100101 Firefox/61.0',\
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',\
              'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',\
              'Accept-Encoding': 'gzip, deflate',\
              'Connection': 'keep-alive'}
    session = requests.session()
    cookies = None
#     id_config = get_config(config_name = 'id_config.yaml')['hive']
    id_config = get_config(config_path = Path(__file__).parent, config_name = 'id_config.yaml')['hive']
    
    def __init__(self):
        self.username = self.id_config['username']
        self.password = self.id_config['password']
    
    def get(self, url, *args, **kwargs):
        try:
            return self.session.get(url, *args, **kwargs)
        except Exception as e:
            print('网络连接失败')
            print(e)
    
    def post(self, url, *args, **kwargs):
        try:
            return self.session.post(url, *args, **kwargs)
        except Exception as e:
            print('网络连接失败')
            print(e)
    
    def log(self):
        #第一次访问
        url_begin = 'http://10.10.1.39/accounts/login/?next=/beeswax/'
        r = self.get(url = url_begin, headers=self.headers, cookies=self.cookies)
        #获取cookies
        self.cookies = r.cookies

        #登录hive
        url_log = 'http://10.10.1.39/accounts/login/'
        data = {'csrfmiddlewaretoken':r.cookies['csrftoken'], \
                'username':self.username, 'password':self.password, 'next':'/beeswax/'}
        r_log = self.post(url_log, data=data, headers=self.headers, cookies=self.cookies)
        #获取登录后的cookies
        self.cookies_log = r_log.cookies

        #登录测试
        url_log_test = 'http://10.10.1.39/beeswax/#query'
        r_log_test = self.get(url_log_test, headers=self.headers, cookies=self.cookies_log)
        
        if r_log_test.text.find(self.username) > 0:
            #登录成功后，制作登录后的头
            self.headers_log = self.headers.copy()
            self.headers_log.update({'Accept': '*/*',\
                                   'Referer': 'http://10.10.1.39/beeswax/',\
                                   'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',\
                                   'X-Requested-With': 'XMLHttpRequest',\
                                   'X-CSRFToken': self.cookies_log['csrftoken']})
            print('hive log sucess')
    
        
    def execute(self, hive_query, hive_database='a'):
        #查询地址、headers、data
        url_para = 'http://10.10.1.39/beeswax/api/query/parameters'
        url_exe = 'http://10.10.1.39/beeswax/api/query/execute/'
        hive_database = re.findall('from\s*(\S*)\.', hive_query, re.I)[0]
        data_log = {'query-query':hive_query, 'query-database':hive_database,\
             'settings-next_form_id':'0','file_resources-next_form_id':'0', 'functions-next_form_id':'0', \
             'query-email_notify':'false', 'query-is_parameterized':'true'}

        #执行查询
        r_para = self.post(url_para, data=data_log, headers=self.headers_log, cookies=self.cookies_log)
        r_exe = self.post(url_exe, data=data_log, headers=self.headers_log, cookies=self.cookies_log)
        #获取查询id
        exe_id = r_exe.json()['id']
        print('hive id : %s'%  exe_id)

        #生成id观察地址
        url_watch = 'http://10.10.1.39/beeswax/api/watch/json/%s' %exe_id

        #执行观察
        r_watch = self.post(url_watch, data=data_log, headers=self.headers_log, cookies=self.cookies_log)
        #获取观察数据，直到成功才返回
        j_watch = r_watch.json()
        while (not j_watch['isSuccess']) and (not j_watch['isFailure']):
            time.sleep(5)
            r_watch = self.post(url_watch, data=data_log, headers=self.headers_log, cookies=self.cookies_log)
            j_watch = r_watch.json()
            if j_watch['log']:
                print(j_watch['log'])
        #返回id
        return exe_id

    def down_csv(self, exe_id):
        url_down_csv = 'http://10.10.1.39/beeswax/download/%s/csv' %exe_id
        #下载
        r_down_csv = self.get(url_down_csv, headers=self.headers_log, cookies=self.cookies_log)
        df_csv = pd.read_csv(BytesIO(r_down_csv.content))
        return df_csv

    def down_excel(self, exe_id):
        url_down_excel = 'http://10.10.1.39/beeswax/download/%s/xls' % exe_id
        # 下载
        r_down_excel = self.get(url_down_excel, headers=self.headers_log, cookies=self.cookies_log)
        df_excel = pd.read_excel(BytesIO(r_down_excel.content))
        return df_excel


    def cancel(self, exe_id):
        url_cancel = 'http://10.10.1.39/beeswax/api/query/%s/cancel' % exe_id
        r_cancel = self.post(url_cancel , headers=self.headers_log, cookies=self.cookies_log)
        if r_cancel.json()['status'] == 0:
            print('cancel success')
        else:
            print('cancel fail')

