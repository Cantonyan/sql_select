#Coding: utf-8
#Creat by Yan 2018.10.15
#Modified by Yan 2018.11.27 使用pathlib替代os模块，引入yaml配置文件
#Modified by Yan 2018.11.28 简化make_time_df、month_plus，get_father_config改为更通用的get_config

import yaml
import pandas as pd
from pathlib import Path

def get_brother_path(folder):
    brother_path = Path.cwd().parent.joinpath(folder)
    if not Path.exists(brother_path):
        Path.mkdir(brother_path)
    return brother_path
	
def get_config(parent = 0, config_path = Path.cwd(), config_name = 'config.yaml', encoding='utf-8'):
    if parent > 0:
        return get_config(parent - 1, config_path.parent)
    elif not parent:
        config_path = Path(config_path).joinpath(config_name)
        with open(config_path, 'r') as f:
            return yaml.load(f.read())
	
def make_time_df(begin_month, end_month, time_format = '%Y-%m-%d'):
    begin_month, end_month = pd.to_datetime([begin_month, end_month])
    #建立时间序列
    begin_range = pd.date_range(begin_month, end_month, freq = 'MS')
    end_range = begin_range.shift(1, freq = 'M')
    date_range = begin_range.days_in_month
    #格式化
    begin_range = begin_range.strftime(time_format)
    end_range = end_range.strftime(time_format)
    df_time = pd.DataFrame({'begin_time':begin_range, 'end_time':end_range, 'days':date_range})
    return df_time
	
def month_plus(ym, m):             
    p = pd.Period(pd.to_datetime('%s01' %ym), freq = 'M')
    return (p + m).strftime('%Y%m')