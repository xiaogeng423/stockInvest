#--------------------
# Purpose: crawl_main
# Author: Lewis
# Date: 2022-01-11

#--------------------
import os, glob, sys
import time
import datetime
import re
import pandas as pd
import random

#--------------------
"""
input = {'co_type': '農業科技', 'n_periods': '1', 'path': '/Users/lewis_1/Documents/SideProject/'}
locals().update(input)
"""
co_type = sys.argv[1]
n_periods = sys.argv[2]
path = sys.argv[3]

#--------------------
sys.path.append(path)
from crawler_com_defs import isfloat, isint, get_traceback
from crawler_defs import get_co_id_name_type, get_asset_byco

#--------------------
if type(n_periods) is str:
    n_periods = int(n_periods)

if type(co_type) is str and co_type != 'all':
    # co_type str to list
    co_type_new = []
    for ee in co_type.strip().split(','):
        if isfloat(ee):
            co_type_new.append(int(ee) if isint(ee) else float(ee))
        else:
            co_type_new.append(ee)
else:
    co_type_new = co_type

now = datetime.datetime.now()

# get co_id, co_name
try:
    path_new = path + 'stockInvest/tab_coInfo/'
    if os.path.isdir(path_new) == False:
        raise Exception(path_new + ' missing coInfo.')
    co_infos = get_co_id_name_type(path_new)
    co_infos = pd.DataFrame([e.split('_') for e in co_infos], columns=['co_id', 'co_name', 'co_type'])
    if type(co_type_new) is list:
        pattern = [re.escape(s) for s in co_type_new]
        pattern = '|'.join(pattern)
        co_infos = co_infos.loc[co_infos.co_type.str.contains(pattern, case=False), :]
except Exception as e:
    errMsg = get_traceback(e)
    print(errMsg)
    print('test')
    sys.exit(1)


# get n_periods seasonal asset
path_new = path + 'stockInvest/tab_asset/'
if os.path.isdir(path_new) == False:
    os.makedirs(path_new)

issue_file = {}

for co_info in co_infos.itertuples():
    year = now.year
    season = now.month % 12 // 3 + 1
    i = 0
    max_except_i = 3
    while i < n_periods:
        print('parsing', co_info.co_id, co_info.co_name, year, season)
        except_i = 0
        while True:
            try:
                df = get_asset_byco(co_info.co_id, year, season)
                df.to_csv(path_new + 'asset_' + '-'.join([str(e) for e in [co_info.co_id, co_info.co_name, year, season]]) + '.csv', encoding='utf_8_sig')
                i += 1
                break
            except Exception as e:
                errMsg = get_traceback(e)
                if bool(re.search('No tables found', errMsg)):
                    print('no data')
                    break
                print(errMsg)
                except_i += 1
            # fake sleep
            if except_i > max_except_i:
                if except_i > max_except_i + 1:
                    issue_file[str(co_info.co_id) + '-' + co_info.co_name] = '-'.join([str(e) for e in [year, season]])
                    break
                print('reach max exception tolerance, sleep 20 mins')
                time.sleep(20*60)
            else:
                # fake sleep
                time.sleep(random.randint(3,5))
        # fake sleep
        time.sleep(random.randint(3,5))
        season -= 1
        if season == 0:
            season = 4
            year -= 1

if len(issue_file) > 0:
    log = open(path_new + 'issue_file.txt', "w")
    log.write(str(issue_file))
    log.close()

#--------------------
"""
log = open(path_new + 'issue_file.txt', "r")
issue_file = eval(log.read())
log.close()
"""
#--------------------