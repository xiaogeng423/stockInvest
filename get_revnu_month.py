#--------------------
# Purpose: crawl_main
# Author: Lewis
# Date: 2022-01-11

#--------------------
import os, glob, sys
import time
import datetime
import re
import random

#--------------------
"""
input = {'mkt_types': 'sii', 'n_periods': 1, 'path': '/Users/lewis_1/Documents/SideProject/'}
locals().update(input)
"""
mkt_types = sys.argv[1]
n_periods = sys.argv[2]
path = sys.argv[3]

#--------------------
sys.path.append(path)
from crawler_com_defs import isfloat, isint, get_traceback
from crawler_defs import get_revnue_all

#--------------------
if type(n_periods) is str:
    n_periods = int(n_periods)

if type(mkt_types) is str and mkt_types != 'all':
    # mkt_types str to list
    mkt_types_new = []
    for ee in mkt_types.strip().split(','):
        if isfloat(ee):
            mkt_types_new.append(int(ee) if isint(ee) else float(ee))
        else:
            mkt_types_new.append(ee)
else:
    mkt_types_new = ['sii', 'otc', 'rotc']

now = datetime.datetime.now()

# get n_periods monthly revenue
path_new = path + 'stockInvest/tab_revnu/'
if os.path.isdir(path_new) == False:
    os.makedirs(path_new)

issue_file = {}

for mkt_type in mkt_types_new:
    year = now.year
    month = now.month
    i = 0
    max_except_i = 3
    while i < n_periods:
        print('parsing', mkt_type, year, month)
        except_i = 0
        while True:
            try:
                df = get_revnue_all(mkt_type, year, month)
                df.to_csv(path_new + 'revnu_' + '-'.join([str(e) for e in [mkt_type, year, month]]) + '.csv', encoding='utf_8_sig')
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
                    issue_file[mkt_type] = '-'.join([str(e) for e in [year, month]])
                    break
                print('reach max exception tolerance, sleep 20 mins')
                time.sleep(20*60)
            else:
                # fake sleep
                time.sleep(random.randint(3,5))
        # fake sleep
        time.sleep(random.randint(3,5))
        month -= 1
        if month == 0:
            month = 12
            year -= 1

if len(issue_file) > 0:
    log = open(path_new + 'issue_file.txt', "w")
    log.write(str(issue_file))
    log.close()

#--------------------
#--------------------