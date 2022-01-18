#--------------------
# Purpose: crawl_main
# Author: Lewis
# Date: 2022-01-11

#--------------------
import os, glob, sys
import time
import re
import random

#--------------------
"""
input = {'mkt_types': 'sii', 'cates': 'all', 'path': '/Users/lewis_1/Documents/SideProject/'}
locals().update(input)
"""
mkt_types = sys.argv[1]
cates = sys.argv[2]
path = sys.argv[3]

#--------------------
sys.path.append(path)
from crawler_com_defs import isfloat, isint, get_traceback
from crawler_defs import get_coInfo_bycate

if cates == 'all':
    cates = [80, 91]
    cates.extend(range(41))

if type(cates) is not list:
    cates = [int(e) for e in str(cates)]

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

# get company info. of all company
path_new = path + 'stockInvest/tab_coInfo/'
if os.path.isdir(path_new) == False:
    os.makedirs(path_new)

issue_file = {}

max_except_i = 3

for mkt_type in mkt_types_new:
    for cate in cates:
        print('parsing', mkt_type, cate)
        except_i = 0
        while True:
            try:
                df = get_coInfo_bycate(mkt_type=mkt_type, cate=cate)
                df.to_csv(path_new + 'coInfo_' + '-'.join([mkt_type, str(cate), df['產業類別'].iloc[0]]) + '.csv', encoding='utf_8_sig')
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
                    issue_file[mkt_type] = cate
                    break
                print('reach max exception tolerance, sleep 20 mins')
                time.sleep(20*60)
            else:
                # fake sleep
                time.sleep(random.randint(3,5))
        # fake sleep
        time.sleep(random.randint(3,5))

if len(issue_file) > 0:
    log = open(path_new + 'issue_file.txt', "w")
    log.write(str(issue_file))
    log.close()

#--------------------
#--------------------