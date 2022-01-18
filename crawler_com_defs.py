#--------------------
# Purpose: common api for crawler
# Author: Lewis
# Date: 2022-01-11

#--------------------
import os, glob, sys
import datetime
import traceback

#--------------------
def isfloat(x):
    try:
        a = float(x)
    except (TypeError, ValueError):
        return False
    else:
        return True

def isint(x):
    try:
        a = float(x)
        b = int(a)
    except (TypeError, ValueError):
        return False
    else:
        return a == b

def get_traceback(e):
    error_class = e.__class__.__name__ #取得錯誤類型
    detail = e.args[0] #取得詳細內容
    cl, exc, tb = sys.exc_info() #取得Call Stack
    lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    fileName = lastCallStack[0] #取得發生的檔案名稱
    lineNum = lastCallStack[1] #取得發生的行號
    funcName = lastCallStack[2] #取得發生的函數名稱
    errMsg = "Error \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
    return errMsg

def get_rpt_due_date(year, season):
    if season == 1:
        due_date = datetime.datetime(year, 5, 15)
    elif season == 2:
        due_date = datetime.datetime(year, 8, 14)
    elif season == 3:
        due_date = datetime.datetime(year, 11, 14)
    else:
        due_date = datetime.datetime(year+1, 3, 31)
    return due_date

#--------------------
#--------------------