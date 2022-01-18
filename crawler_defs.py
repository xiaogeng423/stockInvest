#--------------------
# Purpose: crawlers
# Author: Lewis
# Date: 2022-01-10

#--------------------
import os, glob, sys
import time
import requests
from io import StringIO
import pandas as pd

#--------------------
def get_coInfo_bycate(mkt_type, cate='all'):
    """ 
    get company information by industry category
    ------------------------
    # input
    1. mkt_type: str, {'sii', 'otc', 'rotc', 'pub'}
    2. cate: num, code id of industry category
    ------------------------
    # output
    1. data.frame
    ------------------------
    # example
    input = {'cate': 5, 'mkt_type': 'otc'}
    locals().update(input)
    """
    payload = {
        'encodeURIComponent': '1',
        'step': '1',
        'firstin': '1',
        'TYPEK': str(mkt_type),
        'code': str('%02d' % cate),
    }
    url = 'https://mops.twse.com.tw/mops/web/ajax_t51sb01'
    res = requests.post(url , data = payload)
    dfs = pd.read_html(res.text)
    df = pd.concat([df for df in dfs if df.shape[1] > 1])
    return df

def get_co_id_name_type(path):
    #path = '/Users/lewis_1/Documents/SideProject/stockInvest/tab_coInfo/'
    co_infos = set()
    for file in glob.glob(path + 'coInfo*.csv'):
        df = pd.read_csv(file, index_col=0)
        co_infos.update(df[['公司代號', '公司簡稱', '產業類別']][df['公司代號'] != '公司代號'].apply(lambda x: '_'.join([str(e) for e in x]), axis=1).tolist())
    return co_infos

def get_income_byco(co_id, year, season, proxy=None):
    """ 
    get season income by company(detail)
    ------------------------
    # input
    1. co_id: num, code id of company
    2. year: num, yyyy or yy
    3. season: num, 1-4
    ------------------------
    # output
    1. data.frame
    ------------------------
    # example
    input = {'co_id': 2330, 'year': 2021, 'season': 3}
    locals().update(input)
    """
    if year > 1990:
        year -= 1911
    url = 'https://mops.twse.com.tw/mops/web/ajax_t164sb04'
    payload = {
        'encodeURIComponent': '1',
        'step': '1',
        'firstin': '1',
        'off': '1',
        'queryName': 'co_id',
        'inpuType': 'co_id',
        'TYPEK': 'all',
        'isnew': 'false',
        'co_id': str(co_id),
        'year': str(year),
        'season': str(season),
    }
    if proxy is None:
        res = requests.post(url , data = payload)
    else:
        res = requests.post(url , data = payload, proxies={"http": proxy})
    df = pd.read_html(res.text)[1]
    df.columns = df.columns.get_level_values(2)
    df.columns = [e if i == 0 else e + ':金額' if i % 2 == 1 else e + ':百分比' for i, e in enumerate(df.columns)]
    df.set_index(df.columns[0], inplace=True)
    df = df.transpose()
    tag = '項目'
    for k, v in df.iteritems():
        if v.isna().all():
            tag = k
        else:
            df.rename(columns = {k: tag + ':' + k}, inplace=True)
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    df.columns = [e.replace('（','(').replace('）',')') for e in df.columns]
    return df

def get_asset_byco(co_id, year, season):
    """ 
    get season asset by company(detail)
    ------------------------
    # input
    1. co_id: num, code id of company
    2. year: num, yyyy or yy
    3. season: num, 1-4
    ------------------------
    # output
    1. data.frame
    ------------------------
    # example
    input = {'co_id': 2330, 'year': 2021, 'season': 3}
    locals().update(input)
    """
    if year > 1990:
        year -= 1911
    url = 'https://mops.twse.com.tw/mops/web/ajax_t164sb03'
    payload = {
        'encodeURIComponent': '1',
        'step': '1',
        'firstin': '1',
        'off': '1',
        'queryName': 'co_id',
        'inpuType': 'co_id',
        'TYPEK': 'all',
        'isnew': 'false',
        'co_id': str(co_id),
        'year': str(year),
        'season': str(season),
    }
    res = requests.post(url , data = payload)
    df = pd.read_html(res.text)[1]
    #df.columns.map('|'.join).str.strip('|')
    df.columns = df.columns.get_level_values(2)
    df.columns = [e if i == 0 else e+':金額' if i % 2 == 1 else e+':百分比' for i, e in enumerate(df.columns)]
    df.set_index(df.columns[0], inplace=True)
    df = df.transpose()
    tag = '項目'
    for k, v in df.iteritems():
        if v.isna().all():
            tag = k
        else:
            df.rename(columns = {k: tag + ':' + k}, inplace=True)
        
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    df.columns = [e.replace('（','(').replace('）',')') for e in df.columns]
    return df

def get_revnue_all(mkt_type, year, month, co_abroad=False):
    """ 
    get monthly revenue
    ------------------------
    # input
    1. year: num, yyyy or yy
    2. month: num, m
    3. mkt_type: str, {'sii', 'otc', 'rotc', 'pub'}
    4. co_abroad: bool
    ------------------------
    # output
    1. data.frame
    ------------------------
    # example
    input = {'mkt_type': 'sii', 'co_abroad': False, 'year': 2021, 'month': 12}
    locals().update(input)
    """
    if year > 1990:
        year -= 1911
    url = 'https://mops.twse.com.tw/nas/t21/'+ str(mkt_type) +'/t21sc03_' + '_'.join([ str(e) for e in [year, month] ]) + '_' + str(int(co_abroad)) + '.html'
    # fake browser
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    # get url
    r = requests.get(url, headers=headers)
    r.encoding = 'big5'
    dfs = pd.read_html(StringIO(r.text))
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = df[list(range(0,10))]
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
    df = df[~df['當月營收'].isnull()]
    df = df[df['公司代號'] != '合計']
    return df

#--------------------
def get_price_day(date):
    r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + str(date).split(' ')[0].replace('-','') + '&type=ALL')
    df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
                                        for i in r.text.split('\n') 
                                        if len(i.split('",')) == 17 and i[0] != '='])), header=0)
    df['成交金額'] = df['成交金額'].str.replace(',','')
    df['成交股數'] = df['成交股數'].str.replace(',','')
    df.dropna(axis=1, how='all', inplace=True)
    return df

def get_income_all(year, season, mkt_type='sii'):
    """ 
    get season income(summary)
    ------------------------
    # input
    1. year: num, yyyy or yy
    2. season: num, 1-4
    3. mkt_type: str, {'sii', 'otc', 'rotc', 'pub'}
    ------------------------
    # output
    1. data.frame
    ------------------------
    # example
    input = {'year': 2021, 'season': 3}
    locals().update(input)
    """
    if year > 1990:
        year -= 1911
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    payload = {
        'encodeURIComponent': '1',
        'step': '1',
        'firstin': '1',
        'off': '1',
        'TYPEK': str(mkt_type),
        'year': str(year),
        'season': str(season),
    }
    res = requests.post(url , data = payload)
    dfs = pd.read_html(res.text)
    df = pd.concat([df for df in dfs if df.shape[1] > 1])
    df['date'] = str(year+1911) + '-' + str(season)
    return df

def get_asset_all(year, season, mkt_type='sii'):
    """ 
    get season asset(summary)
    ------------------------
    # input
    1. year: num, yyyy or yy
    2. season: num, 1-4
    3. mkt_type: str, {'sii', 'otc', 'rotc', 'pub'}
    ------------------------
    # output
    1. data.frame
    ------------------------
    # example
    input = {'year': 2021, 'season': 3}
    locals().update(input)
    """
    if year > 1990:
        year -= 1911
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    payload = {
        'encodeURIComponent': '1',
        'step': '1',
        'firstin': '1',
        'off': '1',
        'isQuery': 'Y',
        'TYPEK': str(mkt_type),
        'year': str(year),
        'season': str(season),
    }
    res = requests.post(url , data = payload)
    dfs = pd.read_html(res.text)
    df = pd.concat([df for df in dfs if df.shape[1] > 1])
    df['date'] = str(year+1911) + '-' + str(season)
    return df


#--------------------