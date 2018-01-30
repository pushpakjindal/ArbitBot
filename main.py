import pandas as pd
import numpy as np
import urllib as ul
import re
import requests
from lxml import html
import csv
import datetime

pd.set_option('display.float_format', lambda x: '%.5f' % x)

######################-------APIs----------######################

#Koinex

def koinex():
    url = 'https://koinex.in/api/ticker'

    try:
        reply = requests.get(url)
        column = ['coin','currency','exchange','ask','bid','last']
        temp_df = pd.DataFrame(data=None, index=None, columns=column)
        i = 0
        for key in koinex_reply.json()['stats']:
            x = [key.lower(),\
                 'inr',\
                 'koinex',\
                 float(koinex_reply.json()['stats'][key]['lowest_ask']),\
                 float(koinex_reply.json()['stats'][key]['highest_bid']),\
                 float(koinex_reply.json()['stats'][key]['last_traded_price'])]
            temp_df.loc[i] = x
            i = i+1
        return temp_df
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        return ("Koinex_Connection_Error")

#coinome

def coinome():

    coinome_url = 'https://www.coinome.com/exchange'
    api_status = ''

    try:
        reply = requests.get(coinome_url)
        column = ['coin','currency','exchange','ask','bid','last']
        i=0
        temp_df = pd.DataFrame(data=None, index=None, columns=column)

        if reply.status_code == 200:
            tree = html.fromstring(reply.content)
            btc = tree.xpath('/html/body/div[1]/nav/div[2]/div/div/div/div[1]/a/span[2]/span/text()')
            bch = tree.xpath('/html/body/div[1]/nav/div[2]/div/div/div/div[2]/a/span[2]/span/text()')
            ltc = tree.xpath('/html/body/div[1]/nav/div[2]/div/div/div/div[3]/a/span[2]/span/text()')
            dash = tree.xpath('/html/body/div[1]/nav/div[2]/div/div/div/div[4]/a/span[2]/span/text()')

            BTC_coinome = int(float(btc[0].replace(',','')))
            BCH_coinome = int(float(bch[0].replace(',','')))
            LTC_coinome = int(float(ltc[0].replace(',','')))
            DASH_coinome = int(float(dash[0].replace(',','')))

            temp_df.loc[0] = ['btc', 'inr', 'coinome',BTC_coinome, BTC_coinome, BTC_coinome]
            temp_df.loc[1] = ['bch', 'inr', 'coinome',BCH_coinome, BCH_coinome, BCH_coinome,]
            temp_df.loc[2] = ['ltc', 'inr', 'coinome',LTC_coinome, LTC_coinome, LTC_coinome,]
            temp_df.loc[3] = ['dash', 'inr', 'coinome',DASH_coinome, DASH_coinome, DASH_coinome]

        else:
            #api_status = 'api_down'
            coinome = ('','','','')
        return temp_df
    except requests.exceptions.RequestException as e:
        return ("Coinome_Connection_Error")

#Coindelta

def coindelta():
    url = 'https://coindelta.com/api/v1/public/getticker/'

    try:
        reply = requests.get(url)
        d = pd.DataFrame(reply.json())
        d.columns = map(str.lower, d.columns)
        d1 = d.marketname.str.split('-',1, expand=True).rename(columns = {0:'coin',1:'currency'})
        d1.insert(2,column = 'exchange',value='coindelta')
        d = d1.join(d)
        del d['marketname']
        return d
    except requests.exceptions.RequestException as e:
        return ("Coindelta_Connection_Error")

#Fetch_data

def fetch_data():
    print ('Fetching_data')
    flag = 0
    column = ['coin','currency','exchange','ask','bid','last']

    data = pd.DataFrame(data=None, columns=column, index=None)

    d = koinex()
    if isinstance(d, pd.DataFrame):
        data = data.append(d)
        flag = 1
    else:
        print (d)

    d = coinome()
    if isinstance(d, pd.DataFrame):
        data = data.append(d)
        flag = 1
    else:
        print (d)

    d = coindelta()
    if isinstance(d, pd.DataFrame):
        data = data.append(d)
        flag = 1
    else:
        print (d)

    if flag == 1:
        return data
    else:
        return ("No_Data_fetched")

#Transfer Formula

def arb1(inr, coin1, exchange1, coin2, exchange2, rates):

    rate = rates
    rate = rate.set_index(['coin','exchange','currency'])
    fees = pd.read_csv('G:Arbit/fees_v2.csv', index_col=[0,1])
    inr = inr

    coin_ex1 = inr*(1-fees.loc[(coin1,exchange1),'buy'])/rate.loc[(coin1, exchange1,'inr'),'ask']
    coin_ex2 = coin_ex1 - fees.loc[(coin1,exchange1),'with']
    inr_ex2 = coin_ex2*(1-fees.loc[(coin2,exchange2),'sell'])*rate.loc[(coin1, exchange2, 'inr'),'bid']
    return (inr_ex2)

#Looping

def calc(principal, data):

    out = pd.DataFrame(data=None, columns=['coin','exchange1', 'exchange2', 'profit'], index=None)
    amount = principal

    if isinstance(data, pd.DataFrame):
        rates = data
        fees = pd.read_csv('G:Arbit/fees_v2.csv', index_col=[0,1])
        coin_list = list(fees.index.values)
        for i in range(0, len(coin_list)):
            t1 = coin_list[i]
            for j in range(0, len(coin_list)):
                t2 = coin_list[j]
                if (t1[0] == t2[0]) & (t1[1] != t2[1]):
                    churn = arb1(amount,t1[0], t1[1], t2[0], t2[1], rates)
                    profit = str(round(((churn-amount)/amount)*100,1))
                    out = pd.concat([out,pd.DataFrame(data=[[t1[0] , t1[1], t2[1], profit]], columns=['coin','exchange1', 'exchange2', 'profit'])], axis=0)
        return out
    else:
        return data

while True:
    calc(1000000, fetch_data())
