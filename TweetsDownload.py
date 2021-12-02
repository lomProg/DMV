# -*- coding: utf-8 -*-
"""
@author: lomProg, enigarv
"""


import snscrape.modules.twitter as sn
import pandas as pd
import numpy as np
import Preprocessing as pp
from datetime import datetime
path_directory = "./DMV/"

#Set parametri queries
query_inquinamento = 'smog OR inquinamento OR pm10 OR pm25 OR particelle \
OR (polveri AND sottili) OR polverisottili OR (effetto AND serra) OR \
effettoserra OR co2 OR ozono OR particolato OR emissioni OR \
riscaldamentoglobale OR pollution OR particulate OR greenhouseeffect OR \
climatechange OR emissions OR globalwarning \
lang:it since:2020-01-1 until:2020-12-31 '
query_meteo = 'vento OR pioggia OR neve OR sole OR caldo OR freddo OR \
ventilato OR nebbia OR umidità OR soleggiato OR nuvoloso OR temporale OR \
tempesta OR ghiaccio OR previsioni OR alluvione OR siccità OR afa OR estate \
OR autunno OR primavera OR inverno \
lang:it since:2020-01-1 until:2020-12-31 '
 
#Download tweets 
def download_tweet(query, provincia, geocode): 
    max = 2000
    tweets = []
    tweets_df = pd.DataFrame()
    for i,tweet in enumerate(sn.TwitterSearchScraper   
                         (query + geocode).get_items()):
      if i > max:
          break
      tweets.append([tweet.username, provincia, tweet.date, tweet.id,
                     tweet.content])    
      tweets_df = pd.DataFrame(tweets, columns =
                                ["user", "provincia","data", "id", "text"])
    return tweets_df

geo_mi = 'geocode:45.2801,9.1124,30km'
prov_mi = 'MI'
tweets_mi = download_tweet(query_inquinamento, prov_mi, geo_mi)
tweets_mi_meteo = download_tweet(query_meteo, prov_mi, geo_mi)
geo_lc_co = 'geocode:45.866998,9.40940,25km'
prov_lc_co = 'LC CO'
tweets_lc_co = download_tweet(query_inquinamento, prov_lc_co, geo_lc_co)
tweets_lc_co_meteo = download_tweet(query_meteo, prov_lc_co, geo_lc_co)
geo_so = 'geocode:46.17673024907,9.887353633272,20km'
prov_so = 'SO'
tweets_so = download_tweet(query_inquinamento, prov_so, geo_so)
tweets_so_meteo = download_tweet(query_meteo, prov_so, geo_so)
geo_va = 'geocode:45.82,8.83054,8km'
prov_va = 'VA'
tweets_va = download_tweet(query_inquinamento, prov_va, geo_va)
tweets_va_meteo = download_tweet(query_meteo, prov_va, geo_va)
geo_bs = 'geocode:45.56335275160,10.23728121273,30km'
prov_bs = 'BS'
tweets_bs = download_tweet(query_inquinamento, prov_bs, geo_bs)
tweets_bs_meteo = download_tweet(query_meteo, prov_bs, geo_bs)
geo_mn = 'geocode:45.1585726018,10.79128119649,15km'
prov_mn = 'MN'
tweets_mn = download_tweet(query_inquinamento, prov_mn, geo_mn)
tweets_mn_meteo = download_tweet(query_meteo, prov_mn, geo_mn)
geo_pv = 'geocode:45.1818070853,9.15931402809,25km'
prov_pv = 'PV'
tweets_pv = download_tweet(query_inquinamento, prov_pv, geo_pv)
tweets_pv_meteo = download_tweet(query_meteo, prov_pv, geo_pv)
geo_lo = 'geocode:45.30936960713907,9.504455573103158,30km'
prov_lo = 'LO'
tweets_lo = download_tweet(query_inquinamento, prov_lo, geo_lo)
tweets_lo_meteo = download_tweet(query_meteo, prov_lo, geo_lo)
geo_bg = 'geocode:45.698084637,9.6765354234,20km'
prov_bg = 'BG'
tweets_bg = download_tweet(query_inquinamento, prov_bg, geo_bg)
tweets_bg_meteo = download_tweet(query_meteo, prov_bg, geo_bg)
geo_cr = 'geocode:45.098766349089374,10.031554642109416,10km'
prov_cr = 'CR'
tweets_cr = download_tweet(query_inquinamento, prov_cr, geo_cr)
tweets_cr_meteo = download_tweet(query_meteo, prov_cr, geo_cr)
geo_son = 'geocode:46.33165162198914,10.327515942166363,20km'  
tweets_son = download_tweet(query_inquinamento, prov_so, geo_son) 
tweets_son_meteo = download_tweet(query_meteo, prov_so, geo_son)

#Creazione data frame
tweets_df = pd.DataFrame()
tweets_df = tweets_df.append([tweets_bg, tweets_bs, tweets_cr,
                        tweets_lo, tweets_mi, tweets_mn, tweets_pv,
                        tweets_so, tweets_va])
tweets_df = tweets_df.drop_duplicates('id')
tweets_df = tweets_df.sort_values(by = "data")
tweets_df = tweets_df.set_index(np.arange(len(tweets_df)))


tweets_df_meteo = pd.DataFrame()
tweets_df_meteo = tweets_df_meteo.append([tweets_bg_meteo, tweets_bs_meteo, 
                        tweets_cr_meteo, tweets_lo_meteo, tweets_mi_meteo,
                        tweets_mn_meteo, tweets_pv_meteo, tweets_lc_co_meteo,
                        tweets_so_meteo, tweets_va_meteo, tweets_son_meteo])
tweets_df_meteo = tweets_df_meteo.sort_values(by = "data")
tweets_df_meteo = tweets_df_meteo.drop_duplicates('id')
tweets_df_meteo = tweets_df_meteo.set_index(np.arange(len(tweets_df_meteo)))

def split_date_time(df):
    date_time = df["data"]
    data = []
    time = []
    for i in range(len(date_time)):
        temp = date_time.iloc[i].strftime('%d/%m/%Y %H:%M:%S').split()
        date_temp = datetime.strptime(temp[0], '%d/%m/%Y')
        data.append(date_temp.date())
        time.append(temp[1])
    df["data"] = data
    df["time"] = time
    return df
tweets_aria_df = split_date_time(tweets_df)
tweets_meteo_df = split_date_time(tweets_df_meteo)


#Creazione DB
tweets = []
for i in range(len(tweets_aria_df)):
    tweet = {}
    tweet['_id'] = tweets_aria_df['id'][i].item()
    tweet['text'] = tweets_aria_df['text'][i]
    tweet['data'] = tweets_aria_df['data'][i].strftime('%d/%m/%Y')
    tweet['ora'] = tweets_aria_df['time'][i]
    tweet['user'] = tweets_aria_df['user'][i]
    tweet['provincia'] = tweets_aria_df['provincia'][i]
    tweets.append(tweet)

tweets_meteo = []
for i in range(len(tweets_df_meteo)):
    tweet = {}
    tweet['_id'] = tweets_df_meteo['id'].iloc[i].item()
    tweet['text'] = tweets_df_meteo['text'].iloc[i]   
    tweet['data'] = tweets_df_meteo['data'][i].strftime('%d/%m/%Y')
    tweet['ora'] = tweets_df_meteo['time'][i]
    tweet['user'] = tweets_df_meteo['user'].iloc[i]
    tweet['provincia'] = tweets_df_meteo['provincia'].iloc[i]
    tweets_meteo.append(tweet)    
    

pp.saveData(tweets, path_directory, 'TweetsAriaDB', 'json')
pp.saveData(tweets_meteo, path_directory, 'TweetsMeteoDB', 'json')









