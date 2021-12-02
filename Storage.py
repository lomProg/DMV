# -*- coding: utf-8 -*-
"""
@author: lomProg, enigarv
"""

import json
import pymongo
from pymongo import MongoClient
path_directory = "./DMV/"

client = MongoClient('mongodb://localhost:27027/')

#Creazione db per progetto
db = client.Progetto_DMV
coll_meteo = db.Stazioni_meteo
coll_aria = db.Stazioni_aria
tweets_meteo = db.Tweets_meteo
tweets_aria = db.Tweets_aria

#Popolamento collezione dati meteo
with open(path_directory + "StazioniMeteoDB.json") as f:
  data_meteo = json.load(f)

if isinstance(data_meteo, list): 
    coll_meteo.insert_many(data_meteo)
else: 
    coll_meteo.insert_one(data_meteo)
        
#Popolamento collezione dati aria
with open(path_directory + "StazioniAriaDB.json") as f:
  data_aria = json.load(f)
  
if isinstance(data_aria, list): 
    coll_aria.insert_many(data_aria)
else: 
    coll_aria.insert_one(data_aria)
        
#Popolamento collezione tweet meteo
with open(path_directory + "TweetsMeteoDB.json") as f:
  dati_tweets_meteo = json.load(f)
  
if isinstance(dati_tweets_meteo, list): 
    tweets_meteo.insert_many(dati_tweets_meteo)
else: 
    tweets_meteo.insert_one(dati_tweets_meteo)
        
#Popolamento collezione tweet aria
with open(path_directory + "TweetsAriaDB.json") as f:
  dati_tweets_aria = json.load(f)
  
if isinstance(dati_tweets_aria, list): 
    tweets_aria.insert_many(dati_tweets_aria)
else: 
    tweets_aria.insert_one(dati_tweets_aria)









