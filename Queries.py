# -*- coding: utf-8 -*-
"""
@author: lomProg, enigarv
"""


import pandas as pd
import Preprocessing as pp
from pymongo import MongoClient
path_directory = "./DMV/"

client = MongoClient('mongodb://localhost:27027/')

#Creazione db per progetto
db = client.Progetto_DMV
coll_meteo = db.Stazioni_meteo
coll_aria = db.Stazioni_aria
tweets_aria = db.Tweets_aria
tweets_meteo = db.Tweets_meteo

# =============================================================================
# Elenco query
# =============================================================================
#   
#   query1_(pullate) = max valore per provincia di inquinante 'pullate'
#   query2_(tipoMeteo)_(pullate) = valore meteo nel giorno max valore
#                                  di pullate
#   query3_(tipoMeteo) = max valore di tipoMeteo
#   query4_(tipoMeteo) = min valore tipoMeteo
#   query5 = ricerca tweet in data con max valori atmosferici
#   query6_(pullate) = valori nel corso dell'anno per 'pullate'
#   query7_(tipoMeteo) = valori nel corso dell'anno per tipoMeteo
#   query8 = sensori che superano soglia legge annuale
#   query9 = sensori che superano soglia legge giornaliera
#   query10 = conta occorenze parole inquinanti nei tweets
#   query11 = città con più tweets, topic inquinamento
#   query12 = scarica tutti i dati precipitazione nel corso dell'anno in
#             tutta Lombardia
#
# =============================================================================

# =============================================================================
# ####    query1_     ####
# =============================================================================
prov = ['MI', 'MB', 'CO', 'LC', 'VA', 'SO', 'BG', 'BS', 'PV', 'CR', 'LO', 'MN']
pollutant = ['Benzene', 'Ozono', 'Particelle sospese PM2.5','PM10 (SM2005)',
             'Ossidi di Azoto','Monossido di Carbonio','Biossido di Zolfo']
staz_aria = pd.DataFrame(index = prov, columns = pollutant)
c6h6_staz = [705, 0, 561, 574, 0, 569, 584, 652, 642, 627, 600, 664]
o3_staz = [705, 674, 561, 706, 552, 1264, 583, 669, 642, 677, 1265, 670]
pm25_staz = [705, 674, 561, 706, 560, 1264, 583, 649, 642, 627, 1265, 670]
pm10_staz = [705, 1374, 561, 574, 560, 569, 584, 669, 643, 677, 600, 670]
nox_staz = [705, 1374, 561, 574, 560, 1264, 584, 652, 642, 627, 1265, 670]
co_staz = [501, 674, 561, 574, 560, 569, 584, 652, 643, 677, 600, 670]  
so2_staz = [705, 674, 561, 574, 552, 569, 584, 669,642, 677, 600, 663]  
staz_aria['Benzene'] = c6h6_staz
staz_aria['Ozono'] = o3_staz
staz_aria['Particelle sospese PM2.5'] = pm25_staz
staz_aria['PM10 (SM2005)'] = pm10_staz
staz_aria['Ossidi di Azoto'] = nox_staz
staz_aria['Monossido di Carbonio'] = co_staz
staz_aria['Biossido di Zolfo'] = so2_staz

#Lista sensori aria con corrispondente valore massimo
def findValMaxPull(dfProv):
    max_by_city = []
    for j in range(len(dfProv.columns)):
        temp = []
        for i in range(len(dfProv)):
            q = coll_aria.aggregate([
            {
                '$match': {
                    'provincia': dfProv.index[i], 
                    'sensori.tipo': dfProv.columns[j]
                }
            }, {
                '$unwind': {
                    'path': '$sensori', 
                    'includeArrayIndex': 'nSensore'
                }
            }, {
                '$unwind': {
                    'path': '$sensori.valori', 
                    'includeArrayIndex': 'nValore'
                }
            }, {
                '$match': {
                    'sensori.tipo': dfProv.columns[j], 
                    '_id': dfProv.iloc[i,j].item()
                }
            }, {
                '$sort': {
                    'sensori.valori.valore_medio': -1
                }
            }, {
                '$limit': 1
            }, {
                '$project': {
                    'provincia':1, 
                    'sensori.tipo':1, 
                    'sensori.valore_annuo':1, 
                    'sensori.valori.data':1, 
                    'sensori.valori.valore_medio':1
                }
            }
                ])
            q = list(q)
            temp = temp + q
        max_by_city.append(temp)
    return max_by_city

query1_c6h6 = findValMaxPull(staz_aria)[0]
query1_o3 = findValMaxPull(staz_aria)[1]
query1_pm25 = findValMaxPull(staz_aria)[2]
query1_pm10 = findValMaxPull(staz_aria)[3]
query1_nox = findValMaxPull(staz_aria)[4]
query1_co = findValMaxPull(staz_aria)[5]
query1_so2 = findValMaxPull(staz_aria)[6]

#Liste con province presenti e corrispondente data valore massimo
def dataMaxProv(q):
    prov = []
    data = []
    for i in range(len(q)):
        prov_temp = q[i].get('provincia')
        data_temp = q[i].get('sensori').get('valori').get('data')
        prov.append(prov_temp)
        data.append(data_temp)
    return prov, data

c6h6_max = dataMaxProv(query1_c6h6)
o3_max = dataMaxProv(query1_o3)
pm25_max = dataMaxProv(query1_pm25)
pm10_max = dataMaxProv(query1_pm10)
nox_max = dataMaxProv(query1_nox)
co_max = dataMaxProv(query1_co)
so2_max = dataMaxProv(query1_so2)

# =============================================================================
# ####    query2_     ####
# =============================================================================
agents = ["Direzione Vento", "Precipitazione", "Temperatura",
          "Umidità Relativa", "Velocità Vento", "Radiazione Globale",
          "Altezza Neve", "Livello Idrometrico"]
staz_meteo = pd.DataFrame(index = prov, columns = agents)
dir_vento_staz = [502, 0, 646, 706, 1898, 107, 594, 653, 817, 677, 123, 166]
pioggia_staz = [502, 1510, 646, 706, 1898, 107, 594, 653, 817, 677, 123, 166]
temperatura_staz = [502, 903, 646, 706, 1898, 107, 594, 653, 817, 677, 123,166] 
umidita_staz = [502, 0, 646, 706, 1898, 107, 594, 653, 642, 677, 123, 166]    
vel_vento_staz = [502, 0, 646, 706, 1898, 107, 594, 653, 817, 677, 123, 166]
rad_glob = [502, 0, 141, 706, 559, 107, 594, 134, 817, 677, 123, 166]
alt_neve = [0, 0, 1545, 1347, 1382, 833, 57, 1220, 857, 0, 0, 0]
liv_idro = [869, 868, 870, 880, 859, 9, 872, 1555, 817, 887, 862, 816]
staz_meteo['Direzione Vento'] = dir_vento_staz
staz_meteo['Precipitazione'] = pioggia_staz
staz_meteo['Temperatura'] = temperatura_staz
staz_meteo['Umidità Relativa'] = umidita_staz
staz_meteo['Velocità Vento'] = vel_vento_staz
staz_meteo['Radiazione Globale'] = rad_glob
staz_meteo['Altezza Neve'] = alt_neve
staz_meteo['Livello Idrometrico'] = liv_idro

#Lista sensori meteo nel giorno di valore massimo inquinante
def findAgentInMax(dfProv, tuplaMax):
    agent_by_city = []
    data_max_pull = tuplaMax[1]
    for j in range(len(dfProv.columns)):
        if j == 1:
            val1 = 'sensori.valori.valore'
            val2 = 'sensori.unitaMisura'
        elif j == 2 or j == 3:
            val1 = 'sensori.valori.valore_max'
            val2 = 'sensori.valori.valore_min'
        elif j == 0 or j == 4:
            val1 = 'sensori.valori.valore_medio'
            val2 = 'sensori.valori.dev_st'
        temp = []
        i = 0
        while i < len(dfProv):
            k = 0
            if dfProv.iloc[i,j] == 0:
                i = i + 1
            else:
                while (k < len(tuplaMax[0]) and
                       tuplaMax[0][k] != dfProv.index[i]):
                    k = k + 1
                    if k == len(tuplaMax[0]) - 1:
                        break
            q = coll_meteo.aggregate([
                    {
                        '$match': {
                            'provincia': dfProv.index[i], 
                            'sensori.tipo': dfProv.columns[j]
                        }
                    }, {
                        '$unwind': {
                            'path': '$sensori', 
                            'includeArrayIndex': 'nSensore'
                        }
                    }, {
                        '$unwind': {
                            'path': '$sensori.valori', 
                            'includeArrayIndex': 'nValore'
                        }
                    }, {
                        '$match': {
                            'sensori.tipo': dfProv.columns[j], 
                            '_id': dfProv.iloc[i,j].item(),
                            'sensori.valori.data': data_max_pull[k]
                        }
                    }, {
                        '$project': {
                            'provincia':1,
                            'sensori.tipo':1,
                            'sensori.valori.data':1,
                            #'sensori.valori.valore_medio':1
                            val1: 1,
                            val2: 1
                        }
                    }
                    ])
            q = list(q)
            temp = temp + q
            i = i + 1
        agent_by_city.append(temp)
    return agent_by_city

agents_c6h6 = findAgentInMax(staz_meteo, c6h6_max)
agents_o3 = findAgentInMax(staz_meteo, o3_max)
agents_pm25 = findAgentInMax(staz_meteo, pm25_max)
agents_pm10 = findAgentInMax(staz_meteo, pm10_max)
agents_nox = findAgentInMax(staz_meteo, nox_max)
agents_co = findAgentInMax(staz_meteo, co_max)
agents_so2 = findAgentInMax(staz_meteo, so2_max)

query2_dir_vento_c6h6 = agents_c6h6[0]
query2_dir_vento_o3 = agents_o3[0]
query2_dir_vento_pm25 = agents_pm25[0]
query2_dir_vento_pm10 = agents_pm10[0]
query2_dir_vento_nox = agents_nox[0]
query2_dir_vento_co = agents_co[0]
query2_dir_vento_so2 = agents_so2[0]
# ==== #
query2_prec_c6h6 = agents_c6h6[1]
query2_prec_o3 = agents_o3[1]
query2_prec_pm25 = agents_pm25[1]
query2_prec_pm10 = agents_pm10[1]
query2_prec_nox = agents_nox[1]
query2_prec_co = agents_co[1]
query2_prec_so2 = agents_so2[1]
# ==== #
query2_tempr_c6h6 = agents_c6h6[2]
query2_tempr_o3 = agents_o3[2]
query2_tempr_pm25 = agents_pm25[2]
query2_tempr_pm10 = agents_pm10[2]
query2_tempr_nox = agents_nox[2]
query2_tempr_co = agents_co[2]
query2_tempr_so2 = agents_so2[2]
# ==== #
query2_umid_c6h6 = agents_c6h6[3]
query2_umid_o3 = agents_o3[3]
query2_umid_pm25 = agents_pm25[3]
query2_umid_pm10 = agents_pm10[3]
query2_umid_nox = agents_nox[3]
query2_umid_co = agents_co[3]
query2_umid_so2 = agents_so2[3]
# ==== #
query2_vel_vento_c6h6 = agents_c6h6[4]
query2_vel_vento_o3 = agents_o3[4]
query2_vel_vento_pm25 = agents_pm25[4]
query2_vel_vento_pm10 = agents_pm10[4]
query2_vel_vento_nox = agents_nox[4]
query2_vel_vento_co = agents_co[4]
query2_vel_vento_so2 = agents_so2[4]

# =============================================================================
# ####    query3_, query4_    ####
# =============================================================================
#Lista sensori meteo con corrispondente valore massimo
def findValMaxMinAgent(dfProv):
    max_by_city = []
    min_by_city = []
    for j in range(len(dfProv.columns)):
        if j == 1:
            val1 = 'sensori.valori.valore'
            val2 = 'sensori.valori.valore'
        elif j == 2 or j == 3:
            val1 = 'sensori.valori.valore_max'
            val2 = 'sensori.valori.valore_min'
        elif j == 0 or j == 4:
            val1 = 'sensori.valori.valore_medio'
            val2 = 'sensori.valori.valore_medio'
        temp_max = []
        temp_min = []
        for i in range(len(dfProv)):
            q1 = coll_meteo.aggregate([
            {
                '$match': {
                    'provincia': dfProv.index[i], 
                    'sensori.tipo': dfProv.columns[j]
                }
            }, {
                '$unwind': {
                    'path': '$sensori', 
                    'includeArrayIndex': 'nSensore'
                }
            }, {
                '$unwind': {
                    'path': '$sensori.valori', 
                    'includeArrayIndex': 'nValore'
                }
            }, {
                '$match': {
                    'sensori.tipo': dfProv.columns[j], 
                    '_id': dfProv.iloc[i,j].item()
                }
            }, {
                '$sort': {
                    val1: -1
                }
            }, {
                '$limit': 1
            }, {
                '$project': {
                    'provincia':1, 
                    'sensori.tipo':1, 
                    'sensori.valore_annuo':1, 
                    'sensori.valori.data':1, 
                    val1:1
                }
            }
                ])
            q1 = list(q1)
            temp_max = temp_max + q1
            q2 = coll_meteo.aggregate([
            {
                '$match': {
                    'provincia': dfProv.index[i], 
                    'sensori.tipo': dfProv.columns[j]
                }
            }, {
                '$unwind': {
                    'path': '$sensori', 
                    'includeArrayIndex': 'nSensore'
                }
            }, {
                '$unwind': {
                    'path': '$sensori.valori', 
                    'includeArrayIndex': 'nValore'
                }
            }, {
                '$match': {
                    'sensori.tipo': dfProv.columns[j], 
                    '_id': dfProv.iloc[i,j].item()
                }
            }, {
                '$sort': {
                    val2: 1
                }
            }, {
                '$limit': 1
            }, {
                '$project': {
                    'provincia':1, 
                    'sensori.tipo':1, 
                    'sensori.valore_annuo':1, 
                    'sensori.valori.data':1, 
                    val2:1
                }
            }
                ])
            q2 = list(q2)
            temp_min = temp_min + q2
        max_by_city.append(temp_max)
        min_by_city.append(temp_min)
    return max_by_city, min_by_city

agentsMaxMin = findValMaxMinAgent(staz_meteo)

query3_dir_vento = agentsMaxMin[0][0]
query3_prec = agentsMaxMin[0][1]
query3_tempr = agentsMaxMin[0][2]
query3_umid = agentsMaxMin[0][3]
query3_vel_vento = agentsMaxMin[0][4]
query3_rad_glo = agentsMaxMin[0][5]
query3_neve = agentsMaxMin[0][6]
query3_liv_idro = agentsMaxMin[0][7]
# ==== #
query4_dir_vento = agentsMaxMin[1][0]
query4_prec = agentsMaxMin[1][1]
query4_tempr = agentsMaxMin[1][2]
query4_umid = agentsMaxMin[1][3]
query4_vel_vento = agentsMaxMin[1][4]
query4_rad_glo = agentsMaxMin[1][5]
query4_neve = agentsMaxMin[1][6]
query4_liv_idro = agentsMaxMin[1][7]

# =============================================================================
# Analisi dei tweet
# =============================================================================
# =============================================================================
# ####    query5_     ####
# =============================================================================
recap = []
tipo = ['Temperatura', 'Precipitazione', 'Umidita', 'Vel_vento']
tempr_max = dataMaxProv(query3_tempr)
pioggia_max = dataMaxProv(query3_prec)
umidita_max = dataMaxProv(query3_umid)
vel_vento_max = dataMaxProv(query3_vel_vento)
recap.append(tempr_max)# + pioggia_max
recap.append(pioggia_max)
recap.append(umidita_max)
recap.append(vel_vento_max)
flag_max = 0

recap_min = []
tempr_min = dataMaxProv(query4_tempr)
pioggia_min = dataMaxProv(query4_prec)
umidita_min = dataMaxProv(query4_umid)
vel_vento_min = dataMaxProv(query4_vel_vento)
recap_min.append(tempr_min)# + pioggia_min
recap_min.append(pioggia_min)
recap_min.append(umidita_min)
recap_min.append(vel_vento_min)
flag_min = 1

def findTweetPerMaxMinAgent(recap):
    df = pd.DataFrame(columns = ['tipo', 'data', 'prov', 'text'])
    c = 0
    for j in range(len(recap)):      
        for i in range(len((recap[j])[0])):
            prov = (recap[j])[0][i] 
            data = (recap[j])[1][i] 
            out = tweets_meteo.find({'provincia':prov, 'data':data},
                                    {'text':1, 'data':1, '_id':0})
            if not(out is None):
                out = list(out)
                if len(out) > 1:
                    for r in range(len(out)):
                        row = [tipo[j], out[r].get('data'), prov, 
                               out[r].get('text')]
                        df.loc[c] = row
                        c = c + 1
                elif len(out) != 0:
                    row = [tipo[j], out[0].get('data'), prov,
                           out[0].get('text')]
                    df.loc[c] = row
                    c = c + 1 
    return df

query5_max = findTweetPerMaxMinAgent(recap)
query5_min = findTweetPerMaxMinAgent(recap_min)

def cleanDf(df, flag):
    df_clean = pd.DataFrame(columns = ['tipo','data', 'prov', 'text'])
    c = 0
    for d in range(len(df)):
        tweet = df['text'][d]
        words = tweet.split()
        if flag == 0:
            for i in range(len(words)):
                if df['tipo'][d] == 'Precipitazione':
                    if words[i] in ['precipitazioni', 'pioggia', 'temporale',
                                    'tempesta']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row
                        c = c + 1
                elif df['tipo'][d] == 'Temperatura':
                    if words[i] in ['caldo', 'afa', 'temperatura']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row    
                        c = c + 1
                elif df['tipo'][d] == 'Umidita':
                    if words[i] in ['umido', 'pioggia', 'umidità']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row
                        c = c + 1
                elif df['tipo'][d] == 'Vel_vento':
                    if words[i] in ['vento', 'bufera', 'raffiche','folate']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row
                        c = c + 1
        elif flag == 1:
            for i in range(len(words)):
                if df['tipo'][d] == 'Precipitazione':
                    if words[i] in ['precipitazioni', 'pioggia',
                                    'temporale','tempesta']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row
                        c = c + 1
                elif df['tipo'][d] == 'Temperatura':
                    if words[i] in ['freddo', 'gelo', 'temperatura',
                                    'ghiaccio']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row    
                        c = c + 1
                elif df['tipo'][d] == 'Umidita':
                    if words[i] in ['umido', 'pioggia', 'umidità']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row
                        c = c + 1
                elif df['tipo'][d] == 'Vel_vento':
                    if words[i] in ['vento', 'bufera', 'raffiche','folate']:
                        row = [df['tipo'][d], df['data'][d], df['prov'][d],
                               df['text'][d]]
                        df_clean.loc[c] = row
                        c = c + 1
    return df_clean

query5_max_clean = cleanDf(query5_max, flag_max)
query5_min_clean = cleanDf(query5_min, flag_min)      

# =============================================================================
# ####    query6_     ####
# =============================================================================
city = ['Milano', 'Monza', 'Como', 'Lecco', 'Varese', 'Sondrio', 'Bergamo',
          'Brescia', 'Pavia', 'Cremona', 'Lodi', 'Mantova']
staz_tipo = pd.DataFrame(index = city, columns = pollutant)
staz_tipo['Benzene'] = c6h6_staz
staz_tipo['Ozono'] = o3_staz
staz_tipo['Particelle sospese PM2.5'] = pm25_staz
staz_tipo['PM10 (SM2005)'] = pm10_staz
staz_tipo['Ossidi di Azoto'] = nox_staz
staz_tipo['Monossido di Carbonio'] = co_staz
staz_tipo['Biossido di Zolfo'] = so2_staz

#Lista valori sensori aria con valori annuali
def findPollByCity(dfProv):
    poll_by_city = []
    for r in range(len(dfProv.columns)):
        temp = []
        for i in range(len(dfProv)):
            res = coll_aria.aggregate([
            {
                '$match': {
                    'comune': dfProv.index[i], 
                    'sensori.tipo': dfProv.columns[r]
                }
            }, {
                '$unwind': {
                    'path': '$sensori', 
                    'includeArrayIndex': 'nSensore'
                }
            }, {
                '$unwind': {
                    'path': '$sensori.valori', 
                    'includeArrayIndex': 'nValore'
                }
            }, {
                '$match': {
                    'sensori.tipo': dfProv.columns[r],
                    '_id': dfProv.iloc[i,r].item()
                }
            },
                {
                    '$project': {
                        'provincia':1,
                        'sensori.tipo':1,
                        'sensori.valori':1,
                        'lat': 1,
                        'lng': 1
                        }}
                ])
            res = list(res)
            temp = temp + res
        poll_by_city.append(temp)        
    return poll_by_city

poll_city = findPollByCity(staz_tipo)
query6_c6h6 = poll_city[0]
query6_o3 = poll_city[1]
query6_PM25 = poll_city[2]
query6_PM10_SM2005 = poll_city[3]
query6_nox = poll_city[4]
query6_co = poll_city[5]
query6_so2 = poll_city[6]

# Costruzione df per dati aria
def buildDf(qa):
    col = ['id', 'prov', 'lat', 'lng', 'tipo_aria', 'data', 'val', 'std']       
    df = pd.DataFrame(columns = col)   
    c = 0
    for i in range(len(qa)):
        temp = qa[i]        
        prov_temp = temp['provincia']
        id_temp = temp['_id']
        sensore_temp = temp['sensori']        
        data_temp = sensore_temp['valori'].get('data')
        tipo_temp = sensore_temp['tipo']
        val_medio_temp = str(sensore_temp['valori'].get('valore_medio'))
        sd_temp = sensore_temp['valori'].get('dev_st')
        lat_temp = temp['lat']
        lng_temp = temp['lng']
        val = [id_temp, prov_temp, lat_temp, lng_temp, tipo_temp, data_temp,
               val_medio_temp, sd_temp]
        df.loc[c] = val
        c = c + 1
    return df        
     
c6h6 = buildDf(query6_c6h6)
o3 = buildDf(query6_o3)
PM25 = buildDf(query6_PM25)
PM10_SM2005 = buildDf(query6_PM10_SM2005)
nox = buildDf(query6_nox)
co = buildDf(query6_co)
so2 = buildDf(query6_so2)

# =============================================================================
# ####    query7_     ####
# =============================================================================
staz_tipo_meteo = pd.DataFrame(index = prov, columns = agents)
staz_tipo_meteo['Direzione Vento'] =  dir_vento_staz 
staz_tipo_meteo['Precipitazione'] =  pioggia_staz
staz_tipo_meteo['Temperatura'] =  temperatura_staz
staz_tipo_meteo['Umidità Relativa'] =  umidita_staz
staz_tipo_meteo['Velocità Vento'] =  vel_vento_staz
staz_tipo_meteo['Radiazione Globale'] = rad_glob
staz_tipo_meteo['Altezza Neve'] = alt_neve
staz_tipo_meteo['Livello Idrometrico'] = liv_idro

#Lista valori sensori meteo nel corso dell'anno
def findMeteoByCity(dfProv):
    meteo_by_city = []
    for r in range(len(dfProv.columns)):
        temp = []
        for i in range(len(dfProv)):
            res = coll_meteo.aggregate([
            {
                '$match': {
                    'provincia': dfProv.index[i], 
                    'sensori.tipo': dfProv.columns[r]
                }
            }, {
                '$unwind': {
                    'path': '$sensori', 
                    'includeArrayIndex': 'nSensore'
                }
            }, {
                '$unwind': {
                    'path': '$sensori.valori', 
                    'includeArrayIndex': 'nValore'
                }
            }, {
                '$match': {
                    'sensori.tipo': dfProv.columns[r],
                    '_id': dfProv.iloc[i,r].item()
                }
            },
                {
                    '$project': {
                        'provincia':1,
                        'sensori.tipo':1,
                        'sensori.valori':1,
                        'lat': 1,
                        'lng': 1
                        }}
                ])
            res = list(res)
            temp = temp + res
        meteo_by_city.append(temp)        
    return meteo_by_city
     
meteo_city = findMeteoByCity(staz_meteo)
query7_dir_vento = meteo_city[0]
query7_prec = meteo_city[1]
query7_tempr = meteo_city[2]
query7_umid = meteo_city[3]
query7_vel_vento = meteo_city[4]
query7_rad_glob = meteo_city[5]
query7_alt_neve = meteo_city[6]
query7_liv_idro = meteo_city[7]

def buildDfMeteo(qm):
    
    opt = 0
    
    if qm[1].get('sensori').get('tipo') in ['Precipitazione']:
        col = ['id', 'prov', 'lat', 'lng', 'tipo_meteo', 'data', 'val']        
        opt = 1
        
    elif qm[1].get('sensori').get('tipo') in ['Temperatura',
                                              'Umidità Relativa',
                                              'Livello Idrometrico']:
        col = ['id', 'prov', 'lat', 'lng', 'tipo_meteo', 'data', 'val_max',
               'val_min']
        opt = 2
        
    else:        
        col = ['id', 'prov', 'lat', 'lng', 'tipo_meteo', 'data', 'val', 'std']
        
    df = pd.DataFrame(columns = col)   
    c = 0
    for i in range(len(qm)):
        
        temp = qm[i]
        prov_temp = temp['provincia']
        id_temp = temp['_id']
        sensore_temp = temp['sensori']                
        data_temp = sensore_temp['valori'].get('data')
        tipo_temp = sensore_temp['tipo']
        lat_temp = temp['lat']
        lng_temp = temp['lng']
        
        if opt == 0:
            val_medio_temp = sensore_temp['valori'].get('valore_medio')
            sd_temp = sensore_temp['valori'].get('dev_st')
            val = [id_temp, prov_temp, lat_temp, lng_temp, tipo_temp,
                   data_temp, val_medio_temp, sd_temp]
            df.loc[c] = val
            c = c + 1
        elif opt == 2:
            val_max = sensore_temp['valori'].get('valore_max')
            val_min = sensore_temp['valori'].get('valore_min')
            val = [id_temp, prov_temp, lat_temp, lng_temp, tipo_temp,
                   data_temp, val_max, val_min]
            df.loc[c] = val
            c = c + 1
        else:
            val_cml = sensore_temp['valori'].get('valore')
            val = [id_temp, prov_temp, lat_temp, lng_temp, tipo_temp,
                   data_temp, val_cml]
            df.loc[c] = val
            c = c + 1
            
    return df

d_df = buildDfMeteo(query7_dir_vento) 
p_df = buildDfMeteo(query7_prec)
t_df = buildDfMeteo(query7_tempr)
u_df = buildDfMeteo(query7_umid)
v_df = buildDfMeteo(query7_vel_vento)
r_df = buildDfMeteo(query7_rad_glob)
a_df = buildDfMeteo(query7_alt_neve)
l_df = buildDfMeteo(query7_liv_idro)

pp.saveData(p_df, path_directory, 'precipitazione_df')
pp.saveData(u_df, path_directory, 'umidita_df')
pp.saveData(v_df, path_directory, 'vel_vento_df')
pp.saveData(t_df, path_directory, 'temperatura_df')
pp.saveData(r_df, path_directory, 'rad_glob')
pp.saveData(a_df, path_directory, 'alt_neve')
pp.saveData(l_df, path_directory, 'liv_idro')

# =============================================================================
# ####    query8_     ####
# =============================================================================
sensori_con_annuo = ['PM10 (SM2005)', 'Particelle sospese PM2.5', 
                     'Ossidi di Azoto','Biossido di Zolfo', 'Benzene', 
                     'Benzo(a)pirene','Arsenico', 'Cadmio', 'Nikel', 'Piombo']
valori = [40, 25, 40, 20, 5, 1, 5, 6, 20, 0.5]
soglie_annue = []
for i in range(len(city)):
    sensori = [city[i]]
    for j in range(len(sensori_con_annuo)):
        q = [sensori_con_annuo[j]]
        q = q + (list(coll_aria.aggregate([
            {
                '$match': {
                    'comune': city[i], 
                    'sensori.tipo': sensori_con_annuo[j]
                }
            }, {
                '$unwind': {
                    'path': '$sensori', 
                    'includeArrayIndex': 'nSensore'
                }
            }, {
                '$match': {
                    'sensori.tipo': sensori_con_annuo[j], 
                    'sensori.valore_annuo': {
                        '$gt': valori[j]
                    }
                }
            }, {
                '$project': {
                    'sensori.valore_annuo': 1
                }
            }
        ])))
        sensori.append(q)      
    soglie_annue.append(sensori)

query8 = soglie_annue

# =============================================================================
# ####    query9_     ####
# =============================================================================
sensori_daily = ['PM10 (SM2005)', 'Ozono', 'Monossido di Carbonio', 
                 'Biossido di Zolfo']   
valori_d = [50, 125,10, 125]    
soglie_day = []
for i in range(len(city)):
    sensori = [city[i]]
    for j in range(len(sensori_daily)):
        q = [sensori_daily[j]]
        q = q + list(coll_aria.aggregate([
    {
        '$match': {
            'comune': city[i], 
            'sensori.tipo': sensori_daily[j]
        }
    }, {
        '$unwind': {
            'path': '$sensori', 
            'includeArrayIndex': 'nSensore'
        }
    }, {
        '$unwind': {
            'path': '$sensori.valori', 
            'includeArrayIndex': 'nValore'
        }
    }, {
        '$match': {
            'sensori.tipo': sensori_daily[j], 
            'sensori.valori.valore_medio': {
                '$gt': valori_d[j]
            }
        }
    }, {
        '$project': {
            'sensori.tipo': 1, 
            'sensori.valori.data': 1, 
            'sensori.valori.valore_medio': 1
        }
    }
        ]))
        sensori.append(q)      
    soglie_day.append(sensori)
    
query9 = soglie_day

# =============================================================================
# ####    query10_     ####
# =============================================================================
tweets_inquin = tweets_aria.find()       
tweets_inquin = list(tweets_inquin)  
# Contatori sostanze           
c_pm = 0
c_benzene = 0
c_co = 0
c_zolfo = 0
c_ozono = 0
c_azoto = 0
c_pb = 0
c_ni = 0
c_cd = 0

c_aria = 0
c_inquinamento = 0
c_emissioni = 0
c_clima = 0
c_smog = 0
c_aria = 0
for i in range(len(tweets_inquin)):
    text = tweets_inquin[i].get('text')
    text_splitted = text.split()
    for t in range(len(text_splitted)):
        if text_splitted[t] in ['pm25', 'pm2.5', 'PM2.5', 'PM25', 'pm10',
                                'PM10', 'polveri']:
            c_pm = c_pm + 1
        if text_splitted[t] in ['monossido', 'carbonio', 'co', 'CO']:
            c_co = c_co + 1
        if text_splitted[t] in ['benzene', 'c6h6', 'C6H6']:
            c_benzene = c_benzene + 1
        if text_splitted[t] in ['zolfo', 'biossido', 'so2', 'SO2']:
            c_zolfo = c_zolfo + 1
        if text_splitted[t] in ['ozono', 'O3', 'o3']:
            c_ozono = c_ozono + 1
        if text_splitted[t] in ['azoto', 'Nox', 'NOx']:
            c_azoto = c_azoto + 1
        if text_splitted[t] in ['cadmio', 'Cadmio','cd', 'Cd']:
            c_cd = c_cd + 1
        if text_splitted[t] in ['smog','Smog', '#smog','#Smog']:
            c_smog = c_smog + 1
        if text_splitted[t] in ['inquinamento','Inquinamento','#inquinamento']:
            c_inquinamento = c_inquinamento + 1
        if text_splitted[t] in ['aria', 'qualità','Aria']:
            c_aria = c_aria + 1
        if text_splitted[t] in ['piombo', 'Piombo', 'Pb', 'pb']:
            c_pb = c_pb + 1
        if text_splitted[t] in ['nikel','Nikel','ni','Ni','Nichel', 'nichel']:
            c_ni = c_ni + 1
        if text_splitted[t] in ['emissioni', 'Emissioni', '#emissioni']:
            c_emissioni = c_emissioni + 1
        if text_splitted[t] in ['clima','Clima', 'climatechange']:
            c_clima = c_clima + 1
            
# =============================================================================
# ####    query11_     ####
# =============================================================================
# Contatori province
mi = 0
mb = 0
so = 1
mn = 0
bg = 0
bs = 0
lc = 0 
va = 0
co = 0 
pv = 0
lo = 0
cr = 0

for i in range(len(tweets_inquin)):
    text = tweets_inquin[i].get('text')
    prov = tweets_inquin[i].get('provincia')
    text_splitted = text.split()
    for t in range(len(text_splitted)):
        if text_splitted[t] in ['inquinamento', 'smog', 'polveri', '#smog',
                                'Smog', 'Inquinamento', '#inquinamento',
                                '#Smog', 'pm25', 'PM25', 'pm2.5', 'pm10',
                                'PM10', 'ozono', 'Ozono']:
            if prov == 'MI': mi = mi + 1
            elif prov == 'MB': mb = mb + 1
            elif prov == 'SO': so = so + 1
            elif prov == 'MN': mn = mn + 1
            elif prov == 'BG': bg = bg + 1
            elif prov == 'BS': bs = bs + 1
            elif prov == 'LC': lc = lc + 1
            elif prov == 'VA': va = va + 1
            elif prov == 'CO': co = co + 1
            elif prov == 'LO': lo = lo + 1
            elif prov == 'CR': cr = cr + 1
            elif prov == 'PV': pv = pv + 1

# =============================================================================
# ####    query12_     ####
# =============================================================================
pioggia = []
for p in prov:
    q = list(coll_meteo.aggregate([
                {
                    '$match': {
                        'provincia': p, 
                        'sensori.tipo': 'Precipitazione'
                    }
                }, {
                    '$unwind': {
                        'path': '$sensori', 
                        'includeArrayIndex': 'nSensore'
                    }
                }, {
                    '$unwind': {
                        'path': '$sensori.valori', 
                        'includeArrayIndex': 'nValore'
                    }
                }, {
                    '$match': {
                        'sensori.tipo': 'Precipitazione'
                    }
                }, {
                    '$project': {
                        'provincia': 1, 
                        'sensori.tipo': 1, 
                        'sensori.valori': 1, 
                        'lat': 1, 
                        'lng': 1
                    }
                }
            ]))
    pioggia = pioggia + q

query12_df = buildDfMeteo(pioggia)
pp.saveData(query12_df, path_directory, 'Precipitazione_annua')









