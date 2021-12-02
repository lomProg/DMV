# -*- coding: utf-8 -*-
"""
@author: lomProg, enigarv
"""


import pandas as pd
import numpy as np
import Preprocessing as pp
path_directory = "./DMV/"

# =============================================================================
# Definizione funzione per creazione dei db
# =============================================================================

def dbCreation(dfDati, dfStaz, sensList, sensList_staz):
    
    sensori = []
    j = 0
    dfLink = 0
    
    for i in range(len(sensList)):
        
        if sensList[i] in sensList_staz:
            dict_sensore = {}
            dict_sensore['idSensore'] = sensList[i]
            
            if (dfStaz.columns[1] == 'NomeTipoSensore' or
                dfStaz.iloc[dfLink,1] != "Precipitazione"):
                datiYear = dfDati[dfDati['IdSensore'] == sensList[i]]
                dict_sensore['valore_annuo'] = np.mean(datiYear['Valore'])
                
            else:
                datiYear = dfDati[dfDati['IdSensore'] == sensList[i]]
                dict_sensore['valore_annuo'] = sum(datiYear['Valore'])
                
            if sensList[i] == dfStaz.iloc[dfLink,0]:
                dict_sensore['tipo'] = dfStaz.iloc[dfLink,1]
                dict_sensore['unitaMisura'] = dfStaz.iloc[dfLink,2]
                dfLink = dfLink + 1
                
            else:
                
                while sensList[i] > dfStaz.iloc[dfLink,0]:
                    dfLink = dfLink + 1
                    
                dict_sensore['tipo'] = dfStaz.iloc[dfLink,1]
                dict_sensore['unitaMisura'] = dfStaz.iloc[dfLink,2]
                dfLink = dfLink + 1
                
            valori = []
            
            while j < len(dfDati) and dfDati['IdSensore'][j] == sensList[i]:
                
                vals = {}
                sensore = dfDati[dfDati['IdSensore'] == sensList[i]]
                data_temp = dfDati['Data'][j]
                sensore = sensore[sensore['Data'] == data_temp]
                
                if not(isinstance(data_temp, str)):
                    vals['data'] = data_temp.strftime('%Y-%m-%d')
                    
                else: vals['data'] = data_temp
                
                if dict_sensore['tipo'] in ['Temperatura', 'Umidità Relativa',
                                            'Livello Idrometrico']:
                    vals['valore_max'] = max(sensore['Valore'])
                    vals['valore_min'] = min(sensore['Valore'])
                    
                elif dict_sensore['tipo'] == 'Precipitazione':
                    vals['valore'] = sum(sensore['Valore']) #cumulato pioggia
                    
                else:
                    #AltezzaNeve, VelocitaVento, DirezioneVento,
                    #RadiazioneGlobale, inquinanti aria
                    vals['valore_medio'] = np.mean(sensore['Valore'])
                    vals['dev_st'] = np.std(sensore['Valore']) 
                    
                valori.append(vals)
                j = j + len(sensore)
                
            dict_sensore['valori'] = valori
            sensori.append(dict_sensore)
            
        elif (not(sensList[i] in sensList_staz) and
              j < len(dfDati)):
            #SI' valori - NO informazioni sens
            id_missing = dfDati['IdSensore'][j]
            sens_missing = dfDati[dfDati['IdSensore'] == id_missing]
            j = j + len(sens_missing)
            
    #Lista con Id stazioni df
    stazList = dfStaz.iloc[:,3]
    stazList = list(dict.fromkeys(stazList))
    stazList.sort()
    
    dfStaz = dfStaz.sort_values(by = dfStaz.columns[3])
    dfStaz = dfStaz.set_index(np.arange(len(dfStaz)))
    
    stazioni = []
    for k in range(len(stazList)):
        
        dict_stazione = {}
        staz_temp = dfStaz[dfStaz.iloc[:,3] == stazList[k]]
        dict_stazione['_id'] = staz_temp.iloc[0,3].item()
        dict_stazione['provincia'] = staz_temp['Provincia'][:1].item()
        dict_stazione['lat'] = staz_temp['lat'][:1].item()
        dict_stazione['lng'] = staz_temp['lng'][:1].item()
        
        if staz_temp.columns[5] == 'Quota': #Dati Meteo
            dict_stazione['comune'] = staz_temp['NomeStazione'][:1].item()
            
        else: dict_stazione['comune'] = staz_temp['Comune'][:1].item()
        
        if not(np.isnan(staz_temp['Quota'][:1].item())):
            dict_stazione['quota'] = staz_temp['Quota'][:1].item()
            
        if isinstance(staz_temp['DataStart'][:1].item(), str):
            dict_stazione['dataStart'] = staz_temp['DataStart'][:1].item()
            
        stazioni.append(dict_stazione)
        sens = []
        pos = 0
        staz_temp = staz_temp.sort_values(by = 'IdSensore')
        
        for i in range(len(staz_temp)):
            
            if staz_temp['IdSensore'].iloc[i] in sensList:
                
                while (pos < (len(sensori)-1) and
                       staz_temp['IdSensore'].iloc[i] !=
                       sensori[pos].get('idSensore')):
                    pos = pos + 1
                    
                sens.append(sensori[pos])
                
            else:
                #SI' informazioni sens - NO valori
                dict_temp = {'idSensore': staz_temp['IdSensore'].iloc[i].item(),
                             'tipo': staz_temp.iloc[i,1],
                             'unitaMisura': staz_temp.iloc[i,2]}
                sens.append(dict_temp)
                
        dict_stazione['sensori'] = sens
        
    return stazioni

# =============================================================================
# Pre-processing aria
# =============================================================================

dati_aria, stazioni_aria = pp.preprocessingAria(path_directory)

# =============================================================================
# Pre-processing Meteo
# =============================================================================

dati_meteo, stazioni_meteo = pp.preprocessingMeteo(path_directory)

# =============================================================================
# Ordinamento dei dati
# =============================================================================

#Sort dati per id sensore e data
dati_meteo = dati_meteo.sort_values(by = ["IdSensore", "Data", "Time"])
dati_meteo = dati_meteo.set_index(np.arange(len(dati_meteo)))
stazioni_meteo = stazioni_meteo.sort_values(by = 'IdSensore')
#Sort dati per id sensore e data
dati_aria = dati_aria.sort_values(by = ["IdSensore", "Data", "Time"])
dati_aria = dati_aria.set_index(np.arange(len(dati_aria)))
stazioni_aria = stazioni_aria.sort_values(by = 'IdSensore')

#### Istanziazione variabili per funzione dbCreation ####
#Id sensori in 'dati_meteo'
sens_meteo = dati_meteo["IdSensore"]
sens_meteo = list(dict.fromkeys(sens_meteo))
sens_meteo.sort()
#Id sensori in 'stazioni_meteo'
sens_in_staz = stazioni_meteo["IdSensore"]
sens_in_staz = list(dict.fromkeys(sens_in_staz))
sens_in_staz.sort()
#Id sensori in 'dati_aria'
idSensori_aria = dati_aria["IdSensore"]
idSensori_aria = list(dict.fromkeys(idSensori_aria))
idSensori_aria.sort()
#Id sensori in 'stazioni_aria'
idSensori_in_staz = stazioni_aria["IdSensore"]
idSensori_in_staz = list(dict.fromkeys(idSensori_in_staz))
idSensori_in_staz.sort()

# =============================================================================
# Creazione dei db
# =============================================================================

list_stazioni_meteo = dbCreation(dati_meteo, stazioni_meteo,
                                 sens_meteo, sens_in_staz)
list_stazioni_aria = dbCreation(dati_aria, stazioni_aria,
                                idSensori_aria, idSensori_in_staz)

pp.saveData(list_stazioni_meteo, path_directory, 'StazioniMeteoDB', 'json')
pp.saveData(list_stazioni_aria, path_directory, 'StazioniAriaDB', 'json')









