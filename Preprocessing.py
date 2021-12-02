# -*- coding: utf-8 -*-
"""
@author: lomProg, enigarv
"""


import pandas as pd
import numpy as np
from datetime import datetime
import json
from sklearn.ensemble import IsolationForest

# =============================================================================
# Definizione funzione per Preprocessing
# =============================================================================

def saveData(data, path, fileName, typeFile = 'csv', fFormat = None):
    
    if typeFile == "csv":
        data.to_csv(path + fileName + ".csv",
                    index = False, float_format = fFormat)
        
    elif typeFile == "json":
        with open(path + fileName + '.json', 'w') as fp:
            json.dump(data, fp, indent = 4)


def split_date_time(df):
    
    date_time = df["Data"]
    data = []
    time = []
    
    for i in range(len(date_time)):
        temp = date_time.iloc[i].split()
        date_temp = datetime.strptime(temp[0], '%d/%m/%Y')
        data.append(date_temp.date())
        time.append(temp[1])
        
    df["Data"] = data
    df["Time"] = time


def subDf(l, df):
    
    subData = pd.DataFrame(columns = ['Vals', 'Sens', 'Data', 'Time'])
    valsTemp = []
    sensTemp = []
    dateTemp = []
    timeTemp = []
    indxTemp = []
    
    for sens in l:
        valsTemp = valsTemp + list(df[df['IdSensore'] == sens]['Valore'])
        sensTemp = sensTemp + list(df[df['IdSensore'] == sens]['IdSensore'])
        dateTemp = dateTemp + list(df[df['IdSensore'] == sens]['Data'])
        timeTemp = timeTemp + list(df[df['IdSensore'] == sens]['Time'])
        indxTemp = indxTemp + list(df[df['IdSensore'] == sens].index)
        
    subData['Vals'] = valsTemp
    subData['Sens'] = sensTemp
    subData['Data'] = dateTemp
    subData['Time'] = timeTemp
    subData['Indx'] = indxTemp
    
    return subData


def z_score(scores, m, s):
    
    threshold = 3
    mean = m
    std = s
    out = []
    
    if std != 0:
        
        for val in scores:
            z_score = (val - mean)/std
        
            if np.abs(z_score) > threshold:
                out.append(True)
            else: out.append(False)
    else: out = list(np.repeat(False, len(scores), axis=0))
            
    return pd.Series(out)


def iqr_bounds(scores, k = float(1.5)):
    
    scores = scores.sort_values()
    q1, q3 = np.percentile(scores, [25, 75])
    iqr = q3 - q1
    lower_bound = q1 - (k * iqr)
    upper_bound = q3 + (k * iqr)
    
    return lower_bound, upper_bound


def find_IForest(df, l):
    
    df_temp = df.copy()
    outlier = pd.Series(dtype=str)
    score = pd.Series(dtype=float)
    outlier_z = pd.Series(dtype=bool)
    outlier_iqr = pd.Series(dtype=bool)
    output_outliers = pd.DataFrame(columns = ['Vals','Index'])
    
    for sens in l:
        
        df_sensore = df_temp[df_temp['Sens'] == sens]
        model = IsolationForest(contamination = float(.004))
        model.fit(df_sensore[['Vals']])
        temp = pd.Series(model.predict(df_sensore[['Vals']])).apply(
            lambda x: 'yes' if (x == -1) else 'no')
        outlier = outlier.append(temp)
        
        model_1 = IsolationForest() #contamination = float(.0001)
        model_1.fit(df_sensore[['Vals']])
        temp_score = pd.Series(model_1.decision_function(df_sensore[['Vals']]))
        score = score.append(temp_score)
        #Z score
        mean = np.mean(temp_score)
        std = np.std(temp_score)
        outlier_z = outlier_z.append(z_score(temp_score, mean, std))
        #IQR
        l_b, u_b = iqr_bounds(temp_score)
        iqr_anomalies = []
        for val in temp_score:
            if val < l_b or val > u_b:
                iqr_anomalies.append(True)
            else: iqr_anomalies.append(False)
        iqr_anomalies = pd.Series(iqr_anomalies)
        outlier_iqr = outlier_iqr.append(iqr_anomalies)
        
    outlier = outlier.reset_index()
    outlier = pd.Series(outlier.loc[:,0])
    df_temp['Outlier'] = outlier
    score = score.reset_index()
    score = pd.Series(score.loc[:,0])
    df_temp['Score'] = score
    outlier_z = outlier_z.reset_index()
    outlier_z = pd.Series(outlier_z.loc[:,0])
    df_temp['OutlierZ'] = outlier_z
    outlier_iqr = outlier_iqr.reset_index()
    outlier_iqr = pd.Series(outlier_iqr.loc[:,0])
    df_temp['OutlierIQR'] = outlier_iqr
    
    j = 0 #iteratore per il df in output "output_outliers"
    for i in range(len(df_temp)):
        out_temp = []
        
        if df_temp['Outlier'][i] == 'yes' and df_temp['OutlierIQR'][i]:
            
            out_temp.append(df_temp['Vals'].iloc[i])
            out_temp.append(df_temp['Indx'].iloc[i])
            output_outliers.loc[j] = out_temp
            output_outliers['Index'] = output_outliers['Index'].astype(int)
            j += 1
            
    return output_outliers


def detect_outlierZ(df, l):
    
    outliers = pd.DataFrame(columns = ['Vals','Index'])
    threshold = 3
    j = 0
    
    for sens in l:
        sens_temp = df[df['Sens'] == sens]
        sens_temp = sens_temp.sort_values(by = ['Data','Time'])
        
        for i in range(5, len(sens_temp)):
            vals = sens_temp['Vals']
            
            if ((vals.iloc[i-1] >= -1.5 and vals.iloc[i-1] < -0.8) or
                (vals.iloc[i-1] > 0.8 and vals.iloc[i-1] <= 1.5)):
                
                if ((vals.iloc[i-1] > 0 and
                     (vals.iloc[i] < -vals.iloc[i-1] or
                      vals.iloc[i] > 4 * vals.iloc[i-1])) or
                    (vals.iloc[i-1] < 0 and
                     (vals.iloc[i] > -vals.iloc[i-1] or
                      vals.iloc[i] < 4 * vals.iloc[i-1]))):
                    
                    mean = np.mean(vals[(i-5) : i])
                    std = np.std(vals[(i-5) : i])
                    
                    if std != 0:
                        
                        z_score = (vals.iloc[i] - mean)/std
                        out_temp = []
                        
                        if np.abs(z_score) > threshold:
                            
                            out_temp.append(vals.iloc[i])
                            temp = sens_temp['Indx'].iloc[i]
                            out_temp.append(temp)
                            outliers.loc[j] = out_temp
                            outliers['Index'] = outliers['Index'].astype(int)
                            j += 1
                            
            elif ((vals.iloc[i-1] >= -6 and vals.iloc[i-1] < -1.5) or
                  (vals.iloc[i-1] > 1.5 and vals.iloc[i-1] <= 6)):
                
                if ((vals.iloc[i-1] > 0 and
                     (vals.iloc[i] < -vals.iloc[i-1] or
                      vals.iloc[i] > 2 * vals.iloc[i-1])) or
                    (vals.iloc[i-1] < 0 and
                     (vals.iloc[i] > -vals.iloc[i-1] or
                      vals.iloc[i] < 2 * vals.iloc[i-1]))):
                    
                    mean = np.mean(vals[(i-5) : i])
                    std = np.std(vals[(i-5) : i])
                    
                    if std != 0:
                        
                        z_score = (vals.iloc[i] - mean)/std
                        out_temp = []
                        
                        if np.abs(z_score) > threshold:
                            
                            out_temp.append(vals.iloc[i])
                            temp = sens_temp['Indx'].iloc[i]
                            out_temp.append(temp)
                            outliers.loc[j] = out_temp
                            outliers['Index'] = outliers['Index'].astype(int)
                            j += 1
                            
            elif vals.iloc[i-1] < -6 or vals.iloc[i-1] > 6:
                
                if ((vals.iloc[i-1] > 0 and
                     (vals.iloc[i] < 0 or
                      vals.iloc[i] > 1.5 * vals.iloc[i-1])) or
                    (vals.iloc[i-1] < 0 and
                     (vals.iloc[i] > 0 or
                      vals.iloc[i] < 1.5 * vals.iloc[i-1]))):
                    
                    mean = np.mean(vals[(i-5) : i])
                    std = np.std(vals[(i-5) : i])
                    
                    if std != 0:
                        
                        z_score = (vals.iloc[i] - mean)/std
                        out_temp = []
                        
                        if np.abs(z_score) > threshold:
                            
                            out_temp.append(vals.iloc[i])
                            temp = sens_temp['Indx'].iloc[i]
                            out_temp.append(temp)
                            outliers.loc[j] = out_temp
                            outliers['Index'] = outliers['Index'].astype(int)
                            j += 1
                            
            elif vals.iloc[i-1] >= -0.8 and vals.iloc[i-1] <= 0.8:
                if ((vals.iloc[i-1] >= 0 and vals.iloc[i] > 10)
                    or (vals.iloc[i-1] < 0 and
                        vals.iloc[i] < -10)):
                    
                    out_temp = []
                    out_temp.append(vals.iloc[i])
                    temp = sens_temp['Indx'].iloc[i]
                    out_temp.append(temp)
                    outliers.loc[j] = out_temp
                    outliers['Index'] = outliers['Index'].astype(int)
                    j += 1                    
                            
            else:
                mean = np.mean(vals[(i-5) : (i+1)])
                std = np.std(vals[(i-5) : (i+1)])
                
                if std != 0:
                        
                        z_score = (vals.iloc[i] - mean)/std
                        out_temp = []
                        
                        if np.abs(z_score) > threshold:
                            
                            out_temp.append(vals.iloc[i])
                            temp = sens_temp['Indx'].iloc[i]
                            out_temp.append(temp)
                            outliers.loc[j] = out_temp
                            outliers['Index'] = outliers['Index'].astype(int)
                            j += 1
                            
    return outliers


def update_outlier(df, outliers):
    
    val_new = 0
    
    for indx in outliers['Index']:
        
        idSens_temp = df.loc[indx]['IdSensore']
        day_temp = df.loc[indx]['Data']
        temp = df[df['IdSensore'] == idSens_temp]
        temp = temp[temp['Data'] == day_temp]
        indx_prec = indx - 1
        indx_suc = indx + 1
        
        if (indx == 0 or idSens_temp != df.loc[indx_prec]['IdSensore']):
            if (indx < len(df) and indx_suc < len(df) and
                indx_suc + 1 < len(df) and indx_suc + 2 < len(df)):
                
                if (not(indx_suc in list(outliers['Index']))
                    and not(indx_suc + 1 in list(outliers['Index']))):
                    ## Caso 1 ##
                    val_new = np.mean([df.loc[indx_suc]['Valore'],
                                       df.loc[indx_suc + 1]['Valore']])
                    
                elif (not(indx_suc + 1 in list(outliers['Index'])) and
                      not(indx_suc + 2 in list(outliers['Index']))):
                    ## Caso 2 ##
                    val_new = np.mean([df.loc[indx_suc + 1]['Valore'],
                                       df.loc[indx_suc + 2]['Valore']])
                    
            elif (indx_prec in list(outliers['Index']) and
                  (indx_prec - 1 == 0 or
                   idSens_temp != df.loc[indx_prec - 1]['IdSensore']) and
                  (not(indx_suc + 1 in list(outliers['Index'])) and
                   not(indx_suc + 2 in list(outliers['Index'])))):
                ## Caso 3 ##
                val_new = np.mean([df.loc[indx_suc + 1]['Valore'],
                                   df.loc[indx_suc + 2]['Valore']])
                
        elif ((indx == len(df) - 1) or (indx_suc < len(df) and
               idSens_temp != df.loc[indx_suc]['IdSensore'])):
            
            if (not(indx_prec in list(outliers['Index'])) and
                not(indx_prec - 1 in list(outliers['Index']))):
                ## Caso 4 ##
                val_new = np.mean([df.loc[indx_prec]['Valore'],
                                   df.loc[indx_prec - 1]['Valore']])
            
            elif (not(indx_prec - 1 in list(outliers['Index'])) and
                  not(indx_prec - 2 in list(outliers['Index']))):
                ## Caso 5 ##
                val_new = np.mean([df.loc[indx_prec - 1]['Valore'],
                                   df.loc[indx_prec - 2]['Valore']])
                    
        elif (indx_suc in list(outliers['Index']) and
              (indx_suc + 1 == len(df) or
               df.loc[indx_suc]['IdSensore'] !=
               df.loc[indx_suc + 1]['IdSensore']) and
              (not(indx_prec in list(outliers['Index'])) and
               not(indx_prec - 1 in list(outliers['Index'])))):
            ## Caso 6 ##
            val_new = np.mean([df.loc[indx_prec]['Valore'],
                               df.loc[indx_prec - 1]['Valore']])
            
        elif (not(indx_prec in list(outliers['Index'])) and
              not(indx_suc in list(outliers['Index']))):
            ## Caso 7 ##
            val_new = np.mean([df.loc[indx_prec]['Valore'],
                               df.loc[indx_suc]['Valore']])
            
        elif (not(indx_prec in list(outliers['Index'])) and
              indx_suc in list(outliers['Index']) and
              not(indx_suc + 1 in list(outliers['Index']))):
            ## Caso 8 ##
            val_new = np.mean([df.loc[indx_prec]['Valore'],
                               df.loc[indx_suc + 1]['Valore']])
            
        elif (indx_prec in list(outliers['Index']) and
              not(indx_suc in list(outliers['Index'])) and
              not(indx_prec - 1 in list(outliers['Index']))):
            ## Caso 9 ##
            val_new = np.mean([df.loc[indx_prec - 1]['Valore'],
                               df.loc[indx_suc]['Valore']])
            
        else: ## Caso 10 ##
            vals = 0
            n = 0
            for j in range(len(temp)):
                
                if not(temp.iloc[j].name in list(outliers['Index'])):
                    
                    vals += temp.iloc[j]['Valore']
                    n += 1
                    
            if n != 0:
                val_new = vals / n
            else: val_new = df.loc[indx]['Valore']
        
        outliers = outliers.drop(outliers[outliers['Index'] == indx].index)
        df['Valore'][indx] = val_new


# =============================================================================
# Aria
# =============================================================================

def preprocessingAria(path, method = 'IsolationForest', epoches = int(1)):
    
    dati_aria = pd.read_csv(path + "Dati_sensori_aria_2020.csv")
    stazioni_aria = pd.read_csv(path + "Stazioni_sensori_aria.csv")
    
    #Rimozione sensori con valori non validi
    dati_aria = dati_aria.sort_values(by = ["Valore"])
    c = 0
    row_del = []
    while(dati_aria["Valore"].iloc[c] == -9999):
        row_del.append(dati_aria.iloc[c].name)
        c = c + 1
    dati_aria = dati_aria.drop(row_del)
    
    #Rimozione colonne 'Stato' ed 'idOperatore'
    dati_aria = dati_aria.drop("Stato", 1)
    dati_aria = dati_aria.drop("idOperatore", 1)
    
    #Rimozione sensori in disuso 
    for i in range(len(stazioni_aria)):
        if isinstance(stazioni_aria["DataStop"][i], str):
            temp = stazioni_aria["DataStop"][i].split('/')
            if temp[2] != '2020':
                stazioni_aria = stazioni_aria.drop(i)
    stazioni_aria = stazioni_aria.set_index(np.arange(len(stazioni_aria)))
    
    #Rimozione colonne 'NomeStazione', 'Storico', 'DataStop', 'UTM_Nord' e
    #'UTM_Est'
    stazioni_aria = stazioni_aria.drop("NomeStazione", 1)
    stazioni_aria = stazioni_aria.drop("Storico", 1)
    stazioni_aria = stazioni_aria.drop("DataStop", 1)
    stazioni_aria = stazioni_aria.drop("Utm_Nord", 1)
    stazioni_aria = stazioni_aria.drop("UTM_Est", 1)
    
    #Suddivisione colonna data in 'Data' e 'Time
    split_date_time(dati_aria)
    dati_aria = dati_aria.sort_values(by = ['IdSensore', 'Data', 'Time'])
    dati_aria = dati_aria.set_index(np.arange(len(dati_aria)))
    
    #Creazione liste gruppi sensori
    nh3List = [] #Ammoniaca
    asList = [] #Arsenico
    c6h6List = [] #Benzene
    c20h12List = [] #Benzo(a)pirene
    no2List = [] #Biossido di Azoto
    so2List = [] #Biossido di Zolfo
    blackCList = [] #BlackCarbon
    cdList = [] #Cadmio
    coList = [] #Monossido di Carbonio
    niList = [] #Nikel
    noList = [] #Ossidi di Azoto
    o3List = [] #Ozono
    pm10List = [] #PM10
    pm10SMList = [] #PM10 (SM2005)
    pm25List = [] #Particelle sospese PM2.5
    ptsList = [] #Particolato Totale Sospeso
    pbList = [] #Piombo
    for i in range(len(stazioni_aria)):
        if stazioni_aria['NomeTipoSensore'].iloc[i] == "Ammoniaca":
            nh3List.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Arsenico":
            asList.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Benzene":
            c6h6List.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Benzo(a)pirene":
            c20h12List.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Biossido di Azoto":
            no2List.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Biossido di Zolfo":
            so2List.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "BlackCarbon":
            blackCList.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Cadmio":
            cdList.append(stazioni_aria["IdSensore"].iloc[i])
        elif (stazioni_aria['NomeTipoSensore'].iloc[i] ==
              "Monossido di Carbonio"):
            coList.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Nikel":
            niList.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Ossidi di Azoto":
            noList.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Ozono":
            o3List.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "PM10":
            pm10List.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "PM10 (SM2005)":
            pm10SMList.append(stazioni_aria["IdSensore"].iloc[i])
        elif (stazioni_aria['NomeTipoSensore'].iloc[i] ==
              "Particelle sospese PM2.5"):
            pm25List.append(stazioni_aria["IdSensore"].iloc[i])
        elif (stazioni_aria['NomeTipoSensore'].iloc[i] ==
              "Particolato Totale Sospeso"):
            ptsList.append(stazioni_aria["IdSensore"].iloc[i])
        elif stazioni_aria['NomeTipoSensore'].iloc[i] == "Piombo":
            pbList.append(stazioni_aria["IdSensore"].iloc[i])
    
    if method == 'IsolationForest':
        #Suddiviosione in subDF per inquinanti
        subNH3 = subDf(nh3List, dati_aria)
        subAS = subDf(asList, dati_aria)
        subC6H6 = subDf(c6h6List, dati_aria)
        subC20H12 = subDf(c20h12List, dati_aria)
        subNO2 = subDf(no2List, dati_aria)
        subSO2 = subDf(so2List, dati_aria)
        subBlackC = subDf(blackCList, dati_aria)
        subCD = subDf(cdList, dati_aria)
        subNI = subDf(niList, dati_aria)
        subNO = subDf(noList, dati_aria)
        subO3 = subDf(o3List, dati_aria)
        subPM10 = subDf(pm10List, dati_aria)
        subPM10SM = subDf(pm10SMList, dati_aria)
        subPM25 = subDf(pm25List , dati_aria)
        subPTS = subDf(ptsList , dati_aria)
        subPB = subDf(pbList , dati_aria)
        
        #Rilevamento outliers
        outliersNH3_iF = find_IForest(subNH3, nh3List)
        outliersAS_iF = find_IForest(subAS, asList)
        outliersC6H6_iF = find_IForest(subC6H6, c6h6List)
        outliersC20H12_iF = find_IForest(subC20H12, c20h12List)
        outliersNO2_iF = find_IForest(subNO2, no2List)
        outliersSO2_iF = find_IForest(subSO2, so2List)
        outliersBlackC_iF = find_IForest(subBlackC, blackCList)
        outliersCD_iF = find_IForest(subCD, cdList)
        outliersNI_iF = find_IForest(subNI, niList)
        outliersNO_iF = find_IForest(subNO, noList)
        outliersO3_iF = find_IForest(subO3, o3List)
        outliersPM10_iF = find_IForest(subPM10, pm10List)
        outliersPM10SM_iF = find_IForest(subPM10SM, pm10SMList)
        outliersPM25_iF = find_IForest(subPM25, pm25List)
        outliersPTS_iF = find_IForest(subPTS, ptsList)
        outliersPB_iF = find_IForest(subPB, pbList)
        
        #Modifica valori anomali rilevati
        update_outlier(dati_aria, outliersNH3_iF)
        update_outlier(dati_aria, outliersAS_iF)
        update_outlier(dati_aria, outliersC6H6_iF)
        update_outlier(dati_aria, outliersC20H12_iF)
        update_outlier(dati_aria, outliersNO2_iF)
        update_outlier(dati_aria, outliersSO2_iF)
        update_outlier(dati_aria, outliersBlackC_iF)
        update_outlier(dati_aria, outliersCD_iF)
        update_outlier(dati_aria, outliersNI_iF)
        update_outlier(dati_aria, outliersNO_iF)
        update_outlier(dati_aria, outliersO3_iF)
        update_outlier(dati_aria, outliersPM10_iF)
        update_outlier(dati_aria, outliersPM10SM_iF)
        update_outlier(dati_aria, outliersPM25_iF)
        update_outlier(dati_aria, outliersPTS_iF)
        update_outlier(dati_aria, outliersPB_iF)
        
    elif method == "RollingMean":
        eps = 0
        while eps < epoches:
            #Suddiviosione in subDF per inquinanti
            subNH3 = subDf(nh3List, dati_aria)
            subAS = subDf(asList, dati_aria)
            subC6H6 = subDf(c6h6List, dati_aria)
            subC20H12 = subDf(c20h12List, dati_aria)
            subNO2 = subDf(no2List, dati_aria)
            subSO2 = subDf(so2List, dati_aria)
            subBlackC = subDf(blackCList, dati_aria)
            subCD = subDf(cdList, dati_aria)
            subNI = subDf(niList, dati_aria)
            subNO = subDf(noList, dati_aria)
            subO3 = subDf(o3List, dati_aria)
            subPM10 = subDf(pm10List, dati_aria)
            subPM10SM = subDf(pm10SMList, dati_aria)
            subPM25 = subDf(pm25List , dati_aria)
            subPTS = subDf(ptsList , dati_aria)
            subPB = subDf(pbList , dati_aria)
            
            #Rilevamento outliers
            outliersNH3_Z = detect_outlierZ(subNH3, nh3List)
            outliersAS_Z = detect_outlierZ(subAS, asList)
            outliersC6H6_Z = detect_outlierZ(subC6H6, c6h6List)
            outliersC20H12_Z = detect_outlierZ(subC20H12, c20h12List)
            outliersNO2_Z = detect_outlierZ(subNO2, no2List)    #10 rilevati
            outliersSO2_Z = detect_outlierZ(subSO2, so2List)    #58 rilevati
            outliersBlackC_Z = detect_outlierZ(subBlackC, blackCList)
            outliersCD_Z = detect_outlierZ(subCD, cdList)
            outliersNI_Z = detect_outlierZ(subNI, niList)
            outliersNO_Z = detect_outlierZ(subNO, noList)       #44 rilevati
            outliersO3_Z = detect_outlierZ(subO3, o3List)       #11 rilevati
            outliersPM10_Z = detect_outlierZ(subPM10, pm10List)
            outliersPM10SM_Z = detect_outlierZ(subPM10SM, pm10SMList)
            outliersPM25_Z = detect_outlierZ(subPM25, pm25List)
            outliersPTS_Z = detect_outlierZ(subPTS, ptsList)
            outliersPB_Z = detect_outlierZ(subPB, pbList)
            
            #Modifica valori anomali rilevati
            update_outlier(dati_aria, outliersNH3_Z)
            update_outlier(dati_aria, outliersAS_Z)
            update_outlier(dati_aria, outliersC6H6_Z)
            update_outlier(dati_aria, outliersC20H12_Z)
            update_outlier(dati_aria, outliersNO2_Z)
            update_outlier(dati_aria, outliersSO2_Z)
            update_outlier(dati_aria, outliersBlackC_Z)
            update_outlier(dati_aria, outliersCD_Z)
            update_outlier(dati_aria, outliersNI_Z)
            update_outlier(dati_aria, outliersNO_Z)
            update_outlier(dati_aria, outliersO3_Z)
            update_outlier(dati_aria, outliersPM10_Z)
            update_outlier(dati_aria, outliersPM10SM_Z)
            update_outlier(dati_aria, outliersPM25_Z)
            update_outlier(dati_aria, outliersPTS_Z)
            update_outlier(dati_aria, outliersPB_Z)
            
            eps += 1
    
    return dati_aria, stazioni_aria

# =============================================================================
# Meteo
# =============================================================================

def preprocessingMeteo(path, method = 'IsolationForest', epoches = int(1)):
    
    dati_meteo = pd.read_csv(path + "Dati_sensori_meteo_2020.csv")
    stazioni_meteo = pd.read_csv(path + "Stazioni_sensori_meteo.csv")
    
    #Rimozione colonne 'Stato'e 'idOperatore'
    dati_meteo = dati_meteo.drop("Stato", 1)
    dati_meteo = dati_meteo.drop("idOperatore", 1)
    
    #Rimozione sensori in disuso
    for i in range(len(stazioni_meteo)):
        if isinstance(stazioni_meteo["DataStop"][i], str):
            temp = stazioni_meteo["DataStop"][i].split('/')
            if temp[2] != '2020':
                stazioni_meteo = stazioni_meteo.drop(i)
    stazioni_meteo = stazioni_meteo.set_index(np.arange(len(stazioni_meteo)))
    
    #Rimozione colonne 'NomeStazione', 'Storico', 'DataStop', 'UTM_Nord' e
    #'UTM_Est'
    stazioni_meteo = stazioni_meteo.drop("Storico", 1)
    stazioni_meteo = stazioni_meteo.drop("DataStop", 1)
    stazioni_meteo = stazioni_meteo.drop("UTM_Nord", 1)
    stazioni_meteo = stazioni_meteo.drop("UTM_Est", 1)
    
    #Suddivisione colonna data in 'Data' e 'Time
    split_date_time(dati_meteo)
    dati_meteo = dati_meteo.sort_values(by = ['IdSensore', 'Data', 'Time'])
    dati_meteo = dati_meteo.set_index(np.arange(len(dati_meteo)))
    
    #Creazione liste gruppi sensori
    altNeveList = [] #Altezza Neve
    dirVentoList = [] #Direzione Vento
    livIdroList = [] #Livello Idrometrico
    precList = [] #Precipitazione
    radGlobList = [] #Radiazione Globale
    temprList = [] #Temperatura
    umRelList = [] #Umidità Relativa
    velVentoList = [] #Velocità Vento
    for i in range(len(stazioni_meteo)):
        if stazioni_meteo['Tipologia'].iloc[i] == "Altezza Neve":
            altNeveList.append(stazioni_meteo["IdSensore"].iloc[i])
        elif stazioni_meteo['Tipologia'].iloc[i] == "Direzione Vento":
            dirVentoList.append(stazioni_meteo["IdSensore"].iloc[i])
        elif stazioni_meteo['Tipologia'].iloc[i] == "Livello Idrometrico":
            livIdroList.append(stazioni_meteo["IdSensore"].iloc[i])
        elif stazioni_meteo['Tipologia'].iloc[i] == "Precipitazione":
            precList.append(stazioni_meteo["IdSensore"].iloc[i])
        elif stazioni_meteo['Tipologia'].iloc[i] == "Radiazione Globale":
            radGlobList.append(stazioni_meteo["IdSensore"].iloc[i])
        elif stazioni_meteo['Tipologia'].iloc[i] == "Temperatura":
            temprList.append(stazioni_meteo["IdSensore"].iloc[i])
        elif stazioni_meteo['Tipologia'].iloc[i] == "Umidità Relativa":
            umRelList.append(stazioni_meteo["IdSensore"].iloc[i])
        elif stazioni_meteo['Tipologia'].iloc[i] == "Velocità Vento":
            velVentoList.append(stazioni_meteo["IdSensore"].iloc[i])
            
    if method == 'IsolationForest':
        #Suddiviosione in subDF per agenti atmosferici
        subAltNeve = subDf(altNeveList, dati_meteo)
        subDirVento = subDf(dirVentoList, dati_meteo)
        subLivIdro = subDf(livIdroList, dati_meteo)
        subPrec = subDf(precList, dati_meteo)
        subRadGlob = subDf(radGlobList, dati_meteo)
        subTempr = subDf(temprList, dati_meteo)
        subUmRel = subDf(umRelList, dati_meteo)
        subVelVento = subDf(velVentoList, dati_meteo)
        
        #Rilevamento outliers
        outliersAltNeve_iF = find_IForest(subAltNeve, altNeveList)
        outliersDirVento_iF = find_IForest(subDirVento, dirVentoList)
        outliersLivIdro_iF = find_IForest(subLivIdro, livIdroList)
        outliersPrec_iF = find_IForest(subPrec, precList)
        outliersRadGlob_iF = find_IForest(subRadGlob, radGlobList)
        outliersTempr_iF = find_IForest(subTempr, temprList)
        outliersUmRel_iF = find_IForest(subUmRel, umRelList)
        outliersVelVento_iF = find_IForest(subVelVento, velVentoList)
        
        #Update dati senza outliers
        update_outlier(dati_meteo, outliersAltNeve_iF)
        update_outlier(dati_meteo, outliersDirVento_iF)
        update_outlier(dati_meteo, outliersLivIdro_iF)
        update_outlier(dati_meteo, outliersPrec_iF)
        update_outlier(dati_meteo, outliersRadGlob_iF)
        update_outlier(dati_meteo, outliersTempr_iF)
        update_outlier(dati_meteo, outliersUmRel_iF)
        update_outlier(dati_meteo, outliersVelVento_iF)
        
    elif method == "RollingMean":
        eps = 0
        while eps < epoches:
            #Suddiviosione in subDF per agenti atmosferici
            subAltNeve = subDf(altNeveList, dati_meteo)
            subDirVento = subDf(dirVentoList, dati_meteo)
            subLivIdro = subDf(livIdroList, dati_meteo)
            subPrec = subDf(precList, dati_meteo)
            subRadGlob = subDf(radGlobList, dati_meteo)
            subTempr = subDf(temprList, dati_meteo)
            subUmRel = subDf(umRelList, dati_meteo)
            subVelVento = subDf(velVentoList, dati_meteo)
            
            #Rilevamento outliers
            outliersAltNeve_Z = detect_outlierZ(subAltNeve, altNeveList)
            outliersDirVento_Z = detect_outlierZ(subDirVento, dirVentoList)
            outliersLivIdro_Z = detect_outlierZ(subLivIdro, livIdroList)
            outliersPrec_Z = detect_outlierZ(subPrec, precList)
            outliersRadGlob_Z = detect_outlierZ(subRadGlob, radGlobList)
            outliersTempr_Z = detect_outlierZ(subTempr, temprList)
            outliersUmRel_Z = detect_outlierZ(subUmRel, umRelList)
            outliersVelVento_Z = detect_outlierZ(subVelVento, velVentoList)
            
            #Update dati senza outliers
            update_outlier(dati_meteo, outliersAltNeve_Z)
            update_outlier(dati_meteo, outliersDirVento_Z)
            update_outlier(dati_meteo, outliersLivIdro_Z)
            update_outlier(dati_meteo, outliersPrec_Z)
            update_outlier(dati_meteo, outliersRadGlob_Z)
            update_outlier(dati_meteo, outliersTempr_Z)
            update_outlier(dati_meteo, outliersUmRel_Z)
            update_outlier(dati_meteo, outliersVelVento_Z)
            
            eps += 1
    
    return dati_meteo, stazioni_meteo







