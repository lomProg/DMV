# Guida operativa per il codice

Questo documento è stato creato come guida dei file e delle funzioni implementate ed ha come obiettivo quello di descrivere in modo pratico come utilizzare il codice raccolto nella cartella ```DMV```.\
In particolare la cartella contiene:
* ```Relazione.pdf``` (non presente online)
* ```Docker-compose.yml```, file di configurazione dello sharding
* Gli script python implementati
  + ```Preprocessing.py```
  + ```Integration.py```
  + ```TweetsDownload.py```
  + ```Storage.py```
  + ```Queries.py```

I dati originali, scaricati da ARPA Lombardia, sono individuabili ai seguenti link:
* [dati_sensori_meteo_2020.csv](https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo/647i-nhxk)
* [stazioni_sensori_meteo.csv](https://www.dati.lombardia.it/Ambiente/Stazioni-Meteorologiche/nf78-nj6b)
* [dati_sensori_aria_2020.csv](https://www.dati.lombardia.it/Ambiente/Dati-sensori-aria-2020/88sp-5tmj)
* [stazioni_sensori_aria.csv](https://www.dati.lombardia.it/Ambiente/Stazioni-qualit-dell-aria/ib47-atvt)
## Fase di pre processing
Le seguenti funzioni sono raccolte nel file denominato *Preprocessing.py* il cui scopo è di sottoporre i dati originali ad un processo di scrematura e pulizia. Tale file viene poi richiamato e importato da *Integration.py*.

1. ```saveData(data, path, fileName, typeFile = 'csv', fFormat = None)```: questa funzione ci è servita nel corso dello svolgimento del progetto per salvare i file necessari alla realizzazione del db e delle infografiche. In particolare la funzione genera un file denominato ```fileName``` nel percorso ```path``` salvandoci ```data``` nel formato ```typeFile```, scelto tra ```'json'``` e ```'csv'```. Nel caso in cui si salvasse il file in formato ```.csv``` la funzione permette di selezionare il formato di salvatto dei numeri tipo float attraverso ```fFormat```.
2. ```split_date_time(df)```: la funzione prende in input un'istanza pd.DataFrame, suddividendo la colonna *data* in due differenti attributi *data* e *time*. La funzione ci è servita a migliorare la leggibilità dei dati originali, in cui la data e l'ora di ogni rilevazione erano inserite in un unico attributo *Data*.
3. ```subDf(l, df)```: prende in input una lista di interi e un'istanza pd.DataFrame in modo da suddividere il dataframe in cluster. In particolare questa funzione utilizza il parametro ```l```, una lista che raccoglie gli id dei sensori di una particolare tipologia (es.: 'Precipitazioni', 'PM2.5' ...), per suddividere l'insieme di dati originale ```df``` in sottoinsiemi disgiunti.
4. ```z_score(scores, m, s)```: dati in input la lista di float ```scores``` e due float ```m``` e ```s```, la funzione calcola per ogni valore appartenente a ```scores``` il relativo *z score* e verifica se esso sia maggiore della soglia. Restituisce in output la lista che raccoglie i valori booleani corrispondendi ai risultati di tale ricerca.
5. ```iqr_bounds(scores, k = float(1.5))```: la funzione, presi in input la lista di float ```scores``` ed un float ```k``` (di default ```k = 1.5```), determina l'*upper* e il *lower bound* dell'IQR in funzione del parametro ```k```.
6. ```find_IForest(df, l)```: la funzione prende in input un'istanza pd.DataFrame ed una lista di interi. In particolare il parametro ```df``` passato alla funzione è uno dei sotto dataframe creati attraverso la funzione ```subDf(l, df)``` e il parametro ```l``` è la lista degli id dei sensori di quel particolare cluster. All'insieme delle rilevazione dei sensori nella lista ```l``` viene applicato l'algortimo di machine learning *Isolation Forest* per ricercare i valori outlier. La funzione restituisce un dataframe che raccoglie i valori identificati come anomali con i rispettivi indici.
7. ```detect_outlierZ(df, l)```: la funzione, esattamente come la precedente, prende in input un'istanza pd.DataFrame ed una lista di interi. Questa funzione è l'implementazione della prima soluzione progettata che, attraverso la media mobile con una finestra di dimensione 6, valuta secondo la statistica *z score* se una rilevazione sia anomala. L'output è un dataframe che raccoglie i valori identificati come anomali con i rispettivi indici. 
8. ```update_outlier(df, outliers)```: dati in input un dataframe ```df``` e un dataframe costruito attraverso una delle due funzione di *anomaly detection* ```find_IForest``` o ```detect_outlierZ```, la funzione apporta delle modifiche alle rilevazioni del dataset originario ```df``` che sono presenti nel dataframe ```outliers```.
9. ```preprocessingAria(path, method = 'IsolationForest', epoches = int(1))```: la funzione è stata implementata con l'obiettivo di raccogliere tutto il processo di pre processing sui dataset relativi ai dati sul'inquinamento dell'aria. In particolare la funzione carica i due dataset processandoli attraverso le funzioni descritte sopra. Abbiamo deciso di lasciare la possibilità di selezionare il metodo di pre processing preferito attraverso il parametro ```method```, una stringa selezionabile tra "*IsolationForest*" e "*RollingMean*" (si riferiscono rispettivamente alla funzione ```7``` e ```8```); inoltre la funzione prende in input un intero ```epoches``` che imposta il numero richiesto di epoche nel caso in cui si fosse scelto il metodo a media mobile, aumentando l'accuratezza di rilevazione degli outliers ma peggiorando notevolmente i tempi di esecuzione. La funzione restituisce in output i due dataset ```dati_sensori_aria_2020``` e ```stazioni_sensori_aria``` ripuliti e pronti per la fase successiva.
10. ```preprocessingMeteo(path, method = 'IsolationForest', epoches = int(1))```: questa funzione effettua tutte le operazioni descritte nella funzione precedente ma appliccandole sui dataset relativi ai dati meteo (```dati_sensori_meteo_2020``` e ```stazioni_sensori_meteo```).

## Fase di integrazione degli schemi
Il processo di Data Integration è stato sviluppato nel file ```Integration.py```, che richiamando ```PreProcessing.py```, utilizza le funzioni ```preprocessingAria``` e ```preprocessingMeteo``` che permettono il caricamento dei file originali, *Dati_sensori_aria_2020*, *Dati_sensori_meteo_2020*, *Stazioni_sensori_aria_2020* e *Dati_sensori_meteo_2020* e lo svolgimento del pre-processing su di essi.

1. ```dbCreation(dfDati, dfStaz, sensList, sensList_staz)```: la funzione effettua l'integrazione del dataset contentente le rilevazioni (```dfDati```) e quello delle stazioni (```dfStaz```) sulla base dell'attributo *idSensore*, comune ad entrambi gli schemi. Il parametro ```sensList``` è la lista contenenti gli id dei sensori presenti in ```dfDati```, mentre ```sensList_staz``` è una lista contenente i sensori nel dataset ```dfStaz```. Il risultato dell'integrazione è stato memorizzato per mezzo di un modello documentale, sfruttando la chiamata a funzione ```Preprocessing.saveData(dict_to_save, path, name_file, 'json')```. La funzione viene eseguita su dataset già pre-processati (dopo aver eseguito le due funzioni di pre processing) e ordinati per *IdSensore*, *Data* e *Time*.

## Web-Scraping
Il *Web Scraping* viene eseguito nel file denominato ```TweetsDownload.py```. In questo file sono stati scaricati i tweets attraverso dei criteri di ricerca, come parole chiave e posizione geografica. Il download fa uso della libreria ```snscrape.modules.twitter```, che a differenza delle API di Twitter, permette di scaricare i tweets in qualsiasi periodo si desideri.
 
1. ```download_tweet(query, provincia, geocode)```: tale funzione è stata implementata per effettuare il download dei tweet. Essa prende in imput il parametro ```query```, una stringa contenente un insieme di parole chiave e il periodo di interesse, necessario per la ricerca, il parametro ```provincia``` ovvero la sigla della provincia ed infine il parametro ```geocode``` corrispondente alle coordinate geografiche del relativo capoluogo di provincia. Tali parametri verranno passati alla funzione ```TwitterSearchScraper``` della libreria che genera in output degli oggetti di tipo Tweet, da cui noi abbiamo estrapolato i seguenti attributi: *content*, *id*, *date* e *username*.

Abbiamo effettuato ricerche diverse per i tweets sul meteo e quelli sulla qualità dell'aria, attraverso l'impostazione di due queries separate contenenti parole chiave relative ad un determinato tema. I tweets una volta scaricati sono stati convertiti in formato documentale, necessario per costruire il database su MongoDB.

## Popolamento del database
Come si legge nel report, per gestire il volume dei dati si è implementata un'architettura MongoDB con sharding. Di seguito viene riportato il codice che ha permesso l'implementazione di tale soluzione.\
Per attivare i nodi costruiti attraverso lo script di configurazione ```docker-compose.yml```:
```
docker-compose up -d
```
Verificare che i nodi siano attivi:
```
docker ps
```
Per spostarsi nel terminale del container:
```
docker exec -it mongos1 /bin/bash
```
Dal terminale alla mongo shell:
```
mongo
```
Creazione database ***Progetto_DMV***:
```
use Progetto_DMV
```
Per attivare lo sharding per il database creato:
```
sh.enableSharding("Progetto_DMV")
```
Creazione delle collezioni nel database con chiavi *hashed* (bilancia la scrittura negli shard):
```
db.Stazioni_meteo.ensureIndex({_id: "hashed"})
sh.shardCollection("Progetto_DMV.Stazioni_meteo", {_id: "hashed"})

db.Stazioni_aria.ensureIndex({_id: "hashed"})
sh.shardCollection("Progetto_DMV.Stazioni_aria", {_id: "hashed"})

db.Tweets_meteo.ensureIndex({_id: "hashed"})
sh.shardCollection("Progetto_DMV.Tweets_meteo", {_id: "hashed"})

db.Tweets_aria.ensureIndex({_id: "hashed"})
sh.shardCollection("Progetto_DMV.Tweets_aria", {_id: "hashed"})
```

Il popolamento delle collezioni *distribuite* viene svolto nel file ```Storage.py``` che, caricati i file ```.json```, provvede a popolare le collezioni così create.

Per verificare il corretto caricamento *bilanciato* sugli shard si esegue:
```
db.Stazioni_meteo.getShardDistribution()
```

## Estrazione dei dati tramite queries

L'estrazione delle informazioni dei dati necessari all'analisi è avvenuta all'interno del file ```Queries.py```. 

 1. ```findValMaxPull(dfProv)```: la funzione preso in input ```dfProv```, un dataframe contenente l'*id* delle stazioni relativa a ciascuna tipologia di sensore per ogni provincia; produce in output una lista contenente l'id della stazione, il valore massimo per tipologia di inquinante e la data in cui si è verifico.
 2. ```dataMaxProv(q)```: presa in input la query in output dalla funzione precedente genera una lista contenente per ciascuna provincia la data in cui si è verificato il valore massimo.
 3. ```findAgentInMax(dfProv, tuplaMax)```: la funzione, preso in input ```tuplaMax```, l'output della funziona precedente, determina in output quale siano i valori corrispondenti degli agenti atmosferici.
 4. ```findValMaxMinAgent(dfProv)```: la funzione preso in input ```dfProv```, un dataframe contenente l'*id* delle stazioni relativa a ciascuna tipologia di sensore per ogni provincia, produce in output una lista contenente i valori sia massimi che minimi per ogni tipologia di agente atmosferico, per ogni provincia, con la corrispondete data.
 5. ```findTweetPerMaxMinAgent(recap)```: presa in input la lista ```recap``` contenente le date in cui si sono verificati i valori massimi degli agenti atmosferici, ricerca tra i tweet scaricati quelli pubblicati in quelle date.
 7. ```findMeteoByCity(dfProv)```: dato in input il dataframe contenente l'*id* delle stazioni relative a ciascun fenomeno metereologico per ogni provincia, permette di ottenere tutti i dati relativi a quello specifico fenomeno.
 8. ```buildDF(q)```: presa in input il risultato di una qualsiasi query, la funzione produce un dataframe con le informazioni relative, al fine di scaricare i dati necessari per le infografiche.

All'interno del file sono raccolte anche tutte le altre queries che sono servite per scaricare i dati per le analisi come, ad esempio, quelle necessarie per verificare in che giorni i limiti imposti dalla legge siano stati superati e in quali città. Sul report è possibile visionare l'elenco di tutte le queries svolte.
 

