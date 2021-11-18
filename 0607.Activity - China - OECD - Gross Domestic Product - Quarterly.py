#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[2]:


#flat
# r1 = requests.get('https://stats.oecd.org/SDMX-JSON/data/QNA/CHN.B1_GE+B1_GA.CQR+VNBQR+GYSA+GPSA.Q/all?startTime=1947-Q1&endTime=2021-Q2&dimensionAtObservation=allDimensions').text

#time series
# r2 = requests.get('https://stats.oecd.org/SDMX-JSON/data/QNA/CHN.B1_GE+B1_GA.CQR+VNBQR+GYSA+GPSA.Q/all?startTime=1947-Q1&endTime=2021-Q2').text


# In[3]:


#Si el mes actual es anterior a abril, tomo el último trimestre del año previo
if datetime.now().month < 4:
    year = datetime.now().year - 1
    quarter = 4
else:
    #si es un mes posterior, calculo el numero del trimestre y despues le resto 1 para que me traiga un trimestre
    #previo al actual
    year = datetime.now().year
    quarter = (datetime.now().month + 2) //3 - 1


# In[4]:


#Defino cada uno de los parametros
dataset = 'QNA'
series = ['CHN.B1_GE', 'B1_GA.CQR', 'VNBQR', 'GYSA', 'GPSA.Q']
start_time = '1947-Q1'
#defino la variable del ultimo periodo
end_time = str(year) + '-Q' + str(quarter)

url = ('https://stats.oecd.org/SDMX-JSON/data/' + dataset + '/' + '+'.join(series) + '/all?startTime=' +
      start_time + '&endTime=' + end_time + '&contentType=csv')


# In[5]:


df = pd.read_csv(url)


# In[6]:


#Solo se mantiene las columnas Subject (indicador), Measure (en que unidad esta expresada), TIME y Value
df = df[['Subject', 'Measure', 'TIME', 'Value']]

#Concateno los valores
df['Subject_Measure'] = df['Subject'] + ' - ' + df['Measure']
df.rename(columns = {'TIME': 'Date'}, inplace=True)

#Elimino las 2 primeras columnas
df = df.iloc[:, 2:]


# In[7]:


#Reemplazo los quarters por el mes
dict_quarters = {'Q1': '01', 'Q2': '04', 'Q3' : '07', 'Q4':'10'}
df['Date'].replace(dict_quarters, regex=True, inplace=True)

df['Date'] = pd.to_datetime(df['Date'], format = '%Y-%m')


# In[8]:


#Pivoteo los datos para que queden las variables como columna
df = df.pivot(index='Date', columns = 'Subject_Measure', values='Value')

#Elimino el nombre del eje
df.rename_axis(None, axis=1, inplace=True)
df['country'] = 'China'

alphacast.datasets.dataset(607).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

