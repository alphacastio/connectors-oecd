#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from alphacast import Alphacast
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


# In[3]:


actual_date = date.today() - relativedelta(months=1)
start_date = date.today() - relativedelta(months=6)

actual_date = actual_date.strftime(format='%Y-%m')
start_date = start_date.strftime(format='%Y-%m')


# In[4]:


#Trae la informacion de los ultimos 6 meses
#Se comenta el de la carga adicional

# response = requests.get('https://stats.oecd.org/SDMX-JSON/data/PRICES_CPI/KOR.CPALTT01.IXOB.M/all?startTime=1965-01&endTime=2021-10')
response = requests.get(f'https://stats.oecd.org/SDMX-JSON/data/PRICES_CPI/KOR.CPALTT01.IXOB.M/all?startTime={start_date}&endTime={actual_date}')


# In[5]:


#Traigo los datos de los valores
false=False
null=None
df_values = pd.DataFrame(eval(response.content.decode('utf-8'))['dataSets'][0]['series']['0:0:0:0']['observations']).T


# In[6]:


#Traigo la estructura de fechas
df_dates = pd.DataFrame(eval(response.content.decode('utf-8'))['structure']['dimensions']['observation'][0]['values'])


# In[7]:


#Antes de concatenar, hacemos el reset de los indices
df = pd.concat([df_dates.reset_index(drop=True), df_values.reset_index(drop=True)], axis=1)


# In[8]:


#Elimino las columnas innecesarias
df.drop(['name', 1], axis=1, inplace=True)
#Renombro las columnas
df.columns = ['Date', 'CPI']
#Cambio a formato fecha
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m')
df.set_index('Date', inplace=True)
#Agrego el país
df['country'] = 'South Korea'


# In[9]:


# dataset_name = 'Prices - South Korea - Statistics Korea - Inflation Index'

# #Para raw data
# dataset = alphacast.datasets.create(dataset_name, 965, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[10]:


alphacast.datasets.dataset(8439).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)

