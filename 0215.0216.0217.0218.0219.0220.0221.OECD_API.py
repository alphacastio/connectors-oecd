#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
 
import xml.etree.ElementTree as ET
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[2]:


def oecd_data_structure(df, db):
    structure = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/{}".format(db)
    structure_file = 'oecd_structure.xls'

    r = requests.get(structure)
    open(structure_file, "wb").write(r.content)
    tree = ET.parse(structure_file)
    root = ET.parse(structure_file).getroot()


    structure_dict = {}
    for Code in df.SUBJECT.unique():
        text = ""
        parentCode = Code
        while parentCode:
            for node in tree.findall('.//*[@value="' + parentCode +'"]'):
                for description in node.findall('.//*[@{http://www.w3.org/XML/1998/namespace}lang="en"]'):    
                    text = description.text.strip() + text
                parentCode = node.get("parentCode")
                if parentCode:
                    text =  " - " + text
        structure_dict[Code] = text
        print (text)

    df_structure = pd.DataFrame.from_dict(structure_dict, orient='index', columns=["Description"])
    return df.merge(df_structure, left_on="SUBJECT", right_index=True)


# In[3]:


url = "https://stats.oecd.org/SDMX-JSON/data/{}/{}{}/all?startTime={}&dimensionAtObservation=allDimensions&contentType=csv"


def fix_date(df, periodicity):
    if periodicity == "Q":
        df["Time"] = df["Time"].str.replace(r'Q(\d).(\d+)', r'\2-Q\1')
        df['Date'] = pd.PeriodIndex(df["Time"], freq='Q').to_timestamp()
        del df["Time"]
        df = df.set_index("Date")
    if periodicity == "M":
        #df["Time"] = df["Time"].str.replace(r'Q(\d).(\d+)', r'\2-Q\1')
        df['Date'] = pd.PeriodIndex(df["TIME"], freq='M').to_timestamp()
        del df["TIME"]
        df = df.set_index("Date")        
    return df 


# In[4]:


datasets = {
            215:
                {
                    "dataset": "MEI_BOP6",
                    "filter": "..CXCU+CXCUSA.",
                    "start_time": "2019-Q1",
                    "periodicity": "Q",
                    "Complex_subject": False,
                    "dimensions": ["Measure"]
                },
            216: {
                    "dataset": "MEI_BTS_COS",
                    "filter": "...",
                    #"start_time": "2019-01-01",
                    "start_time": "1900-01-01",
                    "periodicity":"M",    
                    "Complex_subject": True,
                    "dimensions": ["Measure"]
                
            },

            217: {
                    "dataset": "MEI_CLI",
                    "filter": "..",
                    #"start_time": "2019-01-01",
                    "start_time": "1900-01-01",
                    "periodicity":"M",    
                    "Complex_subject": False,
                    "dimensions": ["Unit"]
                
            }, 
            
            218: {
                    "dataset": "MEI_FIN",
                    "filter": "..",
                    #"start_time": "2019-01-01",
                    "start_time": "1900-01-01",
                    "periodicity":"M",    
                    "Complex_subject": False,
                    "dimensions": ["Unit"]
            },
            219: {
                    "dataset": "MEI_TRD",
                    "filter": "..CXMLSA+CXML.",
                    #"start_time": "2019-01-01",
                    "start_time": "1900-01-01",
                    "periodicity":"M",    
                    "Complex_subject": False,
                    "dimensions": ["Measure"]
            },
            
            220: {
                    "dataset": "KEI",
                    "filter": "...",
                    #"start_time": "2019-01-01",
                    "start_time": "1900-01-01",
                    "periodicity":"Q",    
                    "Complex_subject": False,
                    "dimensions": ["Measure", "Unit"]
            },
            221: {
                    "dataset": "KEI",
                    "filter": "...",
                    #"start_time": "2019-01-01",
                    "start_time": "1900-01-01",
                    "periodicity":"M",    
                    "Complex_subject": False,
                    "dimensions": ["Measure", "Unit"]
            }        
            }

UpdatedAfter = False


# In[20]:


def get_raw_oecd_data(db_id, db_data):
    req_url = url.format(db_data["dataset"], db_data["filter"], db_data["periodicity"], db_data["start_time"])
    print(req_url)
    r = requests.get(req_url)
    filename = 'oecd_temp.xls'
    open(filename, "wb").write(r.content)
    df = pd.read_csv(filename)
    return df

def get_oecd_data(db_id, db_data):
    df = get_raw_oecd_data(db_id, db_data)
    df = fix_date(df, db_data["periodicity"])
    print(df.columns)
    if db_data["Complex_subject"]:
        df = oecd_data_structure(df, db_data["dataset"])
        df["Subject"] = df["Description"]    
    df = df[["Subject", "Country", "Value"] + db_data["dimensions"]]
    df = df.reset_index().set_index(["Subject", "Country", "Date"] + db_data["dimensions"]).unstack(["Subject"] + db_data["dimensions"]).reset_index().set_index("Date")
    df.columns = df.columns.map(' - '.join)
    df = df.rename(columns={'Country -  - ': "country"})
    df = df.rename(columns={'Country -  -  - ': "country"})
    for col in df.columns:
        df = df.rename(columns={col: col.replace("Value - ", "")})
    return df

for db_id in datasets:

    print(db_id)
    df = get_oecd_data(db_id, datasets[db_id])
    alphacast.datasets.dataset(db_id).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




field_list = ["Date"]
df[df.groupby(field_list).count()>1].reset_index()[field_list]

df.rename(columns={'Country -  -  - ': "country"})
