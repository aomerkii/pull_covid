import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import urllib
import time 
import requests
from pandas.io.json import json_normalize
import pyodbc

connection_string=urllib.parse.quote_plus(f'Driver={{ODBC Driver 17 for SQL Server}};Server=LAPTOP-QIJMC9LO;Database=covid_data;Trusted_Connection=yes;')
engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}', fast_executemany=True)


def pull_location():
    df=pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/locations.csv')
    df=pd.concat([df,df.vaccines.str.get_dummies(',').rename(lambda x:'vaccine_'+x,axis='columns')],axis=1)
    with engine.connect() as conn:
        df.to_sql('locations',con=conn,index=None,if_exists='replace')

def pull_vaccinations():
    df=pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv')
    df=df[df.groupby('location')['date'].transform('max')==df['date']]
    with engine.connect() as conn:
        df.to_sql('vaccinations',con=conn,index=None,if_exists='append')

def pull_data(url,table):
    df=pd.read_csv(url)
    with engine.connect() as conn:
        df.to_sql(table,con=conn,index=None,if_exists='replace')

def request_pull_data_thai():
    offset=get_offset()
    url='https://opend.data.go.th/get-ckan/datastore_search?resource_id=67d43695-8626-45ad-9094-dabc374925ab'+'&offset='+offset+'&limit=1000'
    resp=requests.get(url,headers={'api-key':MY_KEY})
    jsondata=resp.json()
    df=pd.json_normalize(jsondata['result'],record_path=['records'])

    #clean date format
    df.announce_date=df.announce_date.apply(lambda x: x[:10] if isinstance(x,str) else x)
    df['Notified date']=df['Notified date'].apply(lambda x: x[:10] if isinstance(x,str) else x)

    with engine.connect() as conn:
        df.to_sql('covid_thai',con=conn,index=None,if_exists='append')
    print(offset)
    time.sleep(120)
    if len(df)==1000:
        return request_pull_data_thai()
    

def get_offset():
    #count data exist in db
    SQLcommand=("select count(*)"
            "FROM [covid_data].[dbo].[covid_thai]")
    #select last index
    with engine.connect() as conn:
        result=pd.read_sql(sql=SQLcommand,con=conn)

    #get last index
    offset=str(result.iloc[0].values[0])
    return offset
"""pull_location()
print('hello')
time.sleep(120)
pull_vaccinations()
print('hello')"""
print('hello')
request_pull_data_thai()
