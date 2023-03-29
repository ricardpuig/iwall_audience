import requests
import json
import re
import mysql.connector
import pandas as pd
import numpy as np
import logging as log
from datetime import datetime, timedelta
from datetime import date
from sqlalchemy import create_engine
from datetime import datetime, timedelta

from fbprophet import Prophet
from fbprophet.plot import plot_plotly
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.offline as py



host = "ec2-52-18-248-109.eu-west-1.compute.amazonaws.com"
user = "root"
passwd = "sonaeRootMysql2017"
db = "audience"
port = 3306

#connect to mysql audience database
"mysql+pymysql://sylvain:passwd@127.0.0.1/db?host=localhost?port=3306"
    # construct an engine connection string
engine_string = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user = user,
        password = passwd,
        host = host,
        port = port,
        db = db,
)
# create sqlalchemy engine
engine = create_engine(engine_string)
  

db = "seeux"
host = "seeux-test-database.chlgafah2kmy.eu-west-1.rds.amazonaws.com"
user = "postgres"
passwd = "1w4LLinshop"
port = 5432

#conenct to postgre sensors database
engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
        user = user,
        password = passwd,
        host = host,
        port = port,
        db = db,
    )
    # create sqlalchemy engine
engine2 = create_engine(engine_string)


#colombian malls
query = "SELECT * from malls WHERE region LIKE \'Colombia\'"
df_colombia_malls = pd.read_sql_query(query, engine)
print(df_colombia_malls)

sites=df_colombia_malls['ticker'].to_list()
print(sites)

 
if len(sites)>1:
    tuple_sites=tuple(sites)
    query="SELECT * from standard.sites WHERE \"siteName\" in {sites}".format(
            sites=tuple_sites
    )
if len(sites)==1:
    query="SELECT * from standard.sites WHERE \"siteName\" in ({sites})".format(
            sites=sites[0]
    )
df_sites = pd.read_sql_query(query,engine2)
print(df_sites)


#get sensors for each site
df_malls=pd.merge(df_colombia_malls,df_sites, how='left', left_on="ticker", right_on='siteName')
print(df_malls)


#get proximity data for the site
for s in df_malls['siteID'].to_list():
	query="SELECT * from enriched.traffic_proximity_analysis_hourly WHERE \"siteID\"={site}".format(
            site=s
         	)
	df_proximity_hourly = pd.read_sql_query(query,engine2)
	print(df_proximity_hourly)

	#use prophet model and ml to interpolate missing data

#if data no available use manual upload

engine.execute("delete from audience_impressions where mall_id=55")
engine.execute("delete from audience_impressions where mall_id=56")
engine.execute("delete from audience_impressions where mall_id=57")
engine.execute("delete from audience_impressions where mall_id=58")
engine.execute("delete from audience_impressions where mall_id=59")
engine.execute("delete from audience_impressions where mall_id=60")
engine.execute("delete from audience_impressions where mall_id=61")
engine.execute("delete from audience_impressions where mall_id=62")
engine.execute("delete from audience_impressions where mall_id=63")

engine.execute("delete from audience_segments where mall_id=55")
engine.execute("delete from audience_segments where mall_id=56")
engine.execute("delete from audience_segments where mall_id=57")
engine.execute("delete from audience_segments where mall_id=58")
engine.execute("delete from audience_segments where mall_id=59")
engine.execute("delete from audience_segments where mall_id=60")
engine.execute("delete from audience_segments where mall_id=61")
engine.execute("delete from audience_segments where mall_id=62")
engine.execute("delete from audience_segments where mall_id=63")



df=pd.read_csv("csv-audience/imperial.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/andino.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/centromayor.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/fontanar.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.9
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/granestacion.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/sanrafael.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/santafe.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/santafemed.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/titan.csv", sep=';')
df['total_impressions']=df['total_impressions']*0.7
df.to_sql('audience_impressions', engine, if_exists='append', index=False)



df=pd.read_csv("csv-audience/segments-imperial.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-fontanar.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-granestacion.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-sanrafael.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-andino.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-centromayor.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-titan.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-santafe.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)

df=pd.read_csv("csv-audience/segments-santafemed.csv", sep=';', decimal=',')
df=df.drop(['id'], axis=1)
print(df)
df.to_sql('audience_segments', engine, if_exists='append', index=False)




