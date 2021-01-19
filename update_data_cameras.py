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
import numpy as np
import pandas as pd


#connect to AUDIENCE DB

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
  

#connect to Seeux DB

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
query = "SELECT * from malls "
df_malls = pd.read_sql_query(query, engine)
print(df_malls)

#we use tickers to get the camera data from pedcounts.
sites=df_malls['ticker'].to_list()
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
#print(df_sites.dtypes)
df_sites = df_sites[[ 'siteID', 'siteName']]


#print(df_malls.dtypes)
df_malls = df_malls[[ 'id','ticker', 'name', 'default_screen_day_impressions', 'screens', 'screens_type1','screens_type2', 'screens_type3', 'screen_type_deviation']]


#get sensors for each site
df_sites=pd.merge(df_malls,df_sites, how='right', left_on="ticker", right_on='siteName')
print(df_sites)


#get proximity data for the site
for s in df_sites['siteID'].to_list():
    query="SELECT * from enriched.traffic_proximity_analysis WHERE \"siteID\"={site}".format(site=s)
    df_proximity= pd.read_sql_query(query,engine2)
    df_proximity= df_proximity[['date','siteID','detections']]
    print(df_proximity)
    df_ots=pd.merge( df_proximity, df_sites, how='left', left_on='siteID',  right_on='siteID')


    df_ots['total_impressions']=(df_ots['screens_type1']*df_ots['detections']*df_ots['screen_type_deviation']) + (df_ots['screens_type2']*df_ots['detections']) + (df_ots['screens_type3']*df_ots['detections']*(1+df_ots['screen_type_deviation']))
    print(df_ots)

    #now iterate and update audience data 
    for i in range(len(df_ots)) : 
        query = "UPDATE audience_impressions SET total_impressions=\'{imp}\' WHERE date={date} AND mall_id={mall}".format(
            imp=df_ots.loc[i, "total_impressions"],
            date=df_ots.loc[i, "total_impressions"],
            mall=df_ots.loc[i, "id"]
        )
        engine.execute(query)



   
