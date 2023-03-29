from dis import dis
import requests
import json
import re
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import logging as log
from datetime import datetime, timedelta
from datetime import date
import sys
import mysql.connector
from pandasgui import show
from trycourier import Courier 


engine_audience = create_engine("mysql+pymysql://{user}:{pw}@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/{db}"
                       .format(user="root",
                               pw="SonaeRootMysql2021!",
                               db="audience"))

engine_malls_data = create_engine("mysql+pymysql://{user}:{pw}@iwall-01.c52dupxcl4yc.eu-west-1.rds.amazonaws.com/{db}"
                       .format(user="admin",
                               pw="1w4LLinshop",
                               db="malls-data"))


#database connector for audience database
mydb_audience = mysql.connector.connect(
  host="ec2-52-18-248-109.eu-west-1.compute.amazonaws.com",
  user="root",
  passwd="SonaeRootMysql2021!",
  database="audience"
)
mycursor_audience = mydb_audience.cursor()

#database connector for mall data
mydb_malls_data = mysql.connector.connect(
  host="iwall-01.c52dupxcl4yc.eu-west-1.rds.amazonaws.com",
  user="admin",
  passwd="1w4LLinshop",
  database="malls-data"
)
mycursor_malls_data = mydb_malls_data.cursor()

sql= "SELECT DISTINCT mall_name FROM malls_footfall_hist"
mycursor_malls_data.execute(sql)
records= mycursor_malls_data.fetchall()

for row in records:  #for each result
    print("Analysing footfall data for ", row[0])
    mall_name=row[0]

    if mall_name=="Plaza Norte":
        mall_name= "Plaza Norte 2"

    print("Getting mall id from audience database")
    sql= "SELECT id FROM malls WHERE name LIKE \"%s\"" % (mall_name)
    mycursor_audience.execute(sql)
    records1= mycursor_audience.fetchall()
    
    if records1:
        print("Mall ID should be ", records1[0])
        mall_id=records1[0][0]
    else:
        mall_id=0
    
    
    sql= "SELECT visits_today, current_footfall, last_updated, occupancy, max_occupancy from malls_footfall_hist where mall_name='%s'" % (str(row[0]))
    df_mall_data = pd.read_sql_query(sql, engine_malls_data)
    
    #start cleaning dataframe
    df_mall_data = df_mall_data.drop(df_mall_data[df_mall_data.current_footfall == 0].index)
    df_mall_data['last_updated']=df_mall_data['last_updated'] + timedelta(hours=2)
    print(df_mall_data.dtypes)

    df_mall_data=df_mall_data.resample('H', on='last_updated').agg({'current_footfall':'mean', 'visits_today':'max','occupancy':'mean'})
    df_mall_data=df_mall_data.drop(['visits_today', 'occupancy'], axis=1)
    df_mall_data=df_mall_data.dropna()
    df_mall_data['current_footfall']=df_mall_data['current_footfall'].astype(int)
    df_mall_data.index = pd.to_datetime(df_mall_data.index)
    df_mall_data['end_date']=df_mall_data.index + timedelta(hours=1)
    df_mall_data['mall_name']=row[0]
    df_mall_data['period']="HOURLY"
    df_mall_data['mall_id']=mall_id
    df_mall_data['mall_visits']=df_mall_data['current_footfall']
    df_mall_data=df_mall_data.drop(['current_footfall'], axis=1)
    df_mall_data['start_date']=df_mall_data.index
    df_mall_data['country']="SPAIN"
    df_mall_data=df_mall_data.reset_index(drop=True)
    
    print(df_mall_data)
    df_mall_data.to_sql('mall_traffic_data', engine_audience, if_exists='append', index=False)

#removing duplicated rows in case of ingesting same data
query="delete t1 FROM mall_traffic_data t1 INNER  JOIN mall_traffic_data t2 WHERE t1.id< t2.id AND t1.start_date = t2.start_date AND t1.end_date = t2.end_date AND t1.mall_id = t2.mall_id;"
mycursor_audience.execute(query)
mydb_audience.commit()

if(mydb_malls_data.is_connected()):
    mycursor_malls_data.close()
    print("MySQL connection is closed")

if(mydb_audience.is_connected()):
    mycursor_audience.close()
    print("MySQL connection is closed")


