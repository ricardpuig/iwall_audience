# Get Current player status and information from Broadsign and export to Programmatic platforms
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

PUBLISHER_ID = 37
VENUE_TYPES_ID = 60
ALLOWED_AD_TYPES_ID ="1:2:4"
AUDIENCE_DATA_SOURCES_ID=4
DEMOGRAPHY_TYPE= "basic"
TAGS_ID_COLOMBIA = "11572"
TAGS_ID_PERU =  "24731"
TAGS_ID_SPAIN = "11571"

engine = create_engine("mysql+pymysql://{user}:{pw}@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/{db}"
                       .format(user="root",
                               pw="SonaeRootMysql2021!",
                               db="audience"))
#database connector
mydb = mysql.connector.connect(
  host="ec2-52-18-248-109.eu-west-1.compute.amazonaws.com",
  user="root",
  passwd="SonaeRootMysql2021!",
  database="audience"
)
mycursor = mydb.cursor()

if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

if country=="SPAIN":
        container_ids=["21393898"]   #SPAIN PLAYER CONTAINER ID
		#container_ids=['136035622']
elif country=="COLOMBIA":
        container_ids=['135518539']  #COLOMBIA PLAYER CONTAINER ID
		#container_ids=['145094982']
elif country=="PERU":
        container_ids=['53704276']
else:
	print("Country Missing, exiting....")
	exit(1)

df_players=pd.read_sql('programmatic_players', engine)

#filter out country player 
df_players=df_players[df_players['country'] == country]

df_players['publisher_id']=PUBLISHER_ID
df_players['device_id']="broadsign.com:"+df_players['player_id']
df_players['name']=df_players['programmatic_name']
df_players['resolution']=df_players['ad_resolution']
df_players['latitude']=df_players['latitude']
df_players['longitude']=df_players['longitude']
if country == "SPAIN":
    df_players['tags:id']=TAGS_ID_SPAIN
elif country =="COLOMBIA":
    df_players['tags:id']=TAGS_ID_COLOMBIA
elif country =="PERU":
    df_players['tags:id']=TAGS_ID_PERU
else:
    df_players['tags:id']=""

df_players['address']=df_players['address']
df_players['bearing_direction']=""
df_players['diagonal_size']=65
df_players['diagonal_size_units']="inches"
df_players['screen_type']=""
df_players['venue_types:id']=VENUE_TYPES_ID
df_players['allowed_ad_types:id']=ALLOWED_AD_TYPES_ID
df_players['audience_data_sources:id']=AUDIENCE_DATA_SOURCES_ID
df_players['screen_img_url']=""
df_players['min_ad_duration']=10.0
df_players['max_ad_duration']=30.0
df_players['demography_type']=DEMOGRAPHY_TYPE
df_players['average_weekly_impressions']=df_players['player_screens']*8000*6
df_players['internal_publisher_screen_id']=df_players['player_id']
df_players['hivestack_id']=""
df_players['vistar_id']=""
df_players['allows_motion']=1
df_players['ox_enabled']=1
df_players['is_active']=1
df_players['floor_cpm_usd']=5.0
df_players['floor_cpm_eur']=5.0
df_players['average_imp_multiplier']=3.0
df_players['screen_count']=df_players['player_screens']


df_players = df_players[['publisher_id','device_id','name','resolution','latitude','longitude','tags:id','address','bearing_direction','diagonal_size','diagonal_size_units','screen_type','diagonal_size_units','venue_types:id','allowed_ad_types:id','audience_data_sources:id','screen_img_url','min_ad_duration','max_ad_duration','demography_type','average_weekly_impressions','internal_publisher_screen_id','hivestack_id','vistar_id','allows_motion','ox_enabled','is_active','floor_cpm_usd','floor_cpm_eur','average_imp_multiplier','screen_count']]


try:
	sql= "DELETE FROM reach_screens"
	mycursor.execute(sql)
	mydb.commit()
except: 
	print("Table not exists")

df_players.to_sql('reach_screens', engine, if_exists='append', index=False)

print(df_players)








