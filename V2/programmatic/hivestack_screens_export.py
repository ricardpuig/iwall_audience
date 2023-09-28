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

VERSION = "-v.01"

PUBLISHER_ID = 37
VENUE_TYPES_ID = 60
ALLOWED_AD_TYPES_ID ="1:2:4"
AUDIENCE_DATA_SOURCES_ID="4:5"
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
    None
elif country=="COLOMBIA":
    None
elif country=="PERU":
    None
else:
	print("Country Missing, exiting....")
	exit(1)

df_players=pd.read_sql('programmatic_players', engine)

#filter out country player 
df_players=df_players[df_players['country'] == country]

df_players['publisher_id']=PUBLISHER_ID
df_players['device_id']="broadsign.com:"+df_players['player_id']
df_players['name']=df_players['programmatic_name']+VERSION
df_players['resolution']=df_players['ad_resolution']
df_players['latitude']=df_players['latitude']
df_players['longitude']=df_players['longitude']

if country == "SPAIN":
    df_players['Country']='Spain'
    timezone="Europe/Madrid"

elif country =="COLOMBIA":
    df_players['tags:id']=TAGS_ID_COLOMBIA
    timezone="Europe/Madrid"
elif country =="PERU":
    df_players['tags:id']=TAGS_ID_PERU
    timezone="Europe/Madrid"
else:
    df_players['tags:id']=""
    timezone="Europe/Madrid"

df_players['address']=df_players['address']
df_players['bearing_direction']=""
df_players['diagonal_size']=65
df_players['diagonal_size_units']="inches"
df_players['screen_type']="103"
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
df_players['Screen ID']=df_players['player_id']
df_players['Screen name']=df_players['programmatic_name']+VERSION
df_players['Facing direction']=""
df_players['Site ID']=df_players['site_id']
df_players['Network']='iwallinshop'
df_players['Venue type']="Retail – Mall"
df_players['Floor CPM']=5
df_players['Languages']="All / Universal"
df_players['Tags']=""
df_players['Screen width (px)']=df_players['display_type_res_width']
df_players['Screen height (px)']=df_players['display_type_res_height']
df_players['Active']='true'
df_players['Available for ad server']='true'
df_players['Available for deals']='true'
df_players['Available for open exchange']='true'
df_players['Default ad duration']="12"
df_players['Minimum ad duration']="10"
df_players['Maximum ad duration']="30"
df_players['Allow HTML']='true'
df_players['Allow images']='true'
df_players['Allow videos']='true'
df_players['Allow zip']='true'
df_players['Allow audio-only files']='false'
df_players['Advertiser frequency cap']=0.0
df_players['IAB categories']=""
df_players['Multiplier vendor']=""
df_players['Multiplier vendor screen ID']=""
df_players['Strict IAB category blocking']="false"
df_players['Strict IAB frequency capping']="false"
df_players['IAB category frequency cap']=""
df_players['Latitude']=df_players['latitude']
df_players['Longitude']= df_players['longitude']
df_players['Time zone']= timezone
df_players['Operating hours']=""


df_players = df_players[['Screen ID','Screen name','Facing direction','Site ID', 'Network','Venue type','Floor CPM','Languages','Tags','Screen width (px)','Screen height (px)','Active','Available for ad server','Available for deals','Available for open exchange',
                         'Default ad duration','Minimum ad duration','Maximum ad duration','Allow HTML','Allow images','Allow videos','Allow zip','Allow audio-only files','Advertiser frequency cap','IAB categories','Multiplier vendor','Multiplier vendor screen ID','Strict IAB category blocking',
                         'Strict IAB frequency capping','IAB category frequency cap','Latitude','Longitude', 'Time zone', 'Operating hours']]


try:
	sql= "DELETE FROM hivestack_screens"
	mycursor.execute(sql)
	mydb.commit()
except: 
	print("Table not exists")

df_players.to_sql('hivestack_screens', engine, if_exists='append', index=False)

print(df_players)





