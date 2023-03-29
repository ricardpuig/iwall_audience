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


sql_select = "SELECT id, name, num_display_units, broadsign_container_id, screens, photo, address from malls where country='%s'" % (country)
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()


reach_screen={}
mall_info={}
reach_screens_export=[]


for row in records:  

    

    print("Updating mall ", row[1], " id ", row[0])
    mall_info['id']=row[0]
    mall_info['name']=row[1]
    mall_info['num_display_units']=row[2]
    mall_info['broadsign_container_id']=row[3]
    mall_info['screens']=row[4]
    mall_info['photo']=row[5]
    mall_info['address']=row[6]

    print(mall_info)


    players_info=[]
    player_info={}

    sql_select = "SELECT player_id, programmatic_name, player_screens, screen_resolution, latitude, longitude from players where mall_id=%s" % (mall_info['id'])
    mycursor.execute(sql_select)
    records2= mycursor.fetchall()
    for row2 in records2:  
    
        player_info={}
        player_info['player_id']=row2[0]
        player_info['programmatic_name']=row2[1]
        player_info['player_screens']=row2[2]
        player_info['screen_resolution']=row2[3]
        player_info['latitude']=row2[4]
        player_info['longitude']=row2[5]
        
        #print(player_info)
                
        if player_info['player_screens']>0:
            players_info.append(player_info)

    print("Players: ", len(players_info), " details: ",  players_info)


    now = datetime.now() # current date and time

    date_time = now.strftime("%Y%m%d")

    tag_id=22728
    if country=="COLOMBIA":
        tag_id=11572
    if country=="SPAIN":
        tag_id=11571
    if country=="PERU":
        tag_id=11573
    

    for p in players_info:

        reach_screen={}
        reach_screen['publisher_id']=37
        reach_screen['device_id']="broadsign.com:"+str(p['player_id'])
        reach_screen['name']=str(p['programmatic_name'])+"_"+date_time
        reach_screen['resolution']=str(p['screen_resolution']).strip()
        reach_screen['latitude']=float(p['latitude'])
        reach_screen['longitude']=float(p['longitude'])
        reach_screen['dma_code']=""
        reach_screen['tags:id']=tag_id
        reach_screen['address']=mall_info['address']
        reach_screen['bearing_direction']=""
        reach_screen['diagonal_size']=65.0
        reach_screen['diagonal_size_units']="inches"
        reach_screen['venue_types:id']=60
        reach_screen['allowed_ad_types:id']="1:2:4"
        reach_screen['audience_data_sources:id']="4:5"
        reach_screen['screen_img_url']=mall_info['photo']
        reach_screen['min_ad_duration']=10.0
        reach_screen['max_ad_duration']=30.0
        reach_screen['demography_type']="basic"
        reach_screen['total']=p['player_screens']
        reach_screen['average_weekly_impressions']=6000*7*p['player_screens']
        reach_screen['internal_publisher_screen_id']=""
        reach_screen['hivestack_id']=""
        reach_screen['vistar_id']=""
        reach_screen['allows_motion']=1
        reach_screen['ox_enabled']=1
        reach_screen['is_active']=1
        reach_screen['floor_cpm_usd']=5.0
        reach_screen['floor_cpm_cad']=""
        reach_screen['floor_cpm_eur']=5.0
        reach_screen['floor_cpm_gbp']=""
        reach_screen['floor_cpm_cad']=""
        reach_screen['floor_cpm_aud']=""

        #print("Reach: ", reach_screen)

        reach_screens_export.append(reach_screen)

    print("\n")


df_reach_screens_export = pd.DataFrame(reach_screens_export)
print(df_reach_screens_export)
df_reach_screens_export.to_csv("reach_screens_export"+ date_time + ".csv", index=False, sep= ",")