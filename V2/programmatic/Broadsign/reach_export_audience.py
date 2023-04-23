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

    year,week_num,day_of_week = now.isocalendar() # DOW = day of week
    print("Year %d, Week Number %d, Day of the Week %d" %(year,week_num, day_of_week))


    for p in players_info:

        sql_select = "SELECT impression_multiplier, day_of_week, hour from mall_models where mall_id=%s and week_of_year=%s" % (mall_info['id'],week_num)
        mycursor.execute(sql_select)
        records3= mycursor.fetchall()
        for row3 in records3:


            reach_screen={}
            reach_screen['screen_id']="broadsign.com:"+str(p['player_id'])
            reach_screen['start_date']="2020-01-01"
            reach_screen['end_date']="2025-12-31"
            reach_screen['start_time']=str(row3[2])+":00:00"
            reach_screen['end_time']=str(row3[2])+":59:59"

            reach_screen['mon']=1 if row3[1]==0 else 0
            reach_screen['tue']=1 if row3[1]==1 else 0
            reach_screen['wed']=1 if row3[1]==2 else 0
            reach_screen['thu']=1 if row3[1]==3 else 0
            reach_screen['fri']=1 if row3[1]==4 else 0
            reach_screen['sat']=1 if row3[1]==5 else 0
            reach_screen['sun']=1 if row3[1]==6 else 0

            reach_screen['demography_type']="basic"
            reach_screen['total']=row3[0]*p['player_screens']
            reach_screen['male']=""
            reach_screen['females']=""

            reach_screen['males_12']=""
            reach_screen['males_18']=""
            reach_screen['males_25']=""
            reach_screen['males_35']=""
            reach_screen['males_45']=""
            reach_screen['males_55']=""
            reach_screen['males_65']=""
            reach_screen['females_12']=""
            reach_screen['females_18']=""
            reach_screen['females_25']=""
            reach_screen['females_35']=""
            reach_screen['females_45']=""
            reach_screen['females_55']=""
            reach_screen['females_65']=""


            #print("Reach: ", reach_screen)

            reach_screens_export.append(reach_screen)

    print("\n")


df_reach_screens_export = pd.DataFrame(reach_screens_export)
print(df_reach_screens_export)
df_reach_screens_export.to_csv("reach_audience_export"+ date_time + ".csv", index=False, sep= ",")