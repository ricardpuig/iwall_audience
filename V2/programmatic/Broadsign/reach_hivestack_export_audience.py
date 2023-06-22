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

pd.set_option('display.max_rows', None)

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


sql_select = "SELECT player_id, name from programmatic_player_whitelist"
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

reach_screen={}
mall_info={}
reach_screens_export=[]
hivestack_screens_export=[]


for row in records:  

    print("Generating broadsign audience data for ", row[1], " id ", row[0])

    players_info=[]
    player_info={}

    sql_select = "SELECT player_id, programmatic_name, player_screens, screen_resolution,  mall_id from players where player_id=%s" % (row[0])
    mycursor.execute(sql_select)
    records2= mycursor.fetchall()
    for row2 in records2:  
    
        player_info={}
        player_info['player_id']=row2[0]
        player_info['programmatic_name']=row2[1]
        player_info['player_screens']=row2[2]
        player_info['screen_resolution']=row2[3]
        player_info['mall_id']=row2[4]

        print(player_info)
                
        if player_info['player_screens']>0:
            players_info.append(player_info)

    now = datetime.now() # current date and time
    date_time = now.strftime("%Y%m%d")

    year,week_num,day_of_week = now.isocalendar() # DOW = day of week
    week_num=week_num-2 
    if week_num<1:
        week_num=1
    print("Year %d, Week Number %d, Day of the Week %d" %(year,week_num, day_of_week))
    
    

    hivestack_screen={}

    sql_select = "SELECT impression_multiplier, day_of_week, hour from mall_models where mall_id=%s and week_of_year=%s" % (player_info['mall_id'],week_num)
    mycursor.execute(sql_select)
    records3= mycursor.fetchall()

    for row3 in records3:



            reach_screen={}
            reach_screen['screen_id']="broadsign.com:"+str(player_info['player_id'])
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
            reach_screen['total']=row3[0]*player_info['player_screens']
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




            hivestack_screen['screen_id']=player_info['player_id']
            if row3[1]==0:
                hivestack_screen['0-'+str(row3[2])]= row3[0]*player_info['player_screens']

            if row3[1]==1:
                hivestack_screen['1-'+str(row3[2])]= row3[0]*player_info['player_screens']

            if row3[1]==2:
                hivestack_screen['2-'+str(row3[2])]= row3[0]*player_info['player_screens']

            if row3[1]==3:
                hivestack_screen['3-'+str(row3[2])]= row3[0]*player_info['player_screens']

            if row3[1]==4:
                hivestack_screen['4-'+str(row3[2])]= row3[0]*player_info['player_screens']

            if row3[1]==5:
                hivestack_screen['5-'+str(row3[2])]= row3[0]*player_info['player_screens']

            if row3[1]==6:
                hivestack_screen['6-'+str(row3[2])]= row3[0]*player_info['player_screens']















            #print("Reach: ", reach_screen)

            reach_screens_export.append(reach_screen)
            

    print("\n")
    hivestack_screens_export.append(hivestack_screen)

    hivestack_screen['0-0']=0
    hivestack_screen['0-1']=0
    hivestack_screen['0-2']=0
    hivestack_screen['0-3']=0
    hivestack_screen['0-4']=0
    hivestack_screen['0-5']=0
    hivestack_screen['0-6']=0
    hivestack_screen['0-7']=0
    hivestack_screen['0-8']=0
    hivestack_screen['0-9']=0
    hivestack_screen['0-22']=0
    hivestack_screen['0-23']=0

    hivestack_screen['1-0']=0
    hivestack_screen['1-1']=0
    hivestack_screen['1-2']=0
    hivestack_screen['1-3']=0
    hivestack_screen['1-4']=0
    hivestack_screen['1-5']=0
    hivestack_screen['1-6']=0
    hivestack_screen['1-7']=0
    hivestack_screen['1-8']=0
    hivestack_screen['1-9']=0
    hivestack_screen['1-22']=0
    hivestack_screen['1-23']=0

    hivestack_screen['6-0']=0
    hivestack_screen['6-1']=0
    hivestack_screen['6-2']=0
    hivestack_screen['6-3']=0
    hivestack_screen['6-4']=0
    hivestack_screen['6-5']=0
    hivestack_screen['6-6']=0
    hivestack_screen['6-7']=0
    hivestack_screen['6-8']=0
    hivestack_screen['6-9']=0
    hivestack_screen['6-22']=0
    hivestack_screen['6-23']=0

    hivestack_screen['2-0']=0
    hivestack_screen['2-1']=0
    hivestack_screen['2-2']=0
    hivestack_screen['2-3']=0
    hivestack_screen['2-4']=0
    hivestack_screen['2-5']=0
    hivestack_screen['2-6']=0
    hivestack_screen['2-7']=0
    hivestack_screen['2-8']=0
    hivestack_screen['2-9']=0
    hivestack_screen['2-22']=0
    hivestack_screen['2-23']=0

    hivestack_screen['3-0']=0
    hivestack_screen['3-1']=0
    hivestack_screen['3-2']=0
    hivestack_screen['3-3']=0
    hivestack_screen['3-4']=0
    hivestack_screen['3-5']=0
    hivestack_screen['3-6']=0
    hivestack_screen['3-7']=0
    hivestack_screen['3-8']=0
    hivestack_screen['3-9']=0
    hivestack_screen['3-22']=0
    hivestack_screen['3-23']=0

    hivestack_screen['4-0']=0
    hivestack_screen['4-1']=0
    hivestack_screen['4-2']=0
    hivestack_screen['4-3']=0
    hivestack_screen['4-4']=0
    hivestack_screen['4-5']=0
    hivestack_screen['4-6']=0
    hivestack_screen['4-7']=0
    hivestack_screen['4-8']=0
    hivestack_screen['4-9']=0
    hivestack_screen['4-22']=0
    hivestack_screen['4-23']=0

    hivestack_screen['5-0']=0
    hivestack_screen['5-1']=0
    hivestack_screen['5-2']=0
    hivestack_screen['5-3']=0
    hivestack_screen['5-4']=0
    hivestack_screen['5-5']=0
    hivestack_screen['5-6']=0
    hivestack_screen['5-7']=0
    hivestack_screen['5-8']=0
    hivestack_screen['5-9']=0
    hivestack_screen['5-22']=0
    hivestack_screen['5-23']=0






df_hivestack_screens_export = pd.DataFrame(hivestack_screens_export)

df_hivestack_screens_export=df_hivestack_screens_export[['screen_id','0-0','0-1',
                                                        '0-2', '0-3', '0-4','0-5','0-6','0-7','0-8','0-9','0-10','0-11','0-12',
                                                        '0-13', '0-14', '0-15','0-16','0-17','0-18','0-19','0-20','0-21','0-22','0-23',
                                                        '1-0','1-1',
                                                        '1-2', '1-3', '1-4','1-5','1-6','1-7','1-8','1-9','1-10','1-11','1-12',
                                                        '1-13', '1-14', '1-15','1-16','1-17','1-18','1-19','1-20','1-21','1-22','1-23',
                                                        '2-0','2-1',
                                                        '2-2', '2-3', '2-4','2-5','2-6','2-7','2-8','2-9','2-10','2-11','2-12',
                                                        '2-13', '2-14', '2-15','2-16','2-17','2-18','2-19','2-20','2-21','2-22','2-23',
                                                        '3-0','3-1',
                                                        '3-2', '3-3', '3-4','3-5','3-6','3-7','3-8','3-9','3-10','3-11','3-12',
                                                        '3-13', '3-14', '3-15','3-16','3-17','3-18','3-19','3-20','3-21','3-22','3-23',
                                                        '4-0','4-1',
                                                        '4-2', '4-3', '4-4','4-5','4-6','4-7','4-8','4-9','4-10','4-11','4-12',
                                                        '4-13', '4-14', '4-15','4-16','4-17','4-18','4-19','4-20','4-21','4-22','4-23',
                                                        '5-0','5-1',
                                                        '5-2', '5-3', '5-4','5-5','5-6','5-7','5-8','5-9','5-10','5-11','5-12',
                                                        '5-13', '5-14', '5-15','5-16','5-17','5-18','5-19','5-20','5-21','5-22','5-23',
                                                        '6-0','6-1',
                                                        '6-2', '6-3', '6-4','6-5','6-6','6-7','6-8','6-9','6-10','6-11','6-12',
                                                        '6-13', '6-14', '6-15','6-16','6-17','6-18','6-19','6-20','6-21','6-22','6-23']]
                                                        

                                                        
                                                  
print(df_hivestack_screens_export)



df_reach_screens_export = pd.DataFrame(reach_screens_export)
#print(df_reach_screens_export)
df_reach_screens_export.to_csv("reach_audience_export"+ date_time + ".csv", index=False, sep= ",")
df_hivestack_screens_export.to_csv("hivestack_audience_export"+ date_time + ".csv", index=False, sep= ",")