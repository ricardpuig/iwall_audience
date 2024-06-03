# Get Current player status and information from Broadsign and update AUDIENCE player information
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

print("Generating Screen information from players")

sql_select = "SELECT mall_id, player_screens, player_id, latitude, longitude, container_name, player_name, ad_resolution, address, country, geolocation, programmatic_name  from players "
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

screens=[]
screen_data={}

screen_counter=1

for row in records:  
    print(row)
    print("Num screens: ", row[1])
    num_screens=row[1]
    print("Latitude", row[3])
    print("Longitude", row[4])
    player_id= row[2]

    mall_id=row[0]
    
    #input()
    mall_name= ""
    city=""
    region=""

    sql_select = "SELECT name, city, region from malls where id ={id}".format(id=mall_id)
    mycursor.execute(sql_select)
    records= mycursor.fetchall()
    mall_name=""
    for row2 in records:  
        mall_name=row2[0]
        city=row2[1]
        region= row2[2]

    screen_counter= 1
    for index in range(num_screens):

        screen_data={}
        screen_data['screen_id']="IWALL-"+f"{mall_id:03}" +"-" + player_id + "-"+ f"{screen_counter:04}"
        screen_data["mall_id"]=mall_id
        screen_data["player_id"]=row[2]
        screen_data['latitude']=row[3]
        screen_data['longitude']=row[4]
        screen_data['size']=65
        screen_data['visibility_index']=1 
        screen_data['player_name']=row[11]
        screen_data['mall_name']=mall_name
        screen_data['geolocation']=row[10]
        screen_data['address']=row[9]
        screen_data['country']=row[8]
        screen_data['ad_resolution']=row[7]
        screen_data['city']=city
        screen_data['region']=region



        screen_counter=screen_counter+1
           
        screens.append(screen_data)    

df_screens = pd.DataFrame(screens)

#show(df_screens)
try:
    query="DELETE FROM screens"
    mycursor.execute(query)
    mydb.commit()
except:
    None
df_screens.to_sql('screens', engine, if_exists='append', index=False)


#send to DB




