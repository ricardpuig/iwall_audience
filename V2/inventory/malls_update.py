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
from trycourier import Courier 
import collections, functools, operator

def mall_update_report(message_to_send):

    email_to_send="rpuig@iwallinshop.com"
    client = Courier(auth_token="pk_prod_6S1S6BVGGXMEB5Q8TSVHDN59NY8D")
    auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";
    report_template= "63S3SQJ5EA4YAYH10N88VCJJE6QT"

    resp = client.send_message(
				message={
					"to": {
						"email": email_to_send,
					},
					"template": report_template,
						"data": {
							"message_type" : "Mall Inventory Update: ",
							"message": "https://datastudio.google.com/reporting/7e090667-0f67-4af8-bde8-3481df63bdb8"
						},
				})
    print(resp['requestId'])
    None


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


print("Updating and checking mall information")

sql_select = "SELECT id, name, num_display_units, broadsign_container_id, screens, screens_type1_high_visibility, screens_type2_default_visibility, screens_type3_low_visibility, sba, num_locales, screen_type_deviation_type1_high_visibility, screen_type_deviation_type3_low_visibility, mall_size, screen_exposure_area, external from malls"
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

mall_info={}
for row in records:  

    num_screens = 0

    print("Updating mall ", row[1], " id ", row[0])
    mall_info['id']=row[0]
    mall_info['name']=row[1]
    mall_info['num_display_units']=row[2]
    mall_info['broadsign_container_id']=row[3]
    mall_info['screens']=row[4]
    mall_info['screens_type1_high_visibility']=row[5]
    mall_info['screens_type2_default_visibility']=row[6]
    mall_info['screens_type3_low_visibility']=row[7]
    mall_info['sba']=row[8]
    mall_info['num_locales']=row[9]
    mall_info['modifier_high_vis']=row[10]
    mall_info['modifier_low_vis']=row[11]
    mall_info['screen_exposure_area']=row[13]
    mall_info['mall_size']=row[12]
    mall_info['external']=row[14]

    '''
    if mall_info['sba']>40000:
      mall_info['mall_size']= "LARGE"
    elif mall_info['sba']>20000:
      mall_info['mall_size']= "MEDIUM"
    else:
      mall_info['mall_size']= "SMALL"
    '''
    
    #if mall_info['mall_size']!="CUSTOM":
    #  mall_info['screen_exposure_area']= round(mall_info['sba'] * 0.25,0)
 
    #print("Mall Info: ", mall_info)

    players_info=[]
    player_info={}

    num_screens=0
    comments=""
    
    sql_select = "SELECT mall_id, display_unit_container_id, player_container_id, player_screens, latitude, longitude, address from players where mall_id=%s" % (mall_info['id'])
    mycursor.execute(sql_select)
    records2= mycursor.fetchall()
    for row2 in records2:  
      player_info['display_unit_container_id']=row2[1]
      player_info['player_container_id']=row2[2]
      player_info['player_screens']=row2[3]
      player_info['latitude']=row2[4]
      player_info['longitude']=row2[5]
      latitude=player_info['latitude']
      longitude=player_info['longitude']
      address= row2[6]
      num_screens=num_screens + player_info['player_screens']
      players_info.append(player_info)
    
    #print("Players: ", players_info)

    screen_visibility_index = 1

    if num_screens>0:

      try:
        if num_screens==mall_info['screens_type1_high_visibility'] + mall_info['screens_type2_default_visibility'] + mall_info['screens_type3_low_visibility']:
          None
          #screen_visibility_index = round(((mall_info['modifier_high_vis']*mall_info['screens_type1_high_visibility']) + mall_info['screens_type2_default_visibility'] + (mall_info['modifier_low_vis']*mall_info['screens_type3_low_visibility']))/num_screens, 1)
        else:
          mall_info['screens_type3_low_visibility']=0
          mall_info['screens_type2_default_visibility']= num_screens
          mall_info['screens_type1_high_visibility']= 0 
          
      except: 
          mall_info['screens_type3_low_visibility']=0
          mall_info['screens_type2_default_visibility']= num_screens
          mall_info['screens_type1_high_visibility']= 0 
 

    if len(players_info)==0:
      comments= "No players in mall"
    
    mall_info['num_display_units']=len(players_info)
    
    sql= "update malls set  players=%s,  address=%s, screens=%s, screens_type1_high_visibility=%s, screens_type2_default_visibility=%s, screens_type3_low_visibility=%s,  num_display_units=%s, geolocation_lat=%s, geolocation_long=%s, opening_hours=%s, closing_hours=%s, screen_density=%s, comments=%s, mall_size=%s, screen_exposure_area=%s where id=%s"
    val= ( len(records2) , address,  num_screens,mall_info['screens_type1_high_visibility'], mall_info['screens_type2_default_visibility'], mall_info['screens_type3_low_visibility'], len(players_info),latitude, longitude, "10:00", "22:00", round((num_screens / mall_info['screen_exposure_area']) * 1000, 2), comments, mall_info['mall_size'], mall_info['screen_exposure_area'], mall_info['id'])
    mycursor.execute(sql, val)
    mydb.commit()


