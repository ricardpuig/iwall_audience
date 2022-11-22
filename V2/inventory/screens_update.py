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
from pandasgui import show
from trycourier import Courier 

def screen_update_report(message_to_send):

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
							"message_type" : "Screen Inventory Update: ",
							"message": message_to_send
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

if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

print("Generating Screen information from players")

sql_select = "SELECT * from players"
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

screens=[]
screen_data={}

screen_counter=0

for row in records:  
    print(row)
    print("Num screens: ", row[11])
    num_screens=row[11]
    print("Latitude", row[21])
    print("Longitude", row[22])

    mall_id=row[27]
    
    #input()
    sql_select = "SELECT name from malls where id ={id}".format(id=mall_id)
    mycursor.execute(sql_select)
    records= mycursor.fetchall()
    mall_name=""
    for row2 in records:  
        mall_name=row2[0]


    for index in range(num_screens):

        screen_data={}
        screen_data['screen_id']="IWALL-"+str(screen_counter)
        screen_data["mall_id"]=mall_id
        screen_data["player_id"]=row[0]
        screen_data['latitude']=row[21]
        screen_data['longitude']=row[22]
        screen_data['size']=65
        screen_data['visibility_index']=1 
        screen_data['player_name']=row[10]
        screen_data['mall_name']=mall_name
        screen_counter=screen_counter+1
           
        screens.append(screen_data)    

df_screens = pd.DataFrame(screens)
msg="Total Screens "+ country + " : " +str(screen_counter +1)
screen_update_report(msg)
#show(df_screens)
try:
    query="DELETE FROM screens"
    mycursor.execute(query)
    mydb.commit()
except:
    None
df_screens.to_sql('screens', engine, if_exists='append', index=False)


#send to DB




