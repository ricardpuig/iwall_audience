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

df_hivestack_db_players=pd.read_sql('hivestack_screens', engine)
df_hivestack_platform_players=pd.read_csv(sys.argv[1])

print(df_hivestack_db_players)
print(df_hivestack_platform_players)

#export hivestack platform ids
screen_ids = df_hivestack_platform_players['screen ID'].tolist()
print(screen_ids)

missing_screens=[]

# loop through the rows using iterrows()
for index, row in df_hivestack_db_players.iterrows():
    if int(row['Screen ID']) in screen_ids:
        None
    else:
        #print("Screen with ID  ", row['Screen ID'], ", and name ", row['Screen name'], " not in hivestack platform")
        missing_screens.append(row)

df = pd.DataFrame(missing_screens)
df.to_csv('missing_players.csv')



