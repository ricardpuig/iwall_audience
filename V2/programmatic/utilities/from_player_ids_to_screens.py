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
	file_to_process= sys.argv[1]
else:
	print("File to process missing, exiting....")
	exit(1)

df = pd.read_csv(file_to_process, delimiter=";", thousands=".", decimal=",")
print(df)
for index, row in df.iterrows():
    sql= "SELECT player_name, container_name FROM players WHERE player_id=%s" % (row['player_id'])
    mycursor.execute(sql)
    records1= mycursor.fetchall()
    
    if records1:
        print("Screen:  ",row['player_id'], " ", records1)
    else:
        print("Screen:  ",row['player_id'], " no record")