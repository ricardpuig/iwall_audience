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

sql_select = "SELECT name, id from malls"
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

malls=[]
mall={}

for row in records:  
    print(row)
    mall={}
    mall['name']=row[0]
    mall['id']=row[1]
    malls.append(mall)

df_malls = pd.DataFrame(malls)
print(df_malls)
df_malls.to_csv('mall_ids.csv', index=False)



