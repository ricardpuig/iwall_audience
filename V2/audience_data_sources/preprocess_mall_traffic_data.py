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
	print("File tro process missing, exiting....")
	exit(1)

df = pd.read_csv(file_to_process, delimiter=";", thousands=".", decimal=",")
print(df)
df['mall_visits']=df['mall_visits'].str.replace('.', '').astype(int)
print(df.dtypes)
print("Inserting into database")
df.to_sql('mall_traffic_data', engine, if_exists='append', index=False)

print("Removing duplicates / old data")
#removing duplicated rows in case of ingesting same data
query="delete t1 FROM mall_traffic_data t1 INNER  JOIN mall_traffic_data t2 WHERE t1.id< t2.id  AND t1.start_date = t2.start_date AND t1.end_date = t2.end_date AND t1.mall_id = t2.mall_id;"
mycursor.execute(query)
mydb.commit()