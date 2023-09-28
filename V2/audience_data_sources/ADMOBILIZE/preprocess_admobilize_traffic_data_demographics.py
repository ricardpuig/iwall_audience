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
import os



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
	directory= sys.argv[1]
else:
	print("Folder to process missing, exiting....")
	exit(1)

print("Removing duplicates / old data")
#removing duplicated rows in case of ingesting same data
query="delete FROM admobilize_data"
mycursor.execute(query)
mydb.commit()

# iterate over files in
# that directory
print("Admobilize data Directory ", directory)

for path, subdirs, files in os.walk(directory):
  for name in files:
      print(name)
      f=path+"/"+name
      print(" analyzing file ", f)


      if "facev2" in name: 
        df_admobilize = pd.read_csv(f, delimiter=",", decimal=".", encoding_errors='ignore' )
        print(df_admobilize)
        
        df_admobilize=df_admobilize.drop(['mask','zoneId', 'direction'], axis=1)
        df_admobilize.to_sql('admobilize_data', engine, if_exists='append', index=False)

