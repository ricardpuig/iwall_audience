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
query="delete FROM inspide_data_ages"
mycursor.execute(query)
mydb.commit()


print("Removing duplicates / old data")
#removing duplicated rows in case of ingesting same data
query="delete FROM inspide_data_genders"
mycursor.execute(query)
mydb.commit()

print("Removing duplicates / old data")
#removing duplicated rows in case of ingesting same data
query="delete FROM inspide_data_incomes"
mycursor.execute(query)
mydb.commit()


print("Removing duplicates / old data")
#removing duplicated rows in case of ingesting same data
query="delete FROM inspide_data"
mycursor.execute(query)
mydb.commit()

# iterate over files in
# that directory
print("Directory ", directory)

for path, subdirs, files in os.walk(directory):
  for name in files:
      print(name)
      
      f=path+"/"+name

      print(f)
      

      if "visits" in name: 
        filename_split=name.split("_")
        print("Week name: ", filename_split[1])
        print("processing visits file")
        df_inspide = pd.read_csv(f, delimiter=";", decimal=".")
        df_inspide['country']="SPAIN"
        df_inspide['start_date']=df_inspide['day']+ " "+df_inspide['start_hour']
        df_inspide['end_date']=df_inspide['day']+ " "+df_inspide['end_hour']
        df_inspide['country']="SPAIN"
        df_inspide['period']="HOUR"
        df_inspide['mall_name']=df_inspide['name']
        df_inspide['mall_id']=df_inspide['id']
        print(df_inspide)
        df_inspide=df_inspide.drop(['id','uuid','day','name', 'start_hour', 'end_hour'], axis=1)
        df_inspide.to_sql('inspide_data', engine, if_exists='append', index=False)



      if "ages" in name:
        filename_split=name.split("_")
        print("Week name: ", filename_split[1])
        print("processing ages file")
        df_inspide = pd.read_csv(f, delimiter=";", decimal=".")
        df_inspide['country']="SPAIN"
        df_inspide['week']=int(filename_split[1])
        df_inspide['mall_name']=df_inspide['name']
        df_inspide['mall_id']=df_inspide['id']
        df_inspide=df_inspide.drop(['uuid','name'], axis=1)
        df_inspide=df_inspide.drop(['id'], axis=1)
        print(df_inspide)
        print("Inserting into database")
        df_inspide.to_sql('inspide_data_ages', engine, if_exists='append', index=False)

    
      if "genders" in name:
        print("processing genders file")
        filename_split=name.split("_")
        print("Week name: ", filename_split[1])
        df_inspide = pd.read_csv(f, delimiter=";", decimal=".")
        df_inspide['country']="SPAIN"
        df_inspide['week']=int(filename_split[1])
        df_inspide['mall_name']=df_inspide['name']
        
        df_inspide=df_inspide.drop(['uuid','name'], axis=1)
        print(df_inspide)
        

        sql= "SELECT id, name from malls "
        df_malls = pd.read_sql_query(sql, engine)
        print(df_malls)

        if not 'id' in df_inspide:
          df_merged=df_inspide.merge(df_malls, how='cross')
          print(df_merged) 
          mask = df_merged.apply(lambda x: (re.search(rf"\b{x['name']}\b", str(x['mall_name']))) != None, axis=1)
          print(mask)
          df_out = df_merged.loc[mask]
          print(df_out)

          df_out['mall_id']=df_out['id']
          df_out=df_out.drop(['id','name'], axis=1)
          print(df_out)
          print("Inserting into database")
          df_out.to_sql('inspide_data_genders', engine, if_exists='append', index=False)
        else:
          df_inspide['mall_id']=df_inspide['id']
          df_inspide=df_inspide.drop(['id'], axis=1)
          print("Inserting into database")
          df_inspide.to_sql('inspide_data_genders', engine, if_exists='append', index=False)


      if "incomes" in name:
        print("processing incomes file")
        filename_split=name.split("_")
        print("Week name: ", filename_split[1])
        df_inspide = pd.read_csv(f, delimiter=";", decimal=".")
        df_inspide['country']="SPAIN"
        df_inspide['week']=int(filename_split[1])
        df_inspide['mall_name']=df_inspide['name']
        df_inspide['mall_id']=df_inspide['id']
        df_inspide=df_inspide.drop(['uuid','name'], axis=1)
        df_inspide=df_inspide.drop(['id'], axis=1)
        print(df_inspide)
        print("Inserting into database")
        df_inspide.to_sql('inspide_data_incomes', engine, if_exists='append', index=False)
        print("processing incomes file")

    



