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

sql_select = "SELECT * from hivestack_screens"
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

reach_screen={}
mall_info={}
reach_screens_export=[]
hivestack_screens_export=[]

try:
	sql= "DELETE FROM hivestack_audience"
	mycursor.execute(sql)
	mydb.commit()
except: 
	print("Table not exists")
     
print("Generating Hivestack Audience data")

for row in records:  

    print("Generating programmatic audience data for ", row[0])
    players_info=[]
    player_info={}
    sql_select = "SELECT player_id, programmatic_name, player_screens, screen_resolution,  mall_id from players where player_id=%s" % (row[0])
    print(sql_select)
    mycursor.execute(sql_select)
    records2= mycursor.fetchall()

    for row2 in records2:  

        try:
            now = datetime.now() # current date and time
            date_time = now.strftime("%Y%m%d")

            year,week_num,day_of_week = now.isocalendar() # DOW = day of week
            week_num=week_num-2 
            if week_num<1:
                week_num=1
            print("Year %d, Week Number %d, Day of the Week %d" %(year,week_num, day_of_week))

            sql= "SELECT * FROM audience_multipliers  WHERE mall_id=%s and YEAR(date)=%s" % (row2[4], year)
            df_multipliers = pd.read_sql_query(sql, engine)

            print(df_multipliers)
            df_multipliers['date']=pd.to_datetime(df_multipliers['date'])

            #multiplicar por el numero de screens
            df_multipliers['0']=df_multipliers['0']*row2[2]
            df_multipliers['1']=df_multipliers['1']*row2[2]
            df_multipliers['2']=df_multipliers['2']*row2[2]
            df_multipliers['3']=df_multipliers['3']*row2[2]
            df_multipliers['4']=df_multipliers['4']*row2[2]
            df_multipliers['5']=df_multipliers['5']*row2[2]
            df_multipliers['6']=df_multipliers['6']*row2[2]
            df_multipliers['7']=df_multipliers['7']*row2[2]
            df_multipliers['8']=df_multipliers['8']*row2[2]
            df_multipliers['9']=df_multipliers['9']*row2[2]
            df_multipliers['10']=df_multipliers['10']*row2[2]
            df_multipliers['11']=df_multipliers['11']*row2[2]
            df_multipliers['12']=df_multipliers['12']*row2[2]
            df_multipliers['13']=df_multipliers['13']*row2[2]
            df_multipliers['14']=df_multipliers['14']*row2[2]
            df_multipliers['15']=df_multipliers['15']*row2[2]
            df_multipliers['16']=df_multipliers['16']*row2[2]
            df_multipliers['17']=df_multipliers['17']*row2[2]
            df_multipliers['18']=df_multipliers['18']*row2[2]
            df_multipliers['19']=df_multipliers['19']*row2[2]
            df_multipliers['20']=df_multipliers['20']*row2[2]
            df_multipliers['21']=df_multipliers['21']*row2[2]
            df_multipliers['22']=df_multipliers['22']*row2[2]
            df_multipliers['23']=df_multipliers['23']*row2[2]


            # Establecer la columna de fecha como índice
            df_multipliers.set_index('date', inplace=True)

            # Agrupar por día de la semana y hora, luego calcular la media
            df_multiplier_mean = df_multipliers.groupby([df_multipliers.index.dayofweek]).mean().round(2)

            
            df_multiplier_mean = df_multiplier_mean.drop('avg_multiplier', axis=1)
            df_multiplier_mean = df_multiplier_mean.drop('mall_id', axis=1)
            df_multiplier_mean = df_multiplier_mean.drop('id', axis=1)

            # Convertir el DataFrame a un arreglo de NumPy y luego aplanarlo
            datos_aplanados = df_multiplier_mean.values.flatten()

            # Crear un nuevo DataFrame con una sola fila a partir de los datos aplanados
            df_final = pd.DataFrame([datos_aplanados], columns=[f'Valor_{i+1}' for i in range(datos_aplanados.shape[0])])

            df_final['screen_id']=row[0]
            print(df_final)
            df_final.to_sql('hivestack_audience', engine, if_exists='append', index=False)
        except: 
             print("audience export error")
    


       
