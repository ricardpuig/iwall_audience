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


import pandas as pd

def transform_day_hour_dataframe(df, id_vars):
    """
    Transforma un DataFrame que tiene una estructura de días y horas
    en un formato donde cada fila es una combinación específica de día y hora.
    
    Parámetros:
    - df: DataFrame a transformar.
    - id_vars: Lista de columnas que identifican cada fila (por ejemplo, ['date']).
    
    Retorna:
    - DataFrame transformado con tres columnas: día, hora y valor.
    """
    # Realizar el melt para convertir las columnas de horas en filas
    df_melted = df.melt(id_vars=id_vars, var_name='hour', value_name='value')
    
    # Convertir la hora a entero para ordenar correctamente
    df_melted['hour'] = df_melted['hour'].astype(int)
    
    # Ordenar por día de la semana y hora para una mejor visualización
    df_melted.sort_values(by=id_vars + ['hour'], inplace=True)
    
    # Reseteo del índice del DataFrame resultante
    df_melted.reset_index(drop=True, inplace=True)
    
    return df_melted



def modificar_valor(row):
    if row['B'] > 20:
        return row['A'] * 2
    else:
        return row['A'] - 1





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

sql_select = "SELECT internal_publisher_screen_id from reach_screens"
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

reach_screen={}
mall_info={}
reach_screens_export=[]
hivestack_screens_export=[]

try:
	sql= "DELETE FROM reach_audience"
	mycursor.execute(sql)
	mydb.commit()
except: 
	print("Table not exists")
     
print("Generating Reach Audience data")

for row in records:  

    try:

        print("Generating programmatic audience data for ", row[0])
        players_info=[]
        player_info={}

        
        sql_select = "SELECT player_id, programmatic_name, player_screens, screen_resolution, mall_id from players where player_id=%s" % (row[0])
        print(sql_select)
        mycursor.execute(sql_select)
        records2= mycursor.fetchall()

        for row2 in records2:  

            now = datetime.now() # current date and time
            date_time = now.strftime("%Y%m%d")

            year,week_num,day_of_week = now.isocalendar() # DOW = day of week
            week_num=week_num-2 
            if week_num<1:
                week_num=1
            print("Year %d, Week Number %d, Day of the Week %d" %(year,week_num, day_of_week))

            sql= "SELECT * FROM audience_multipliers  WHERE mall_id=%s and YEAR(date)=%s" % (row2[4], year)
            df_multipliers = pd.read_sql_query(sql, engine)

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



            df_multiplier_mean.reset_index(inplace=True)    
            print(df_multiplier_mean)
            df_multiplier_mean.to_csv('multiplicadores.csv', index=True)

            # Ejemplo de uso de la función
            # Supongamos que tienes un DataFrame llamado 'df_original' que deseas transformar
            df_reach_screen = transform_day_hour_dataframe(df_multiplier_mean, id_vars=['date'])


            df_reach_screen['screen_id']="broadsign.com:"+str(row2[0])
            df_reach_screen['start_date']="2020-01-01"
            df_reach_screen['end_date']="2025-12-31"
            df_reach_screen['start_time']="10:00:00"
            df_reach_screen['end_time']="21:59:59"
            df_reach_screen['mon']=1 
            df_reach_screen['tue']=1 
            df_reach_screen['wed']=1 
            df_reach_screen['thu']=1 
            df_reach_screen['fri']=1 
            df_reach_screen['sat']=1
            df_reach_screen['sun']=1 
            df_reach_screen['demography_type']="basic"

            df_reach_screen.rename(columns={'value': 'total'}, inplace=True)


            df_reach_screen['mon'] = df_reach_screen.apply(lambda row: 1 if row['date'] == 0 else 0, axis=1)
            df_reach_screen['tue'] = df_reach_screen.apply(lambda row: 1 if row['date'] == 1 else 0, axis=1)
            df_reach_screen['wed'] = df_reach_screen.apply(lambda row: 1 if row['date'] == 2 else 0, axis=1)
            df_reach_screen['thu'] = df_reach_screen.apply(lambda row: 1 if row['date'] == 3 else 0, axis=1)
            df_reach_screen['fri'] = df_reach_screen.apply(lambda row: 1 if row['date'] == 4 else 0, axis=1)
            df_reach_screen['sat'] = df_reach_screen.apply(lambda row: 1 if row['date'] == 5 else 0, axis=1)
            df_reach_screen['sun'] = df_reach_screen.apply(lambda row: 1 if row['date'] == 6 else 0, axis=1)


            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: '00:00:00' if row['hour'] == 0 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "01:00:00" if row['hour'] == 1 else row['start_time'] , axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "02:00:00" if row['hour'] == 2 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "03:00:00" if row['hour'] == 3 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "04:00:00" if row['hour'] == 4 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "05:00:00" if row['hour'] == 5 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "06:00:00" if row['hour'] == 6 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "07:00:00" if row['hour'] == 7 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "08:00:00" if row['hour'] == 8 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "09:00:00" if row['hour'] == 9 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "10:00:00" if row['hour'] == 10 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "11:00:00" if row['hour'] == 11 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "12:00:00" if row['hour'] == 12 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "13:00:00" if row['hour'] == 13 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "14:00:00" if row['hour'] == 14 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "15:00:00" if row['hour'] == 15 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "16:00:00" if row['hour'] == 16 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "17:00:00" if row['hour'] == 17 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "18:00:00" if row['hour'] == 18 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "19:00:00" if row['hour'] == 19 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "20:00:00" if row['hour'] == 20 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "21:00:00" if row['hour'] == 21 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "22:00:00" if row['hour'] == 22 else row['start_time'], axis=1)
            df_reach_screen['start_time'] = df_reach_screen.apply(lambda row: "23:00:00" if row['hour'] == 23 else row['start_time'], axis=1)
            

            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: '00:59:59' if row['hour'] == 0 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "01:59:59" if row['hour'] == 1 else row['end_time'] , axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "02:59:59" if row['hour'] == 2 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "03:59:59" if row['hour'] == 3 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "04:59:59" if row['hour'] == 4 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "05:59:59" if row['hour'] == 5 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "06:59:59" if row['hour'] == 6 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "07:59:59" if row['hour'] == 7 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "08:59:59" if row['hour'] == 8 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "09:59:59" if row['hour'] == 9 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "10:59:59" if row['hour'] == 10 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "11:59:59" if row['hour'] == 11 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "12:59:59" if row['hour'] == 12 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "13:59:59" if row['hour'] == 13 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "14:59:59" if row['hour'] == 14 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "15:59:59" if row['hour'] == 15 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "16:59:59" if row['hour'] == 16 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "17:59:59" if row['hour'] == 17 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "18:59:59" if row['hour'] == 18 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "19:59:59" if row['hour'] == 19 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "20:59:59" if row['hour'] == 20 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "21:59:59" if row['hour'] == 21 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "22:59:59" if row['hour'] == 22 else row['end_time'], axis=1)
            df_reach_screen['end_time'] = df_reach_screen.apply(lambda row: "23:59:59" if row['hour'] == 23 else row['end_time'], axis=1)

            df_reach_screen['male']=""
            df_reach_screen['females']=""

            df_reach_screen['males_12']=""
            df_reach_screen['males_18']=""
            df_reach_screen['males_25']=""
            df_reach_screen['males_35']=""
            df_reach_screen['males_45']=""
            df_reach_screen['males_55']=""
            df_reach_screen['males_65']=""
            df_reach_screen['females_12']=""
            df_reach_screen['females_18']=""
            df_reach_screen['females_25']=""
            df_reach_screen['females_35']=""
            df_reach_screen['females_45']=""
            df_reach_screen['females_55']=""
            df_reach_screen['females_65']=""
            
            df_reach_screen = df_reach_screen.drop('date', axis=1)
            df_reach_screen = df_reach_screen.drop('hour', axis=1)

            # Imprimir las primeras filas del DataFrame transformado para verificar
            print(df_reach_screen)

            df_reach_screen.to_sql('reach_audience', engine, if_exists='append', index=False)

    except: 
         
        print("Error creating audience data")









