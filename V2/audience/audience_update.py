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
from dateutil.relativedelta import relativedelta
from sklearn import preprocessing
from statistics import mean
import math


OPENING_HOURS= 12
DENSITY_MULTIPLIER= 0.18
DEFAULT_DAILY_IMPACTS = 10000
AD_SLOT_DURATION = 10
K_FACTOR = 0.7   #FACTOR SPAIN 
ADMOBILIZE_FACTOR = 1
WALKING_SPEED_KMH = 0.5  #in km/h
CORRIDOR_WIDTH = 8

LARGE_MALL_EXPOSURE_AREA_THRESHOLD = 6000
SMALL_MALL_EXPOSURE_AREA_THRESHOLD = 4000

MALL_VISIT_TIME_MEDIUM = 45
MALL_VISIT_TIME_LARGE = 90
MALL_VISIT_TIME_SMALL = 30

START_DATE = "2023-01-01"
END_DATE = "2024-12-31"
YEAR = 2024

UPDATE_MALL_MODEL=True

hourly=[]
weekly=[]
weekday=[]
impressions_model=[]
impressions_hour=0

def hourly_impact_calculation(hourly_visits, screen_density, screen_visibility_index, exposure_area, mall_visit_time_min):
  
  #modelo actual 
  #asumimos que la persona se mueve a una velocidad de 1km/h. puede cubrir hasta 1000m de trayecto en 1hora en todo el mall

  #durante su estancia en el centro comercial de media. 
  
  print("mall visit time ( in min ): ", mall_visit_time_min)

  travelled_meters = WALKING_SPEED_KMH * 1000
  travelled_surface = travelled_meters * CORRIDOR_WIDTH

  print("Travelled meters ", travelled_meters)
  print("Travelled surface", travelled_surface, "exposure area ", exposure_area)
  

  exposure= hourly_visits*(travelled_surface/exposure_area)*screen_visibility_index

  print("Hourly ", hourly_visits , " Exposure ", exposure)
  print ("\n")


  return round(exposure,0)

def hourly_admobilize_impact_calculation(hourly_ots, screen_density, screen_visibility_index, exposure_area, mall_visit_time_min):
  
  #use the screen visilbility index for the mall as exposure modifier
  exposure= hourly_ots*screen_visibility_index
  return round(exposure,0)

def compute_screen_visibility_index(total_screens,low_vis, default_vis, high_vis):
  #finds index to apply to impact calculations
  screen_visibility_index = 1
  if low_vis/total_screens>0.5:
    screen_visibility_index = 0.5
  if default_vis/total_screens>0.5:
    screen_visibility_index = 1
  if high_vis/total_screens>0.5:
    screen_visibility_index = 1.5

  return screen_visibility_index

def lambda_model1(x):  
    global hourly  
    return(x['default_hourly_impacts']*hourly[x['hour']])

def lambda_model2(x):  
    global weekday
    return(x['impacts_hour']*weekday[x['day_of_week']])

def lambda_model3(x):  
    global weekly
    return(x['impacts_hour2']*weekly[x['week_of_year']-1])

def lambda_model4(x):
    if  np.isnan(x['inspide_impacts']):
      return x['default_impacts']
    else:
      return x['inspide_impacts']

def lambda_model5(x):
    if  np.isnan(x['inspide_impacts']):
      return x['source_data_x']
    else:
      return x['source_data_y']
    
def lambda_model6(x):
    if  np.isnan(x['camera_ots']):
      return x['default_impacts']
    else:
      return x['camera_ots']

def lambda_model7(x):
    if  np.isnan(x['camera_ots']):
      return x['source_data_x']
    else:
      return "admobilize data"
    
def lambda_impressions(x):
    global impressions_model
    global impressions_hour
    week_of_year=x['week_of_year']
    day_of_week=x['day_of_week']

    #find   
    impacts=list(filter(lambda item: item['week_of_year'] == week_of_year, impressions_model))
    impacts_2=list(filter(lambda item: item['day_of_week'] == day_of_week, impacts))
    impacts_3=list(filter(lambda item: item['hour'] == impressions_hour, impacts_2))    
    return(impacts_3[0]['impacts'])

def lambda_multipliers(x):
    global impressions_model
    global impressions_hour
    week_of_year=x['week_of_year']
    day_of_week=x['day_of_week']

    #find   
    impacts=list(filter(lambda item: item['week_of_year'] == week_of_year, impressions_model))
    impacts_2=list(filter(lambda item: item['day_of_week'] == day_of_week, impacts))
    impacts_3=list(filter(lambda item: item['hour'] == impressions_hour, impacts_2))    
    return(impacts_3[0]['impression_multiplier'])

#SQL connect
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

#update report
update_report = []

numero_de_argumentos = len(sys.argv) - 1

if numero_de_argumentos == 1:
    country= sys.argv[1]
elif numero_de_argumentos == 2:
    country= sys.argv[1]
    mall_id= sys.argv[2]
else:
    print(f"Argumentos erroneos")


if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

try:
  if len((sys.argv -1 )==2):
    mall_id= sys.argv[2]
except:
  print("Analyzin all country")

print("Starting Audience Update script")


#loop through all sites in DB for the country 
if numero_de_argumentos == 2:
  sql= "SELECT DISTINCT id,name FROM malls where active=1 and country='%s' and id='%s'" % (country, mall_id)
elif numero_de_argumentos == 1:
  sql= "SELECT DISTINCT id,name FROM malls where active=1 and country='%s'" % (country)


#sql= "SELECT DISTINCT id,name FROM malls where active=1 and id=88"
mycursor.execute(sql)
records= mycursor.fetchall()



for row in records:  #for each result
   

    print("\n")
    print("----------------------------------------------------------")
    print("----------------------------------------------------------")
    print("Updating Audience data for mall", row[1])
    print("----------------------------------------------------------")
    print("----------------------------------------------------------")
    
    # default values
    multiplicador_medio = 1
    mall_name=row[1]
    available_inspide_data=False
    available_mall_data = False
    available_admobilize_data = False
    available_camera_data= False
    available_default_data = False
    flag_screen_number_error = False
    flag_updated_mall_model = False
    mall_data_estimated_daily_impacts = 0 
    inspide_daily_impacts = 0
    admobilize_data_estimated_daily_impacts = 0
    admobilize_data_estimated_hourly_impacts = 0
    screen_density = 0
    screen_visibility_index = 1
    total_screens =0


    # check if LED alias #
    alias_id=row[0]
    if row[0]==74:  #LEDS CASTELLANA 200
      alias_id=11
    if row[0]==75:  #LEDS FINESTRELLES
      alias_id=58  
    if row[0]==76:  #LEDS GRAN PLAZA 2
      alias_id=25
    if row[0]==77: # LEDS GRAN VIA 2
      alias_id=26
    if row[0]==103: #LEDS PARQUE CORREDOR
      alias_id=101
    if row[0]==78: #LEDS PORTAL DE LA MARINA
      alias_id=43
    if row[0]==80: #LEDS LA VILLA
      alias_id=61
    if row[0]==59: #ILLA GALERIA
      alias_id=2
    if row[0]==119: #LEDS PLAZA NORTE 2
      alias_id=41
    if row[0]==84: #Videowall Rambla Breña
      alias_id=83
    if row[0]==86: #Videowall Rmbla Borja
      alias_id=85
    if row[0]==135: #LED ILLA 
      alias_id=2
    if row[0]==137: #LED JAEN PLAZA
      alias_id=62
    if row[0]==141: #LED FERIAL
      alias_id=140
          
    #********************************************
    #getting mall default data ( not using alias)
    #********************************************
    sql= "SELECT name,screens, screens_type1_high_visibility, screens_type2_default_visibility, screens_type3_low_visibility, screen_exposure_area, num_locales, dwell_time, screen_visibility, avg_mall_visit_time_min, cam_avg_daily_exposure, estimation_error_ratio FROM malls  WHERE id=%s" % (row[0])
    mycursor.execute(sql)
    records_mall_data= mycursor.fetchall()
    for row_2 in records_mall_data:
      print("Mall Name: ", row_2[0], " (ID ", row[0], ")")
      print("Screen Exposure area: ",row_2[5] )
      print("Total Screens: ", row_2[1])
      print("Screens high visibility: ", row_2[2])
      print("Screens default visibility: ", row_2[3])
      print("Screens low visibility: ", row_2[4])
      print("Estimation Error ratio:  ", row_2[11])
      
      #check screens visibility if not used default
      mall_name=row_2[0]
      low_vis=row_2[4]
      default_vis = row_2[3]
      high_vis = row_2[2]
      total_screens= row_2[1]
      screen_exposure_area= row_2[5]
      screen_density= round((total_screens / screen_exposure_area) * 1000, 2)
      locales= row_2[6]
      dwell_time = row_2[7]
      screen_visibility_index = row_2[8]
      avg_mall_visit_tine_min= row_2[9]
      cam_avg_daily_exposure = row_2[10]
      estimation_error_ratio= row_2[11]

      print("Avg mall visit time (in min):", avg_mall_visit_tine_min)
      print("Screens Density ( screens every 1000m2): ", screen_density, " locales: ", locales, " Dwell (sec): ", dwell_time)

      #check num of screens available
      try:
        if total_screens == (low_vis + default_vis + high_vis):
          print("Screens visibility index:", screen_visibility_index)
        else: 
          print("error counting screens")
          flag_screen_number_error = True
      except: 
          print("Error - num of screens for mall ",row[0])
          flag_screen_number_error = True
       

  
    #***********************************
    #inspide data availability
    #***********************************
    print("Getting available inspide data for the mall")
    sql= "SELECT visits, period, start_date, end_date FROM inspide_data  WHERE mall_id=%s and DATE(start_date)>='2023-01-01'" % (alias_id)
    df_inspide = pd.read_sql_query(sql, engine)

    if len(df_inspide)>0:
        print("Success: * hourly inspide data available")
        available_inspide_data=True
        #remove hours with visits 0 
        
        df_inspide_temp = df_inspide.drop(df_inspide[df_inspide.visits < 1].index)
        inspide_hourly_mean= df_inspide_temp['visits'].mean()


        sql= "update malls set inspide_hourly_avg_visits=%s, inspide_data=1  where id=%s"
        val= (int(inspide_hourly_mean), row[0])
        mycursor.execute(sql,val)
        mydb.commit()

        sql= "update malls set inspide_daily_avg_visits=%s, inspide_data=1 where id=%s"
        val= (int(inspide_hourly_mean*6),row[0])
        mycursor.execute(sql,val)
        mydb.commit()

        #IMPACT FORIMULA
        inspide_hourly_impacts = hourly_impact_calculation(inspide_hourly_mean, screen_density, screen_visibility_index,screen_exposure_area,avg_mall_visit_tine_min )
        inspide_daily_impacts = inspide_hourly_impacts * OPENING_HOURS

        #preprocess inspide data
        print("\nInspide statistics: ")
        print("---------------------")
        print("- Visits  - Inspide Hourly mean ", inspide_hourly_mean)
        print("- Estimated hourly Impacts: ", inspide_hourly_impacts)
        print("- Estimated daily Impacts: ",inspide_daily_impacts )

        print("\n")
    else:
        print("Note: inspide data not available!")
    

    #*************************************************
    #update demographics data if available fom inspide
    #*************************************************
    print("Getting available demographics inspide data for the mall")
    sql= "SELECT p_f_tot, p_m_tot FROM inspide_data_genders  WHERE mall_id=%s" % (alias_id)
    df_inspide_dem = pd.read_sql_query(sql, engine)
    if len(df_inspide_dem)>0:
      print("Success: * demographics inspide data available")  
      inspide_male_mean= df_inspide_dem['p_m_tot'].mean()
      inspide_female_mean=df_inspide_dem['p_f_tot'].mean()


      print("Updating default data with inspide GENDER  new averages")
      sql= "update malls set default_dem_male=%s, default_dem_female=%s where id=%s"
      val= (float(inspide_male_mean/100),float(inspide_female_mean/100) , row[0])
      mycursor.execute(sql,val)
      mydb.commit()
    else:
      print("Demographics inspide data (gender) not available")


    print("Getting available demographics inspide data for the mall")
    sql= "SELECT pop_14_18, pop_18_25, pop_25_30, pop_30_50,pop_50_70,pop_70_90 FROM inspide_data_ages  WHERE mall_id=%s" % (alias_id)
    df_inspide_dem = pd.read_sql_query(sql, engine)
    if len(df_inspide_dem)>0:
      print("Success: * demographics inspide data available")  
      inspide_18= df_inspide_dem['pop_14_18'].mean()
      inspide_25= df_inspide_dem['pop_18_25'].mean()
      inspide_30= df_inspide_dem['pop_25_30'].mean()
      inspide_50= df_inspide_dem['pop_30_50'].mean()
      inspide_70= df_inspide_dem['pop_50_70'].mean()
      inspide_90= df_inspide_dem['pop_70_90'].mean()

      print("Updating default data with inspide AGE 1 classification new averages")
      sql= "update malls set default_age_kid=%s, default_age_young=%s,default_age_adult=%s, default_age_senior=%s  where id=%s"
      val= (float(inspide_18/100),float((inspide_18+inspide_25 + inspide_30)/100), float((inspide_50+inspide_70)/100), float(inspide_90/100) , row[0])
      mycursor.execute(sql,val)
      mydb.commit()

      print("Updating default data with inspide AGE 2 classification new averages")
      sql= "update malls set default_age_14_18=%s, default_age_18_25=%s,default_age_25_30=%s, default_age_30_50=%s , default_age_50_70=%s , default_age_70_90=%s where id=%s"
      val= (float(inspide_18/100),float(inspide_25/100), float(inspide_30/100) , float(inspide_50/100), float(inspide_70/100) , float(inspide_90/100), row[0])
      mycursor.execute(sql,val)
      mydb.commit()

    else:
      print("Demographics inspide data (ages) not available")


    print("Getting available demographics inspide INCOMES data for the mall")
    sql= "SELECT low, medium_low,medium,medium_high, high FROM inspide_data_incomes  WHERE mall_id=%s" % (alias_id)
    df_inspide_dem = pd.read_sql_query(sql, engine)
    if len(df_inspide_dem)>0:
      print("Success: * demographics inspide data available")  
      inspide_D= df_inspide_dem['low'].mean()
      inspide_C= df_inspide_dem['medium_low'].mean()
      inspide_B= df_inspide_dem['medium_high'].mean()+ df_inspide_dem['medium'].mean()
      inspide_A= df_inspide_dem['high'].mean()
      

      print("Updating default data with inspide NSE new averages")
      sql= "update malls set nse_A=%s, nse_B=%s,nse_C=%s, nse_D=%s  where id=%s"
      val= (float(inspide_A/100),float(inspide_B/100) , float(inspide_C/100),  float(inspide_D/100), row[0])
      mycursor.execute(sql,val)
      mydb.commit()
    else:
      print("Demographics inspide data (NSE) not available")

    print("\n")



    #***********************************
    #mall traffic source data
    #***********************************
    print("Getting mall traffic data for the mall:")

    sql= "SELECT mall_visits, period, start_date, end_date FROM mall_traffic_data  WHERE mall_id=%s" % (alias_id)
    df_mall_traffic_data = pd.read_sql_query(sql, engine)

    if len(df_mall_traffic_data)>0:
      print("Success: * mall traffic data available: ")
      available_mall_data=True
      #remove hours with visits 0 
      df_mall_traffic_data = df_mall_traffic_data.drop(df_mall_traffic_data[df_mall_traffic_data.mall_visits < 50].index)
      #print(df_mall_traffic_data)

      #check traffic period 
      print("   Hourly data points: ", sum(df_mall_traffic_data['period'] == 'HOURLY'))
      print("   Daily data points: ", sum(df_mall_traffic_data['period'] == 'DAILY'))
      print("   Monthly data points: ", sum(df_mall_traffic_data['period'] == 'MONTHLY'))
      print("   Yearly data points: ", sum(df_mall_traffic_data['period'] == 'YEARLY'))
      
      mall_data_estimated_hourly_impacts= 0 
      mall_data_estimated_daily_impacts= 0

      print("\nMall footfall data statistics: ")
      print("---------------------------------")

      if sum(df_mall_traffic_data['period'] == 'HOURLY') >0:
        df_mall_traffic_data_hourly = df_mall_traffic_data.drop(df_mall_traffic_data[df_mall_traffic_data.period != 'HOURLY'].index)
        mall_data_estimated_hourly_impacts = hourly_impact_calculation( df_mall_traffic_data_hourly['mall_visits'].mean() , screen_density, screen_visibility_index, screen_exposure_area,avg_mall_visit_tine_min )
        mall_data_estimated_daily_impacts =mall_data_estimated_hourly_impacts * OPENING_HOURS
        print("- Average Mall visits (HOURLY): ", df_mall_traffic_data_hourly['mall_visits'].mean())

        sql= "update malls set  mall_footfall_data= 1, footfall_daily_avg_visits=%s, footfall_hourly_avg_visits=%s where id=%s"
        val= ( int(df_mall_traffic_data_hourly['mall_visits'].mean()*OPENING_HOURS),int(df_mall_traffic_data_hourly['mall_visits'].mean()),  row[0])
        mycursor.execute(sql,val)
        mydb.commit()
        print("Updating mall info with footfall data")



      if sum(df_mall_traffic_data['period'] == 'DAILY') >0:
        df_mall_traffic_data_daily = df_mall_traffic_data.drop(df_mall_traffic_data[df_mall_traffic_data.period != 'DAILY'].index)
        mall_data_estimated_hourly_impacts = hourly_impact_calculation((df_mall_traffic_data_daily['mall_visits'].mean()/OPENING_HOURS) , screen_density, screen_visibility_index, screen_exposure_area,avg_mall_visit_tine_min)
        mall_data_estimated_daily_impacts =mall_data_estimated_hourly_impacts * OPENING_HOURS
        print("- Average Mall visits (DAILY): ", df_mall_traffic_data_daily['mall_visits'].mean())
        print("- Average Mall visits (HOURLY): ", df_mall_traffic_data_daily['mall_visits'].mean()/OPENING_HOURS)

        sql= "update malls set  mall_footfall_data= 1, footfall_daily_avg_visits=%s, footfall_hourly_avg_visits=%s where id=%s"
        val= ( int(df_mall_traffic_data_daily['mall_visits'].mean()),int(df_mall_traffic_data_monthly['mall_visits'].mean()/OPENING_HOURS),  row[0])
        mycursor.execute(sql,val)
        mydb.commit()
        print("Updating mall info with footfall data")


      if sum(df_mall_traffic_data['period'] == 'MONTHLY') >0:
        df_mall_traffic_data_monthly = df_mall_traffic_data.drop(df_mall_traffic_data[df_mall_traffic_data.period != 'MONTHLY'].index)
        mall_data_estimated_hourly_impacts = hourly_impact_calculation ((df_mall_traffic_data_monthly['mall_visits'].mean()/(30*OPENING_HOURS)) , screen_density, screen_visibility_index, screen_exposure_area,avg_mall_visit_tine_min)
        mall_data_estimated_daily_impacts =mall_data_estimated_hourly_impacts * OPENING_HOURS
        print("- Average Mall visits (MONTHLY): ", df_mall_traffic_data_monthly['mall_visits'].mean())
        print("- Average Mall visits (DAILY): ", df_mall_traffic_data_monthly['mall_visits'].mean()/30)
        print("- Average Mall visits (HOURLY): ", df_mall_traffic_data_monthly['mall_visits'].mean()/(30*OPENING_HOURS))


        sql= "update malls set affluence=%s , mall_footfall_data= 1, footfall_daily_avg_visits=%s, footfall_hourly_avg_visits=%s where id=%s"
        val= (float(df_mall_traffic_data_monthly['mall_visits'].mean()*12), int(df_mall_traffic_data_monthly['mall_visits'].mean()/30),int(df_mall_traffic_data_monthly['mall_visits'].mean()/(30*OPENING_HOURS)),  row[0])
        mycursor.execute(sql,val)
        mydb.commit()
        print("Updating mall info with footfall data")
   
      if sum(df_mall_traffic_data['period'] == 'YEARLY') >0:
        df_mall_traffic_data_yearly = df_mall_traffic_data.drop(df_mall_traffic_data[df_mall_traffic_data.period != 'YEARLY'].index)
        mall_data_estimated_hourly_impacts = hourly_impact_calculation ( (df_mall_traffic_data_yearly['mall_visits'].mean()/(365*OPENING_HOURS)) , screen_density, screen_visibility_index, screen_exposure_area,avg_mall_visit_tine_min)
        mall_data_estimated_daily_impacts =mall_data_estimated_hourly_impacts * OPENING_HOURS
        print("- Average Mall visits (YEARLY): ", df_mall_traffic_data_yearly['mall_visits'].mean())
        print("- Average Mall visits (DAILY): ", df_mall_traffic_data_yearly['mall_visits'].mean()/365)
        print("- Average Mall visits (HOURLY): ", df_mall_traffic_data_yearly['mall_visits'].mean()/(365*OPENING_HOURS))


        sql= "update malls set affluence=%s , mall_footfall_data= 1, footfall_daily_avg_visits=%s, footfall_hourly_avg_visits=%s  where id=%s"
        val= (float(df_mall_traffic_data_yearly['mall_visits'].mean()), int(df_mall_traffic_data_yearly['mall_visits'].mean()/365), int(df_mall_traffic_data_yearly['mall_visits'].mean()/(365*OPENING_HOURS)) ,row[0])
        mycursor.execute(sql,val)
        mydb.commit()
        print("Updating mall info with footfall data")

      print("- Mall footfall data Avg hourly Impacts: ", round(mall_data_estimated_hourly_impacts, 0))
      print("- Mall footfall data Avg daily Impacts: ",round(mall_data_estimated_daily_impacts,0) )

      print("\n")

    else:
      print("Note: mall traffic data not available!")
  
    #***********************************
    #admobilize source data
    #***********************************
    print("Getting admobilize data for the mall if available...")

    sql= "SELECT deviceId, manual_adjustment  FROM admobilize_sensors  WHERE mall_id=%s" % (alias_id)
    mycursor.execute(sql)
    records_admobilize_sensors= mycursor.fetchall()


    try:
      if len(records_admobilize_sensors)>0:
        print("**** Succcess devices available")
              
        admobilize_avg_hour_ots=[]
        admobilize_avg_daily_ots=[]

        available_admobilize_data=True
        
        for admobilize_s in records_admobilize_sensors:
            manual_adjustment=admobilize_s[1]

            sql= "SELECT fecha_hora, impresiones, deviceId FROM admobilize_impression_data  WHERE deviceId='%s' and DATE(fecha_hora)>='2023-01-01'" % (admobilize_s[0])
            df_admobilize_data = pd.read_sql_query(sql, engine)
            # Convertir la columna 'fecha_hora' al tipo datetime
            df_admobilize_data['fecha_hora'] = pd.to_datetime(df_admobilize_data['fecha_hora'])

            # Filtrar para mantener solo las filas entre las 10 de la mañana y las 10 de la noche
            df_admobilize_data_filtrado = df_admobilize_data[(df_admobilize_data['fecha_hora'].dt.hour >= 10) & (df_admobilize_data['fecha_hora'].dt.hour <= 22)]
            admobilize_hourly_mean= df_admobilize_data_filtrado['impresiones'].mean()*manual_adjustment
            admobilize_daily_mean= admobilize_hourly_mean*OPENING_HOURS
            admobilize_avg_daily_ots.append(int(admobilize_daily_mean))
            admobilize_avg_hour_ots.append(int(admobilize_hourly_mean))


        sql= "update malls set admobilize_hourly_avg_ots=%s, admobilize_data=1  where id=%s"
        val= (int(mean(admobilize_avg_hour_ots)), row[0])
        mycursor.execute(sql,val)
        mydb.commit()

        sql= "update malls set admobilize_daily_avg_ots=%s, admobilize_data=1 where id=%s"
        val= (int(mean(admobilize_avg_daily_ots)),row[0])
        mycursor.execute(sql,val)
        mydb.commit()
      
        print("- Admobilize data overall daily OTS avg: ", int(mean(admobilize_avg_daily_ots)))
        print("- Admobilize data overall hourly OTS avg: ",int(mean(admobilize_avg_hour_ots)))

        admobilize_data_estimated_hourly_impacts = hourly_admobilize_impact_calculation (int(mean(admobilize_avg_hour_ots)), screen_density, screen_visibility_index, screen_exposure_area,avg_mall_visit_tine_min)
        admobilize_data_estimated_daily_impacts =admobilize_data_estimated_hourly_impacts * OPENING_HOURS
  
        print("- Admobilize data overall hourly screen impacts: ", admobilize_data_estimated_hourly_impacts)
        print("- Admobilize data overall daily screen impacts: ",admobilize_data_estimated_daily_impacts)
        print("\n")
      
      else:
        print("No admobilize data available \n")
    except: 

      print("error processing admobilize data")

      


    #***********************************
    #getting default mall data
    #***********************************
    print("Getting default data for the mall")
    sql= "SELECT default_screen_day_impressions, default_dem_male, default_dem_female, default_age_kid, default_age_young, default_age_adult, default_age_senior, mall_model_type, mall_model_behavior  FROM malls  WHERE id=%s" % (row[0])
    mycursor.execute(sql)
    records_default_data= mycursor.fetchall()
    for row_2 in records_default_data:
      default_screen_day_impressions= row_2[0]
      default_dem_male= row_2[1]
      default_dem_female= row_2[2]
      default_age_kid= row_2[3]
      default_age_young= row_2[4]
      default_age_adult= row_2[5]
      default_age_senior= row_2[6]
      mall_model_type= row_2[7]
      mall_model_behavior= row_2[8]

      print("- Default - daily screen impressions: ", default_screen_day_impressions )
      print("- Default - Gender: ", default_dem_male, " ", default_dem_female )
      print("- Default - age ", default_age_kid, " ", default_age_young, " ", default_age_adult, " ",  default_age_senior)
      print("- Default - mall type: ", mall_model_type )
      print("- Default - mall behavior ", mall_model_behavior )
      


    #preprocess inspide data
    print("\nDefault mall statistics: ")
    print("----------------------------")
    print("- Default: Estimated hourly Impacts: ", round(default_screen_day_impressions/OPENING_HOURS, 0))
    print("- Default: Estimated daily Impacts: ",round(default_screen_day_impressions,0) )
    print("\n")


    #updating default mall data if inspide data is available, priorizing camera data
    if len(records_default_data)>0:
      print("Success: * default data available")
      available_default_data=True
      if available_inspide_data:
        print("Updating default data with inspide new averages")
        sql= "update malls set default_screen_day_impressions=%s where id=%s"
        val= (int(inspide_daily_impacts), row[0])
        mycursor.execute(sql,val)
        mydb.commit()
      elif available_admobilize_data:
        print("Updating default data with new admobilize footfall")
        sql= "update malls set default_screen_day_impressions=%s where id=%s"
        val= (int(admobilize_data_estimated_daily_impacts), row[0])
        mycursor.execute(sql,val)
        mydb.commit()
      elif available_mall_data:
        print("Updating default data with new mall footfall")
        sql= "update malls set default_screen_day_impressions=%s where id=%s"
        val= (int(mall_data_estimated_daily_impacts), row[0])
        mycursor.execute(sql,val)
        mydb.commit()
      
    else:
      print("Note: default data not available!") 


    #select daily impacts based on data availability 
    #priority : 
    # 1- INSPIDE
    # 2- ADMOBILIZE 
    # 3- MALL FOOTFALL
    # 4 -DEFAULT   
    flag_no_audience=False
    if inspide_daily_impacts > 0:
      print("using hourly impacts from inspide")
      daily_impacts = inspide_daily_impacts
      hourly_impacts= inspide_hourly_impacts
    elif admobilize_data_estimated_daily_impacts>0:
      print("using hourly impacts from admobilize")
      daily_impacts = admobilize_data_estimated_daily_impacts
      hourly_impacts=  admobilize_data_estimated_hourly_impacts
    elif mall_data_estimated_daily_impacts > 0 :
      print("using hourly impacts from footfall data")
      daily_impacts = mall_data_estimated_daily_impacts
      hourly_impacts= round(mall_data_estimated_hourly_impacts, 0)
    elif default_screen_day_impressions>0:
      print("using default as hourly impacts")
      daily_impacts = default_screen_day_impressions
      hourly_impacts= round(default_screen_day_impressions/OPENING_HOURS, 0)
    else:
      flag_no_audience=True
      print("No audience data for the mall using default ", DEFAULT_DAILY_IMPACTS)
      daily_impacts = DEFAULT_DAILY_IMPACTS
      hourly_impacts=daily_impacts/12
    
  
    #***************************************
    mall_daily_impacts = daily_impacts
    mall_hourly_impacts = hourly_impacts
    #***************************************


    #*************************************
    #if inspide data update default model  behavior
    #*************************************
    if available_inspide_data:

      print("Updating default mall model behavior using Inspide data")

      df_inspide['hour']=df_inspide['start_date'].dt.hour
      df_inspide['weekday']=df_inspide['start_date'].dt.weekday
      df_inspide['weekofyear']=df_inspide['start_date'].dt.weekofyear
      df_inspide['month']=df_inspide['start_date'].dt.month
      df_inspide['year']=df_inspide['start_date'].dt.year

      #df_inspide=df_inspide[df_inspide['month'] > 1]
      #df_inspide=df_inspide[df_inspide['year'] == 2024]
      #pd.set_option('display.max_rows', None)

      print(df_inspide)


      df_inspide_daily= df_inspide.groupby(df_inspide['weekday'])['visits'].mean()
      print("\nWeekday Model= ", df_inspide_daily)
      
      df_max_scaled_weekday = df_inspide_daily/df_inspide_daily.abs().mean()  
      print("\nScaled Weekday Model= ", df_max_scaled_weekday)
      
      df_inspide_daily= df_inspide.groupby(df_inspide['hour'])['visits'].mean()
      df_max_scaled_hour = df_inspide_daily/df_inspide_daily.abs().mean()
      #print("\nScaled Hour  Model= ", df_max_scaled_hour)

      df_inspide_daily= df_inspide.groupby(df_inspide['weekofyear'])['visits'].mean()
      df_max_scaled_weekofyear = df_inspide_daily/df_inspide_daily.abs().mean()
      #print("\nScaled weekof year  Model= ", df_max_scaled_weekofyear)

      df_inspide_daily= df_inspide.groupby(df_inspide['month'])['visits'].mean()
      df_max_scaled_month = df_inspide_daily/df_inspide_daily.abs().mean()
      #print("\nScaled month Model= ", df_max_scaled_month)

      df_inspide = df_inspide.drop('hour', axis=1)
      df_inspide = df_inspide.drop('weekday', axis=1)
      df_inspide = df_inspide.drop('weekofyear', axis=1)
      df_inspide = df_inspide.drop('month', axis=1)


      #get default model 
      query = "SELECT monthly, weekly, hourly, weekday from mall_default_models  WHERE  id = '%s'" % (row_2[7]) 
      df_default_model = pd.read_sql_query(query, engine)
        
      #get data updated data 
      model_info=df_default_model.to_dict('records')
      model=model_info[0]

      hourly_str=model['hourly']
      hourly_str=hourly_str.replace(",",".")
      hourly=hourly_str.split(':')
      hourly=list(np.float_(hourly))

      print("\nDefault Hourly model: ", hourly)

      for i, v in df_max_scaled_hour.items():
        #print('index: ', i, 'value: ', v)
        hourly[i]=v

      print("\nUpdated Hourly model: ", hourly)

      hourly_model=map(str, hourly)
      hourly_model_db = ':'.join(hourly_model)
      #print(hourly_model_db)

      weekday_str=model['weekday']
      weekday_str=weekday_str.replace(",",".")
      weekday=weekday_str.split(':')
      weekday=list(np.float_(weekday))

      print("\nnDefault Weekday model: ", weekday)

      for i, v in df_max_scaled_weekday.items():
        #print('index: ', i, 'value: ', v)
        weekday[i]=v

      print("\nUpdated Weekday model: ", weekday)

      weekday_model=map(str, weekday)
      weekday_model_db = ':'.join(weekday_model)
      #print(weekday_model_db)

      weekly_str=model['weekly']
      weekly_str=weekly_str.replace(",",".")
      weekly=weekly_str.split(':')
      weekly=list(np.float_(weekly))

      print("\nDefault Weekly model: ", weekly)

      for i, v in df_max_scaled_weekofyear.items():
        #print('index: ', i, 'value: ', v)
        weekly[i-1]=v

      print("\nUpdated Weekly model: ", weekly)

      weekly_model=map(str, weekly)
      weekly_model_db = ':'.join(weekly_model)
      #print(weekly_model_db)

      monthly_str=model['monthly']
      monthly_str=monthly_str.replace(",",".")
      monthly=monthly_str.split(':')
      monthly=list(np.float_(monthly))


      print("\nDefault monthly model: ", monthly)
      for i, v in df_max_scaled_month.items():
        #print('index: ', i, 'value: ', v)
        monthly[i-1]=v

      print("\nUpdated Monthly model: ", monthly)
  
      monthly_model=map(str, monthly)
      monthly_model_db = ':'.join(monthly_model)
      print(monthly_model_db)

      #update database with new model
      sql= "DELETE FROM mall_default_models  WHERE id=%s" % (row[0])
      mycursor.execute(sql)
      mydb.commit()

      sql= "insert into mall_default_models (name, monthly, weekly, hourly, weekday, id) values (%s, %s, %s, %s, %s, %s )"
      val= (mall_name, monthly_model_db,weekly_model_db, hourly_model_db, weekday_model_db, row[0] )
      mycursor.execute(sql, val)
      mydb.commit()



    #*************************************
    #if admobilize data available update default model behavior
    #*************************************
    if available_admobilize_data:

      if not available_inspide_data:  # only if inspide model has not been created.


        print("Updating default mall model behavior using admobilize data")
        print("Default - mall type: ", mall_model_type )
                
        df_admobilize_data['hour']=df_admobilize_data['fecha_hora'].dt.hour
        df_admobilize_data['weekday']=df_admobilize_data['fecha_hora'].dt.weekday
        df_admobilize_data['weekofyear']=df_admobilize_data['fecha_hora'].dt.weekofyear
        df_admobilize_data['month']=df_admobilize_data['fecha_hora'].dt.month
        df_admobilize_data['year']=df_admobilize_data['fecha_hora'].dt.year

        print(df_admobilize_data)

        #limpieza de datos no validos - por debajo de un 30 por ciento de la media no los tenemos en cuenta
        df_admobilize_data = df_admobilize_data.loc[df_admobilize_data['impresiones'] >= admobilize_hourly_mean*0.3]

        print(df_admobilize_data)



        df_admobilize_daily= df_admobilize_data.groupby(df_admobilize_data['weekday'])['impresiones'].mean()
        df_max_scaled_weekday = df_admobilize_daily/df_admobilize_daily.abs().mean()  
        print("\nScaled Weekday Model= ", df_max_scaled_weekday)
      
        df_admobilize_daily= df_admobilize_data.groupby(df_admobilize_data['hour'])['impresiones'].mean()
        df_max_scaled_hour = df_admobilize_daily/df_admobilize_daily.abs().mean()  
        print("\nScaled hourly Model= ", df_max_scaled_hour)
        
        df_admobilize_daily= df_admobilize_data.groupby(df_admobilize_data['weekofyear'])['impresiones'].mean()
        df_max_scaled_weekofyear = df_admobilize_daily/df_admobilize_daily.abs().mean()
        print("\nScaled weekof year  Model= ", df_max_scaled_weekofyear)

        df_admobilize_daily= df_admobilize_data.groupby(df_admobilize_data['month'])['impresiones'].mean()
        df_max_scaled_month = df_admobilize_daily/df_admobilize_daily.abs().mean()
        print("\nScaled month Model= ", df_max_scaled_month)

        df_admobilize_data = df_admobilize_data.drop('weekday', axis=1)
        df_admobilize_data = df_admobilize_data.drop('weekofyear', axis=1)
        df_admobilize_data = df_admobilize_data.drop('month', axis=1)


        #get default model 
        query = "SELECT monthly, weekly, hourly, weekday from mall_default_models  WHERE id = '%s'" % (mall_model_type) 
        df_default_model = pd.read_sql_query(query, engine)
          
        #get data updated data 
        model_info=df_default_model.to_dict('records')
        model=model_info[0]

        hourly_str=model['hourly']
        hourly_str=hourly_str.replace(",",".")
        hourly=hourly_str.split(':')
        hourly=list(np.float_(hourly))

        print("\nDefault Hourly model: ", hourly)
        print("\nUpdated Hourly model: ", hourly)

        hourly_model=map(str, hourly)
        hourly_model_db = ':'.join(hourly_model)
        #print(hourly_model_db)

        weekday_str=model['weekday']
        weekday_str=weekday_str.replace(",",".")
        weekday=weekday_str.split(':')
        weekday=list(np.float_(weekday))

        print("\nnDefault Weekday model: ", weekday)

        for i, v in df_max_scaled_weekday.items():
          #print('index: ', i, 'value: ', v)
          weekday[i]=v

        print("\nUpdated Weekday model: ", weekday)

        weekday_model=map(str, weekday)
        weekday_model_db = ':'.join(weekday_model)
        #print(weekday_model_db)



        weekly_str=model['weekly']
        weekly_str=weekly_str.replace(",",".")
        weekly=weekly_str.split(':')
        weekly=list(np.float_(weekly))

        print("\nDefault Weekly model: ", weekly)

        for i, v in df_max_scaled_weekofyear.items():
          #print('index: ', i, 'value: ', v)
          weekly[i-1]=v

        print("\nUpdated Weekly model: ", weekly)

        weekly_model=map(str, weekly)
        weekly_model_db = ':'.join(weekly_model)
        #print(weekly_model_db)

        monthly_str=model['monthly']
        monthly_str=monthly_str.replace(",",".")
        monthly=monthly_str.split(':')
        monthly=list(np.float_(monthly))


        print("\nDefault monthly model: ", monthly)
        for i, v in df_max_scaled_month.items():
          #print('index: ', i, 'value: ', v)
          monthly[i-1]=v

        print("\nUpdated Monthly model: ", monthly)
    
        monthly_model=map(str, monthly)
        monthly_model_db = ':'.join(monthly_model)
        #print(monthly_model_db)

        #update database with new model
        sql= "DELETE FROM mall_default_models  WHERE id=%s" % (row[0])
        mycursor.execute(sql)
        mydb.commit()

        sql= "insert into mall_default_models (name, monthly, weekly, hourly, weekday, id) values (%s, %s, %s, %s, %s, %s )"
        val= (mall_name, monthly_model_db,weekly_model_db, hourly_model_db, weekday_model_db, row[0] )
        mycursor.execute(sql, val)
        mydb.commit()
    
    #***********************************
    #creating mall model and impact estimation
    #***********************************
    if UPDATE_MALL_MODEL:

      print("\nUpdating MALL Model and impresion data:")
      print("\n-------------------------------------------")

      query = "DELETE from mall_models where mall_id='%s'" % (str(row[0]))
      engine.execute(query)
      mydb.commit()

      print("Screen Density: ", screen_density)
      print("density multiplier: ", DENSITY_MULTIPLIER)
      print("Screen visibility index : ", screen_visibility_index)
      
      if available_inspide_data:
        print("Hourly average impacts: ", inspide_hourly_impacts)
        print("Daily  average impacts: ", inspide_daily_impacts)

      if available_admobilize_data:
        print("Admobilize Hourly average ots: ", admobilize_data_estimated_hourly_impacts)
        print("Admobilize Daily  average ots: ", admobilize_data_estimated_daily_impacts)

      print("\nGetting mall model from DB")

      #get default model 
      query = "SELECT monthly, weekly, hourly, weekday from mall_default_models  WHERE id = %s" % (mall_model_type)
      df_default_model = pd.read_sql_query(query, engine)
      
      #si no hay modelo escogemos el por defecto 
      if len(df_default_model)==0:
          #get default model 
          query = "SELECT monthly, weekly, hourly, weekday from mall_default_models  WHERE name='default'"
          df_default_model = pd.read_sql_query(query, engine)
          print("Getting default model behaviour , model for the mail not available . Be careful all linear data accross all days!!!")
 

      model_info=df_default_model.to_dict('records')
      model=model_info[0]
      #hourly distribution update
      hourly_str= model['hourly']
      hourly_str= hourly_str.replace(",", ".")
      hourly=hourly_str.split(':')
      hourly=list(np.float_(hourly))
      #print("\nHourly model:\n", hourly)
      weekday_str=model['weekday']
      weekday_str=weekday_str.replace(",",".")
      weekday=weekday_str.split(':')
      weekday=list(np.float_(weekday))
      #print("\nWeekday model:\n", weekday)
      weekly_str=model['weekly']
      weekly_str=weekly_str.replace(",",".")
      weekly=weekly_str.split(':')
      weekly=list(np.float_(weekly))
      #print("\nWeekly model:\n ", weekly)
      monthly_str=model['monthly']
      monthly_str=monthly_str.replace(",",".")
      monthly=monthly_str.split(':')
      monthly=list(np.float_(monthly))
      #print("\nMonthly model:\n ", monthly)

      #create datafrane 
      begin_date = START_DATE
      end_date= END_DATE
      print("-> MODEL CREATION <-: Creating dates from ", begin_date , " to ", end_date)
      print("Using default impacts ( daily, hourly) ",mall_daily_impacts, mall_hourly_impacts)

      df_default_mall_model=pd.DataFrame({'mall_id' : str(row[0]), 'date':pd.date_range(start=begin_date, end=end_date, freq='H')})
      df_default_mall_model['hour']=df_default_mall_model['date'].dt.hour
      df_default_mall_model['month']=df_default_mall_model['date'].dt.month
      df_default_mall_model['year']=df_default_mall_model['date'].dt.year
      df_default_mall_model['week_of_year']=df_default_mall_model['date'].dt.weekofyear
      df_default_mall_model['day_of_week']=df_default_mall_model['date'].dt.day_of_week
      df_default_mall_model['default_hourly_impacts']=mall_hourly_impacts
      df_default_mall_model['mall_name']=mall_name
      df_default_mall_model['source_data_x']="mall_default_model"

      df_default_mall_model = df_default_mall_model.drop(df_default_mall_model[df_default_mall_model.hour < 10].index)
      df_default_mall_model = df_default_mall_model.drop(df_default_mall_model[df_default_mall_model.hour > 22].index)

      df_default_mall_model = df_default_mall_model.drop('date', axis=1)
      print(df_default_mall_model)

      #apply dayweek, monthh and hourly coorrections
      df_default_mall_model['impacts_hour']=df_default_mall_model.apply(lambda_model1,axis=1)
      df_default_mall_model['impacts_hour2']=df_default_mall_model.apply(lambda_model2,axis=1)
      df_default_mall_model['impacts_hour3']=df_default_mall_model.apply(lambda_model3,axis=1)

      if estimation_error_ratio:
        print("Current Estimation error:", estimation_error_ratio)
        df_default_mall_model['impacts_hour4']= df_default_mall_model['impacts_hour3']*estimation_error_ratio
      else:
        df_default_mall_model['impacts_hour4']= df_default_mall_model['impacts_hour3']*1


      #df_default_mall_model['impacts']=df_default_mall_model['impacts_hour3']
      df_default_mall_model['default_impacts']=df_default_mall_model['impacts_hour4'].astype(int)
      df_default_mall_model = df_default_mall_model.drop('impacts_hour', axis=1)
      df_default_mall_model = df_default_mall_model.drop('impacts_hour2', axis=1)
      df_default_mall_model = df_default_mall_model.drop('impacts_hour3', axis=1)
      df_default_mall_model = df_default_mall_model.drop('impacts_hour4', axis=1)

      #pd.set_option('display.max_rows', None)
      print(df_default_mall_model)

      df_mall_model_updated=df_default_mall_model
      df_mall_model_updated['impacts']=df_mall_model_updated['default_impacts']


      if available_admobilize_data:
        df_admobilize_data['hour']=df_admobilize_data['fecha_hora'].dt.hour      
        df_admobilize_data['month']=df_admobilize_data['fecha_hora'].dt.month
        df_admobilize_data['week_of_year']=df_admobilize_data['fecha_hora'].dt.weekofyear
        df_admobilize_data['day_of_week']=df_admobilize_data['fecha_hora'].dt.day_of_week
        df_admobilize_data['year']=df_admobilize_data['fecha_hora'].dt.year
        df_admobilize_data['camera_ots']=df_admobilize_data['impresiones']
        df_admobilize_data = df_admobilize_data.drop('impresiones', axis=1)
        df_mall_model=df_admobilize_data

        df_mall_model['mall_id']=row[0]

        df_mall_model = df_mall_model.drop('fecha_hora', axis=1)
        df_mall_model = df_mall_model.drop('deviceId', axis=1)

        df_default_mall_model['mall_id']=df_default_mall_model['mall_id'].astype(int)

        df_mall_model_updated=pd.merge(df_default_mall_model, df_mall_model, how='left', on=["mall_id", "hour", "month", "week_of_year", "day_of_week", "year"], )
        print(df_mall_model_updated)
        
        #df_mall_model_updated['camera_error']=df_mall_model_updated['camera_ots'] - df_mall_model_updated['default_impacts']
        #df_mall_model_updated['camera_ratio']=df_mall_model_updated['camera_ots']/df_mall_model_updated['default_impacts']

        df_mall_model_updated['impacts']=df_mall_model_updated.apply(lambda_model6,axis=1)
        df_mall_model_updated['source_data']=df_mall_model_updated.apply(lambda_model7,axis=1)


        print(df_mall_model_updated)
        

      if available_inspide_data:

        df_inspide['hour']=df_inspide['start_date'].dt.hour      
        df_inspide['month']=df_inspide['start_date'].dt.month
        df_inspide['week_of_year']=df_inspide['start_date'].dt.weekofyear
        df_inspide['day_of_week']=df_inspide['start_date'].dt.day_of_week
        df_inspide['year']=df_inspide['start_date'].dt.year
        df_inspide['source_data_y']="inspide"
          
        travelled_meters = WALKING_SPEED_KMH * 1000
        travelled_surface = travelled_meters * CORRIDOR_WIDTH
        
        df_inspide['impacts']= df_inspide['visits']*(travelled_surface/screen_exposure_area)*screen_visibility_index
        df_inspide['inspide_impacts']= df_inspide['impacts'].astype(int)
        df_inspide = df_inspide.drop('impacts', axis=1)


        df_mall_model=df_inspide
        df_mall_model['mall_id']=row[0]
        
        df_mall_model = df_mall_model.drop('start_date', axis=1)
        df_mall_model = df_mall_model.drop('period', axis=1)
        df_mall_model = df_mall_model.drop('end_date', axis=1)

        df_default_mall_model['mall_id']=df_default_mall_model['mall_id'].astype(int)

        #print(df_default_mall_model)

        df_mall_model_updated=pd.merge(df_default_mall_model, df_mall_model, how='left', on=["mall_id", "hour", "month", "week_of_year", "day_of_week", "year"], )
        
        df_mall_model_updated['inspide_error']=df_mall_model_updated['inspide_impacts'] - df_mall_model_updated['default_impacts']
        df_mall_model_updated['inspide_ratio']=df_mall_model_updated['inspide_impacts']/df_mall_model_updated['default_impacts']


        #----mall model error check ***********************************
        #pd.set_option('display.max_rows', None)
        print(df_mall_model_updated)
       
        df_mall_model_updated['impacts']=df_mall_model_updated.apply(lambda_model4,axis=1)
        df_mall_model_updated['source_data']=df_mall_model_updated.apply(lambda_model5,axis=1)

        #pd.set_option('display.max_rows', None)
        print(df_mall_model_updated)
                
        updated_estimation_error_ratio= df_mall_model_updated['inspide_ratio'].mean()

        if not math.isinf(updated_estimation_error_ratio):
          #solo actualizar si el error estimación > 10 por ciento 
          if updated_estimation_error_ratio>1.1 or updated_estimation_error_ratio < 0.9: 
            print("Updated Average estimation error:", updated_estimation_error_ratio, " ID ", row[0])
            sql= "update malls set estimation_error_ratio='%s'  where id='%s'"
            val= (updated_estimation_error_ratio, row[0])
            mycursor.execute(sql,val)
            mydb.commit()
            input()
        else:
          updated_estimation_error_ratio=1
          df_mall_model_updated['inspide_ratio']= 1    
      try:
        df_mall_model_updated = df_mall_model_updated.drop('source_data_x', axis=1)
      except:
        None
      try:
        df_mall_model_updated = df_mall_model_updated.drop('source_data_y', axis=1)
      except:
        None   
      try:
        df_mall_model_updated['impacts']=df_mall_model_updated['impacts'].astype(int)
      except:
        None 
      print(df_mall_model_updated)
      df_mall_model_updated = df_mall_model_updated.drop('default_hourly_impacts', axis=1)      



      #calculo del impression multiplier
      #*********************************
      df_mall_model_updated['impression_multiplier']=round((df_mall_model_updated['impacts']*(dwell_time/AD_SLOT_DURATION))/(3600/AD_SLOT_DURATION),2)
      #*********************************

      #update average multiplier y error de estimacion

      multiplicador_medio= df_mall_model_updated['impression_multiplier'].mean()
      sql= "update malls set multiplicador_medio=%s  where id=%s"
      val= (float(round(multiplicador_medio,2)), row[0])
      mycursor.execute(sql,val)
      mydb.commit()
      print("Average multiplier :", multiplicador_medio)

      #pd.set_option('display.max_rows', None)
      print(df_mall_model_updated)
      

      #update database with new model
      sql= "DELETE FROM mall_models  WHERE mall_id=%s" % (row[0])
      mycursor.execute(sql)
      mydb.commit()

      df_mall_model_updated.to_sql('mall_models', engine, if_exists='append', index=False)
      flag_updated_mall_model= True


      #***********************************
      # Update impression data tables
      #***********************************
      
      query = "DELETE from audience_impressions where mall_id='%s'" % (str(row[0]))
      engine.execute(query)
      mydb.commit()

      query = "DELETE from audience_multipliers where mall_id='%s'" % (str(row[0]))
      engine.execute(query)
      mydb.commit()

      query = "DELETE from audience_segments where mall_id='%s' " % (str(row[0]))
      engine.execute(query)    
      mydb.commit()

      print("Generating audience Data for mall ", mall_name , " based on mall model. " )

      begin_date = START_DATE
      end_date= END_DATE

      print("Creating dates from ", begin_date , " to ", end_date)
      df_audience_impressions=pd.DataFrame({'mall_id' : str(row[0]), 'date':pd.date_range(start=begin_date, end=end_date)})
      df_audience_segments=pd.DataFrame({'mall_id' : str(row[0]), 'date':pd.date_range(start=begin_date, end=end_date)})
      

      sql= "SELECT hour, month, week_of_year, day_of_week, impacts FROM mall_models WHERE mall_id=%s" % (row[0])
      df_model = pd.read_sql_query(sql, engine)
      impressions_model=df_model.to_dict('records')

      print(df_model)
      #print(impressions_model)
      
      df_audience_impressions['month']=df_audience_impressions['date'].dt.month
      df_audience_impressions['week_of_year']=df_audience_impressions['date'].dt.weekofyear
      df_audience_impressions['day_of_week']=df_audience_impressions['date'].dt.day_of_week

      df_audience_impressions['impressions_00']=0
      df_audience_impressions['impressions_01']=0
      df_audience_impressions['impressions_02']=0
      df_audience_impressions['impressions_03']=0
      df_audience_impressions['impressions_04']=0
      df_audience_impressions['impressions_05']=0
      df_audience_impressions['impressions_06']=0
      df_audience_impressions['impressions_07']=0
      df_audience_impressions['impressions_08']=0
      df_audience_impressions['impressions_09']=0
      impressions_hour= 10
      df_audience_impressions['impressions_10']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 11
      df_audience_impressions['impressions_11']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 12
      df_audience_impressions['impressions_12']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 13
      df_audience_impressions['impressions_13']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 14
      df_audience_impressions['impressions_14']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 15
      df_audience_impressions['impressions_15']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 16
      df_audience_impressions['impressions_16']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 17
      df_audience_impressions['impressions_17']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 18
      df_audience_impressions['impressions_18']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 19
      df_audience_impressions['impressions_19']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 20
      df_audience_impressions['impressions_20']= df_audience_impressions.apply(lambda_impressions,axis=1)
      impressions_hour= 21
      df_audience_impressions['impressions_21']= df_audience_impressions.apply(lambda_impressions,axis=1)
      df_audience_impressions['impressions_22']=0
      df_audience_impressions['impressions_23']=0
      df_audience_impressions['total_impressions']=df_audience_impressions['impressions_10']+df_audience_impressions['impressions_11']+ \
                                                    df_audience_impressions['impressions_12'] + df_audience_impressions['impressions_13'] + \
                                                    df_audience_impressions['impressions_14'] +df_audience_impressions['impressions_15'] + \
                                                    df_audience_impressions['impressions_16'] + df_audience_impressions['impressions_17'] + \
                                                      df_audience_impressions['impressions_18'] +df_audience_impressions['impressions_19'] + \
                                                        df_audience_impressions['impressions_20'] + df_audience_impressions['impressions_21']  

      df_audience_impressions = df_audience_impressions.drop('month', axis=1)
      df_audience_impressions = df_audience_impressions.drop('week_of_year', axis=1)
      df_audience_impressions = df_audience_impressions.drop('day_of_week', axis=1)
      
      print(df_audience_impressions)

      df_audience_impressions.to_sql('audience_impressions', engine, if_exists='append', index=False)


      #************ AUDIENCE MULTIPLIERS TABLE *******************************
      print("Creatiing audience multiplier table")
      df_audience_multipliers=pd.DataFrame({'mall_id' : str(row[0]), 'date':pd.date_range(start=begin_date, end=end_date)})

      sql= "SELECT hour, month, week_of_year, day_of_week, impression_multiplier FROM mall_models WHERE mall_id=%s" % (row[0])
      df_model = pd.read_sql_query(sql, engine)
      impressions_model=df_model.to_dict('records')

      print(df_model)
      #print(impressions_model)
      
      df_audience_multipliers['month']=df_audience_multipliers['date'].dt.month
      df_audience_multipliers['week_of_year']=df_audience_multipliers['date'].dt.weekofyear
      df_audience_multipliers['day_of_week']=df_audience_multipliers['date'].dt.day_of_week

      df_audience_multipliers['1']=0
      df_audience_multipliers['2']=0
      df_audience_multipliers['3']=0
      df_audience_multipliers['4']=0
      df_audience_multipliers['5']=0
      df_audience_multipliers['6']=0
      df_audience_multipliers['7']=0
      df_audience_multipliers['0']=0
      df_audience_multipliers['8']=0
      df_audience_multipliers['9']=0
      impressions_hour= 10
      df_audience_multipliers['10']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 11
      df_audience_multipliers['11']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 12
      df_audience_multipliers['12']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 13
      df_audience_multipliers['13']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 14
      df_audience_multipliers['14']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 15
      df_audience_multipliers['15']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 16
      df_audience_multipliers['16']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 17
      df_audience_multipliers['17']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 18
      df_audience_multipliers['18']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 19
      df_audience_multipliers['19']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 20
      df_audience_multipliers['20']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      impressions_hour= 21
      df_audience_multipliers['21']= df_audience_multipliers.apply(lambda_multipliers,axis=1)
      df_audience_multipliers['22']=0
      df_audience_multipliers['23']=0

      df_audience_multipliers['avg_multiplier']=( df_audience_multipliers['10']+df_audience_multipliers['11']+ \
                                                    df_audience_multipliers['12'] + df_audience_multipliers['13'] + \
                                                    df_audience_multipliers['14'] +df_audience_multipliers['15'] + \
                                                    df_audience_multipliers['16'] + df_audience_multipliers['17'] + \
                                                      df_audience_multipliers['18'] +df_audience_multipliers['19'] + \
                                                        df_audience_multipliers['20'] + df_audience_multipliers['21']  ) / 12

      

      df_audience_multipliers = df_audience_multipliers.drop('month', axis=1)
      df_audience_multipliers = df_audience_multipliers.drop('week_of_year', axis=1)
      df_audience_multipliers = df_audience_multipliers.drop('day_of_week', axis=1)
      
      print(df_audience_multipliers)

      df_audience_multipliers.to_sql('audience_multipliers', engine, if_exists='append', index=False)
    
      #update data

      #SEGMENTS DATA

      df_audience_segments_male=df_audience_segments.copy()
      df_audience_segments_female=df_audience_segments.copy()
      df_audience_segments_kid=df_audience_segments.copy()
      df_audience_segments_young=df_audience_segments.copy()
      df_audience_segments_adult=df_audience_segments.copy()
      df_audience_segments_senior=df_audience_segments.copy()

      #male
      df_audience_segments_male['target_id']=35

      df_audience_segments_male['concentration_00']=default_dem_male
      df_audience_segments_male['concentration_01']=default_dem_male
      df_audience_segments_male['concentration_02']=default_dem_male
      df_audience_segments_male['concentration_03']=default_dem_male
      df_audience_segments_male['concentration_04']=default_dem_male
      df_audience_segments_male['concentration_05']=default_dem_male
      df_audience_segments_male['concentration_06']=default_dem_male
      df_audience_segments_male['concentration_07']=default_dem_male
      df_audience_segments_male['concentration_08']=default_dem_male
      df_audience_segments_male['concentration_09']=default_dem_male
      df_audience_segments_male['concentration_10']=default_dem_male
      df_audience_segments_male['concentration_11']=default_dem_male
      df_audience_segments_male['concentration_12']=default_dem_male
      df_audience_segments_male['concentration_13']=default_dem_male
      df_audience_segments_male['concentration_14']=default_dem_male
      df_audience_segments_male['concentration_15']=default_dem_male
      df_audience_segments_male['concentration_16']=default_dem_male
      df_audience_segments_male['concentration_17']=default_dem_male
      df_audience_segments_male['concentration_18']=default_dem_male
      df_audience_segments_male['concentration_19']=default_dem_male
      df_audience_segments_male['concentration_20']=default_dem_male
      df_audience_segments_male['concentration_21']=default_dem_male
      df_audience_segments_male['concentration_22']=default_dem_male
      df_audience_segments_male['concentration_23']=default_dem_male

      df_audience_segments_male['average_concentration']=default_dem_male

      df_audience_segments_female['target_id']=36

      df_audience_segments_female['concentration_00']=default_dem_female
      df_audience_segments_female['concentration_01']=default_dem_female
      df_audience_segments_female['concentration_02']=default_dem_female
      df_audience_segments_female['concentration_03']=default_dem_female
      df_audience_segments_female['concentration_04']=default_dem_female
      df_audience_segments_female['concentration_05']=default_dem_female
      df_audience_segments_female['concentration_06']=default_dem_female
      df_audience_segments_female['concentration_07']=default_dem_female
      df_audience_segments_female['concentration_08']=default_dem_female
      df_audience_segments_female['concentration_09']=default_dem_female
      df_audience_segments_female['concentration_10']=default_dem_female
      df_audience_segments_female['concentration_11']=default_dem_female
      df_audience_segments_female['concentration_12']=default_dem_female
      df_audience_segments_female['concentration_13']=default_dem_female
      df_audience_segments_female['concentration_14']=default_dem_female
      df_audience_segments_female['concentration_15']=default_dem_female
      df_audience_segments_female['concentration_16']=default_dem_female
      df_audience_segments_female['concentration_17']=default_dem_female
      df_audience_segments_female['concentration_18']=default_dem_female
      df_audience_segments_female['concentration_19']=default_dem_female
      df_audience_segments_female['concentration_20']=default_dem_female
      df_audience_segments_female['concentration_21']=default_dem_female
      df_audience_segments_female['concentration_22']=default_dem_female
      df_audience_segments_female['concentration_23']=default_dem_female

      df_audience_segments_female['average_concentration']=default_dem_male


      df_audience_segments_kid['target_id']=24

      df_audience_segments_kid['concentration_00']=default_age_kid
      df_audience_segments_kid['concentration_01']=default_age_kid
      df_audience_segments_kid['concentration_02']=default_age_kid
      df_audience_segments_kid['concentration_03']=default_age_kid
      df_audience_segments_kid['concentration_04']=default_age_kid
      df_audience_segments_kid['concentration_05']=default_age_kid
      df_audience_segments_kid['concentration_06']=default_age_kid
      df_audience_segments_kid['concentration_07']=default_age_kid
      df_audience_segments_kid['concentration_08']=default_age_kid
      df_audience_segments_kid['concentration_09']=default_age_kid
      df_audience_segments_kid['concentration_10']=default_age_kid
      df_audience_segments_kid['concentration_11']=default_age_kid
      df_audience_segments_kid['concentration_12']=default_age_kid
      df_audience_segments_kid['concentration_13']=default_age_kid
      df_audience_segments_kid['concentration_14']=default_age_kid
      df_audience_segments_kid['concentration_15']=default_age_kid
      df_audience_segments_kid['concentration_16']=default_age_kid
      df_audience_segments_kid['concentration_17']=default_age_kid
      df_audience_segments_kid['concentration_18']=default_age_kid
      df_audience_segments_kid['concentration_19']=default_age_kid
      df_audience_segments_kid['concentration_20']=default_age_kid
      df_audience_segments_kid['concentration_21']=default_age_kid
      df_audience_segments_kid['concentration_22']=default_age_kid
      df_audience_segments_kid['concentration_23']=default_age_kid

      df_audience_segments_kid['average_concentration']=default_dem_male


      df_audience_segments_young['target_id']=25

      df_audience_segments_young['concentration_00']=default_age_young
      df_audience_segments_young['concentration_01']=default_age_young
      df_audience_segments_young['concentration_02']=default_age_young
      df_audience_segments_young['concentration_03']=default_age_young
      df_audience_segments_young['concentration_04']=default_age_young
      df_audience_segments_young['concentration_05']=default_age_young
      df_audience_segments_young['concentration_06']=default_age_young
      df_audience_segments_young['concentration_07']=default_age_young
      df_audience_segments_young['concentration_08']=default_age_young
      df_audience_segments_young['concentration_09']=default_age_young
      df_audience_segments_young['concentration_10']=default_age_young
      df_audience_segments_young['concentration_11']=default_age_young
      df_audience_segments_young['concentration_12']=default_age_young
      df_audience_segments_young['concentration_13']=default_age_young
      df_audience_segments_young['concentration_14']=default_age_young
      df_audience_segments_young['concentration_15']=default_age_young
      df_audience_segments_young['concentration_16']=default_age_young
      df_audience_segments_young['concentration_17']=default_age_young
      df_audience_segments_young['concentration_18']=default_age_young
      df_audience_segments_young['concentration_19']=default_age_young
      df_audience_segments_young['concentration_20']=default_age_young
      df_audience_segments_young['concentration_21']=default_age_young
      df_audience_segments_young['concentration_22']=default_age_young
      df_audience_segments_young['concentration_23']=default_age_young

      df_audience_segments_young['average_concentration']=default_age_young


      df_audience_segments_adult['target_id']=26

      df_audience_segments_adult['concentration_00']=default_age_adult
      df_audience_segments_adult['concentration_01']=default_age_adult
      df_audience_segments_adult['concentration_02']=default_age_adult
      df_audience_segments_adult['concentration_03']=default_age_adult
      df_audience_segments_adult['concentration_04']=default_age_adult
      df_audience_segments_adult['concentration_05']=default_age_adult
      df_audience_segments_adult['concentration_06']=default_age_adult
      df_audience_segments_adult['concentration_07']=default_age_adult
      df_audience_segments_adult['concentration_08']=default_age_adult
      df_audience_segments_adult['concentration_09']=default_age_adult
      df_audience_segments_adult['concentration_10']=default_age_adult
      df_audience_segments_adult['concentration_11']=default_age_adult
      df_audience_segments_adult['concentration_12']=default_age_adult
      df_audience_segments_adult['concentration_13']=default_age_adult
      df_audience_segments_adult['concentration_14']=default_age_adult
      df_audience_segments_adult['concentration_15']=default_age_adult
      df_audience_segments_adult['concentration_16']=default_age_adult
      df_audience_segments_adult['concentration_17']=default_age_adult
      df_audience_segments_adult['concentration_18']=default_age_adult
      df_audience_segments_adult['concentration_19']=default_age_adult
      df_audience_segments_adult['concentration_20']=default_age_adult
      df_audience_segments_adult['concentration_21']=default_age_adult
      df_audience_segments_adult['concentration_22']=default_age_adult
      df_audience_segments_adult['concentration_23']=default_age_adult
      df_audience_segments_adult['average_concentration']=default_age_adult

      df_audience_segments_senior['target_id']=27

      df_audience_segments_senior['concentration_00']=default_age_senior
      df_audience_segments_senior['concentration_01']=default_age_senior
      df_audience_segments_senior['concentration_02']=default_age_senior
      df_audience_segments_senior['concentration_03']=default_age_senior
      df_audience_segments_senior['concentration_04']=default_age_senior
      df_audience_segments_senior['concentration_05']=default_age_senior
      df_audience_segments_senior['concentration_06']=default_age_senior
      df_audience_segments_senior['concentration_07']=default_age_senior
      df_audience_segments_senior['concentration_08']=default_age_senior
      df_audience_segments_senior['concentration_09']=default_age_senior
      df_audience_segments_senior['concentration_10']=default_age_senior
      df_audience_segments_senior['concentration_11']=default_age_senior
      df_audience_segments_senior['concentration_12']=default_age_senior
      df_audience_segments_senior['concentration_13']=default_age_senior
      df_audience_segments_senior['concentration_14']=default_age_senior
      df_audience_segments_senior['concentration_15']=default_age_senior
      df_audience_segments_senior['concentration_16']=default_age_senior
      df_audience_segments_senior['concentration_17']=default_age_senior
      df_audience_segments_senior['concentration_18']=default_age_senior
      df_audience_segments_senior['concentration_19']=default_age_senior
      df_audience_segments_senior['concentration_20']=default_age_senior
      df_audience_segments_senior['concentration_21']=default_age_senior
      df_audience_segments_senior['concentration_22']=default_age_senior
      df_audience_segments_senior['concentration_23']=default_age_senior
      df_audience_segments_senior['average_concentration']=default_age_senior

        
      df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_female,ignore_index = True)
      df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_kid,ignore_index = True)
      df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_young,ignore_index = True)
      df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_adult,ignore_index = True)
      df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_senior,ignore_index = True)
      df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_male,ignore_index = True) 

      #save to DB / update
      df_audience_segments=df_audience_segments_male.copy()
      print(df_audience_segments)

      df_audience_segments['datetime']=df_audience_segments['date']
      df_audience_segments['datetime']=df_audience_segments['datetime'].dt.strftime('%Y-%m-%d')

      columns = ['date']
      df_audience_segments.drop(columns, inplace=True, axis=1)

      df_audience_segments.to_sql('audience_segments', engine, if_exists='append', index=False)


    #report update
    update_report.append({"mall_id": 1, 
                          "mall_name":mall_name,
                          "screens": total_screens,
                          "screen_density": screen_density,
                          "screen_visibility_index": screen_visibility_index,
                          "inspide_data_available": available_inspide_data,
                          "mall_footfall_available": available_mall_data,
                          "cam_data_available": available_camera_data,
                          "updated_mall_model": flag_updated_mall_model,
                          "inspide_daily_impacts": inspide_daily_impacts,
                          "mall_data_daily_impacts": mall_data_estimated_daily_impacts,
                          "flag_no_audience": flag_no_audience,
                          "screen_count_error": flag_screen_number_error,
                          "dates_generated": 0})

print("Creating Audience Report table")
df_audience_update_report = pd.DataFrame(update_report)
df_audience_update_report.to_sql('audience_update_report', engine, if_exists='replace', index=False)


        
     

        



