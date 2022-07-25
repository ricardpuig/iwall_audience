import requests
import json
import re
import mysql.connector
import pandas as pd
import numpy as np
import logging as log
from datetime import datetime, timedelta
from datetime import date
import sys

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import datetime
from dateutil.relativedelta import relativedelta

def delete_all_campaign_data():

  print("deleting previous campaign results from database...")

  query="DELETE FROM campaign_analysis "
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_display_unit_performance"
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_daily_performance "
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_daily_audience "
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_display_unit_audience "
  mycursor.execute(query)
  mydb.commit()

def delete_campaign_data(campaign_id):

  print("deleting previous campaign results from database...")
  query="DELETE FROM campaign_analysis WHERE reservation_id={id}".format(id=campaign_id)
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_display_unit_performance WHERE reservation_id={id}".format(id=campaign_id)
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_daily_performance WHERE reservation_id={id}".format(id=campaign_id)
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_daily_audience WHERE reservation_id={id}".format(id=campaign_id)
  mycursor.execute(query)
  mydb.commit()

  query="DELETE FROM campaign_display_unit_audience WHERE reservation_id={id}".format(id=campaign_id)
  mycursor.execute(query)
  mydb.commit()


def pre_check(country, auth, mycursor):

  #listado de malls
  if country=="SPAIN":
    container_ids="106135296"

  if country=="COLOMBIA":
    container_ids='120956285'

  if country=="PERU":
    container_ids='60141576'

  url_container_scoped= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398';

  #compose with parent container
  url_container_scoped = url_container_scoped  +"&parent_container_ids=" +container_ids



  s=requests.get(url_container_scoped,headers={'Accept': 'application/json','Authorization': auth})
  data=json.loads(s.text)

  excluded_folders = [60141576,357277484, 343438965,106135296, 49461537, 21003047, 120956285 ]

  for n in data["container"]:

    if not n['id'] in excluded_folders:

      if n['active']== True: #only active containers
        container_id = n['id']
        container_name= n['name']
        print(" Checking " + str(container_name))

        #check audience data for each container 

        records={}

        #to loop for each display unit
        sql_select_malls= "SELECT id, name, screens from malls where broadsign_container_id='%s'" % (str(container_id))

        mycursor.execute(sql_select_malls)
        records= mycursor.fetchall()
        
        if len(records)==0:
          print("**** NOTE: no DB record for " + container_name)

        for row_0 in records:  #for each result

            #check number of screens for that folder an compare records
            screens_audience = row_0[2]

            url_display_unit_list=  'https://api.broadsign.com:10889/rest/display_unit/v12/by_container?domain_id=17244398' + "&container_id=" + str(container_id); 

            s=requests.get(url_display_unit_list,headers={'Accept': 'application/json','Authorization': auth})
            data2=json.loads(s.text)

            screen_count = 0

            for m in data2['display_unit']:
              screen_count = screen_count + m['host_screen_count']

            screens_broadsign= screen_count

            if row_0[2] != screen_count: 
              print("***** ERROR : screens number do not match for " + container_name + " - Screens audience " + str(row_0[2]) + " Screens broadsign " + str(screens_broadsign) )


            #check that audience schedules exist for each container 

            mall_id = row_0[0]
            #to loop for each display unit
            sql_select_audience_data= "SELECT COUNT(*) from audience_impressions where mall_id='%s'" % (str(mall_id))

            mycursor.execute(sql_select_audience_data)
            records1= mycursor.fetchall()
            for row_1 in records1:  #for each result

              #check number of screens for that folder an compare records
              audience_days = row_1[0]
              if audience_days == 0:
                print("***** ERROR : No Audience data for " + str(mall_id)  + "("+ row_0[1] + ")" +  ":"  + str(audience_days))
                generate_default_audience_data(mycursor, mall_id)

              else:

                #check audience data 
                check_audience_data(mycursor,mall_id)


def check_audience_data(mycursor, mall_id):

  #check number of screens match


  sql_select_malls= "SELECT id, name, screens, screens_type1, screens_type2, screens_type3, default_screen_day_impressions, screen_type_deviation from malls where id='%s'" % (str(mall_id))
  mycursor.execute(sql_select_malls)
  records= mycursor.fetchall()
      
  if len(records)==0:
    print("**** NOTE: no DB record for mall id " + str(mall_id))

  for row_0 in records:  #for each result
    total_screens=row_0[2]
    total_screens_by_type= row_0[3]+row_0[4]+row_0[5]
    screen_type_deviation= row_0[7]

    if total_screens != total_screens_by_type:
      print("**** ERROR : total screens by type do not match in ", str(row_0[1]) + " total:" + str(total_screens) + ", by type:", str(total_screens_by_type))

    if not screen_type_deviation:
      print("**** ERROR : no screen type deviation ")

  #check default audience data

  sql_select_malls= "SELECT id, name, default_dem_male, default_dem_female, default_age_kid, default_age_young, default_age_adult, default_age_senior  from malls where id='%s'" % (str(mall_id))
  mycursor.execute(sql_select_malls)
  records= mycursor.fetchall()
      
  if len(records)==0:
    print("**** NOTE: no demographics data for mall id " + str(mall_id))

  for row_0 in records:  #for each result
    None


  sql_select_malls= "SELECT id, name, default_nse_A,  default_nse_B,default_nse_C,default_nse_D from malls where id='%s'" % (str(mall_id))
  mycursor.execute(sql_select_malls)
  records= mycursor.fetchall()
      
  if len(records)==0:
    print("**** NOTE: no NSE data for mall id " + str(mall_id))

  for row_0 in records:  #for each result
    None


def generate_default_audience_data(mycursor, mall_id, all_malls):



  host = "ec2-52-18-248-109.eu-west-1.compute.amazonaws.com"
  user = "root"
  passwd = "SonaeRootMysql2021!"
  db = "audience"
  port = 3306

  engine_string = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
          user = user,
          password = passwd,
          host = host,
          port = port,
          db = db,
  )
  # create sqlalchemy engine
  engine = create_engine(engine_string)


  #load mall data and audience model parameters from audience DB 

  if all_malls ==1:
      query = "SELECT * from malls"
  else:    
      query = "SELECT * from malls where id='%s'" % (str(mall_id))

  df_malls = pd.read_sql_query(query, engine)


  query="DELETE FROM audience_impressions"
  engine.execute(query)
  query="DELETE FROM audience_segments"
  engine.execute(query)
  

  malls=df_malls['id'].to_list()
  #print(df_malls)
  print("Generating default audience data for mall " + str(mall_id))

  #get holidays
  query = "SELECT * from holidays"
  df_holidays = pd.read_sql_query(query, engine)
  holidays = df_holidays['date'].to_list()
  adjustments = df_holidays['adjustment'].to_list()


  for m in malls: 

      query = "DELETE from audience_impressions where mall_id='%s'" % (str(mall_id))
      engine.execute(query)

      query = "DELETE from audience_segments where mall_id='%s'" % (str(mall_id))
      engine.execute(query)    
  
      df_mall=df_malls.loc[df_malls['id'] == m]
      malls_info=df_mall.to_dict('records')
      mall=malls_info[0]

      model_behavior=mall['mall_model_behavior']
      print("Creating Audience Data for mall ", mall['name'], "(", m,")",  " behavior: ", model_behavior, " type ", mall['mall_model_type'], " Default impressions: ", mall['default_screen_day_impressions'])

      #get default model 
      query = "SELECT * from mall_default_models  WHERE mall_type = {type}".format(type=mall['mall_model_type'])
      df_model = pd.read_sql_query(query, engine)
      
      #get data updated data from cameras
      model_info=df_model.to_dict('records')
      model=model_info[0]

      #build default audience data for the current year
      #dates create 
      begin_date = '2022-01-01'
      end_date= '2022-12-31'

      print("Creating dates from ", begin_date , " to ", end_date)
      df_audience_impressions=pd.DataFrame({'mall_id' : mall['id'], 'date':pd.date_range(start=begin_date, end=end_date)})
      df_audience_segments=pd.DataFrame({'mall_id' : mall['id'], 'date':pd.date_range(start=begin_date, end=end_date)})
      

      #get the overall multiplier from screen types
      num_screens=mall['screens']
      default_screen_impressions=mall['default_screen_day_impressions']
      type_deviation=mall['screen_type_deviation_type1_high_visibility']
      type_deviation2=mall['screen_type_deviation_type3_low_visibility']


      #type 1 : top screens, 
      #type 2 : average screens
      #type 3: low visibility screens


      overall_daily=(mall['screens_type1_high_visibility']*default_screen_impressions*(1+type_deviation)) + \
          (mall['screens_type2_default_visibility']*default_screen_impressions) + \
          (mall['screens_type3_low_visibility']*default_screen_impressions*type_deviation2)
      
      hourly_str= model['hourly']
      hourly_str= hourly_str.replace(",", ".")
      hourly=hourly_str.split(':')
      hourly=list(np.float_(hourly))

      print("\nHourly model-----", hourly)

      weekday_str=model['weekday']

      weekday_str=weekday_str.replace(",",".")
      weekday=weekday_str.split(':')
      weekday=list(np.float_(weekday))

      print("\nWeekday model-----", weekday)

      weekly_str=model['weekly']
      weekly_str=weekly_str.replace(",",".")
      weekly=weekly_str.split(':')
      weekly=list(np.float_(weekly))

      print("\nWeekly model------- ", weekly)

      monthly_str=model['monthly']
      monthly_str=monthly_str.replace(",",".")
      monthly=monthly_str.split(':')
      monthly=list(np.float_(monthly))

      print("\nMonthly model: ", monthly)

     

      df_audience_impressions['total_impressions']=overall_daily

      print("\nOverall daily impressions for mall: ", overall_daily)

      #do weekday adjustements
      df_audience_impressions['date']=pd.to_datetime(df_audience_impressions['date'], format="%Y-%m-%d")
      df_audience_impressions=df_audience_impressions.set_index('date')
      df_audience_impressions['weekday']=df_audience_impressions.index.weekday
      df_audience_impressions['week']=df_audience_impressions.index.week


      df_audience_impressions['weekday_multiplier']=1.0
      df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 0, weekday[0], df_audience_impressions['weekday_multiplier'])
      df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 1, weekday[1], df_audience_impressions['weekday_multiplier'])
      df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 2, weekday[2], df_audience_impressions['weekday_multiplier'])
      df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 3, weekday[3], df_audience_impressions['weekday_multiplier'])
      df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 4, weekday[4], df_audience_impressions['weekday_multiplier'])
      df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 5, weekday[5], df_audience_impressions['weekday_multiplier'])
      df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 6, weekday[6], df_audience_impressions['weekday_multiplier'])

      df_audience_impressions['weekly_multiplier']=1.0

      date = datetime.date(2022, 1, 1) 

      print(df_audience_impressions)
      
      
      i = 1
      while i < 53:
        res = date + relativedelta(weeks = +i)
        index_month=int(res.month)
        print("Week: ", i , " Month: ", index_month)
        if model_behavior=="WEEKLY":
          df_audience_impressions['weekly_multiplier']=np.where(df_audience_impressions['week'] == i, weekly[i-1], df_audience_impressions['weekly_multiplier'])
        elif model_behavior=="MONTHLY":
          df_audience_impressions['weekly_multiplier']=np.where(df_audience_impressions['week'] == i, monthly[index_month-1], df_audience_impressions['weekly_multiplier'])
        else:
          print("Mall model behavior not defined")
          exit(1)
        i += 1
      print(df_audience_impressions)

      df_audience_impressions['total_impressions']=df_audience_impressions['total_impressions']*df_audience_impressions['weekday_multiplier']
      df_audience_impressions['total_impressions']=df_audience_impressions['total_impressions']*df_audience_impressions['weekly_multiplier']


      df_audience_impressions['impressions_00']=df_audience_impressions['total_impressions']*hourly[0]
      df_audience_impressions['impressions_01']=df_audience_impressions['total_impressions']*hourly[1]
      df_audience_impressions['impressions_02']=df_audience_impressions['total_impressions']*hourly[2]
      df_audience_impressions['impressions_03']=df_audience_impressions['total_impressions']*hourly[3]
      df_audience_impressions['impressions_04']=df_audience_impressions['total_impressions']*hourly[4]
      df_audience_impressions['impressions_05']=df_audience_impressions['total_impressions']*hourly[5]
      df_audience_impressions['impressions_06']=df_audience_impressions['total_impressions']*hourly[6]
      df_audience_impressions['impressions_07']=df_audience_impressions['total_impressions']*hourly[7]
      df_audience_impressions['impressions_08']=df_audience_impressions['total_impressions']*hourly[8]
      df_audience_impressions['impressions_09']=df_audience_impressions['total_impressions']*hourly[9]
      df_audience_impressions['impressions_10']=df_audience_impressions['total_impressions']*hourly[10]
      df_audience_impressions['impressions_11']=df_audience_impressions['total_impressions']*hourly[11]
      df_audience_impressions['impressions_12']=df_audience_impressions['total_impressions']*hourly[12]
      df_audience_impressions['impressions_13']=df_audience_impressions['total_impressions']*hourly[13]
      df_audience_impressions['impressions_14']=df_audience_impressions['total_impressions']*hourly[14]
      df_audience_impressions['impressions_15']=df_audience_impressions['total_impressions']*hourly[15]
      df_audience_impressions['impressions_16']=df_audience_impressions['total_impressions']*hourly[16]
      df_audience_impressions['impressions_17']=df_audience_impressions['total_impressions']*hourly[17]
      df_audience_impressions['impressions_18']=df_audience_impressions['total_impressions']*hourly[18]
      df_audience_impressions['impressions_19']=df_audience_impressions['total_impressions']*hourly[19]
      df_audience_impressions['impressions_20']=df_audience_impressions['total_impressions']*hourly[20]
      df_audience_impressions['impressions_21']=df_audience_impressions['total_impressions']*hourly[21]
      df_audience_impressions['impressions_22']=df_audience_impressions['total_impressions']*hourly[22]
      df_audience_impressions['impressions_23']=df_audience_impressions['total_impressions']*hourly[23]
      #print(df_audience_impressions)


      columns = ['weekday', 'week', 'weekday_multiplier', 'weekly_multiplier']
      df_audience_impressions.drop(columns, inplace=True, axis=1)

      #print(df_audience_impressions)

      df_audience_impressions=df_audience_impressions.reset_index()
      df_audience_impressions['date']=df_audience_impressions['date'].dt.strftime('%Y-%m-%d')


      #update data
      df_audience_impressions.to_sql('audience_impressions', engine, if_exists='append', index=False)

      #SEGMENTS DATA

      df_audience_segments_male=df_audience_segments.copy()
      df_audience_segments_female=df_audience_segments.copy()
      df_audience_segments_kid=df_audience_segments.copy()
      df_audience_segments_young=df_audience_segments.copy()
      df_audience_segments_adult=df_audience_segments.copy()
      df_audience_segments_senior=df_audience_segments.copy()

      #male
      df_audience_segments_male['target_id']=35

      df_audience_segments_male['concentration_00']=mall['default_dem_male']
      df_audience_segments_male['concentration_01']=mall['default_dem_male']
      df_audience_segments_male['concentration_02']=mall['default_dem_male']
      df_audience_segments_male['concentration_03']=mall['default_dem_male']
      df_audience_segments_male['concentration_04']=mall['default_dem_male']
      df_audience_segments_male['concentration_05']=mall['default_dem_male']
      df_audience_segments_male['concentration_06']=mall['default_dem_male']
      df_audience_segments_male['concentration_07']=mall['default_dem_male']
      df_audience_segments_male['concentration_08']=mall['default_dem_male']
      df_audience_segments_male['concentration_09']=mall['default_dem_male']
      df_audience_segments_male['concentration_10']=mall['default_dem_male']
      df_audience_segments_male['concentration_11']=mall['default_dem_male']
      df_audience_segments_male['concentration_12']=mall['default_dem_male']
      df_audience_segments_male['concentration_13']=mall['default_dem_male']
      df_audience_segments_male['concentration_14']=mall['default_dem_male']
      df_audience_segments_male['concentration_15']=mall['default_dem_male']
      df_audience_segments_male['concentration_16']=mall['default_dem_male']
      df_audience_segments_male['concentration_17']=mall['default_dem_male']
      df_audience_segments_male['concentration_18']=mall['default_dem_male']
      df_audience_segments_male['concentration_19']=mall['default_dem_male']
      df_audience_segments_male['concentration_20']=mall['default_dem_male']
      df_audience_segments_male['concentration_21']=mall['default_dem_male']
      df_audience_segments_male['concentration_22']=mall['default_dem_male']
      df_audience_segments_male['concentration_23']=mall['default_dem_male']

      df_audience_segments_male['average_concentration']=mall['default_dem_male']


      df_audience_segments_female['target_id']=36

      df_audience_segments_female['concentration_00']=mall['default_dem_female']
      df_audience_segments_female['concentration_01']=mall['default_dem_female']
      df_audience_segments_female['concentration_02']=mall['default_dem_female']
      df_audience_segments_female['concentration_03']=mall['default_dem_female']
      df_audience_segments_female['concentration_04']=mall['default_dem_female']
      df_audience_segments_female['concentration_05']=mall['default_dem_female']
      df_audience_segments_female['concentration_06']=mall['default_dem_female']
      df_audience_segments_female['concentration_07']=mall['default_dem_female']
      df_audience_segments_female['concentration_08']=mall['default_dem_female']
      df_audience_segments_female['concentration_09']=mall['default_dem_female']
      df_audience_segments_female['concentration_10']=mall['default_dem_female']
      df_audience_segments_female['concentration_11']=mall['default_dem_female']
      df_audience_segments_female['concentration_12']=mall['default_dem_female']
      df_audience_segments_female['concentration_13']=mall['default_dem_female']
      df_audience_segments_female['concentration_14']=mall['default_dem_female']
      df_audience_segments_female['concentration_15']=mall['default_dem_female']
      df_audience_segments_female['concentration_16']=mall['default_dem_female']
      df_audience_segments_female['concentration_17']=mall['default_dem_female']
      df_audience_segments_female['concentration_18']=mall['default_dem_female']
      df_audience_segments_female['concentration_19']=mall['default_dem_female']
      df_audience_segments_female['concentration_20']=mall['default_dem_female']
      df_audience_segments_female['concentration_21']=mall['default_dem_female']
      df_audience_segments_female['concentration_22']=mall['default_dem_female']
      df_audience_segments_female['concentration_23']=mall['default_dem_female']

      df_audience_segments_female['average_concentration']=mall['default_dem_male']


      df_audience_segments_kid['target_id']=24

      df_audience_segments_kid['concentration_00']=mall['default_age_kid']
      df_audience_segments_kid['concentration_01']=mall['default_age_kid']
      df_audience_segments_kid['concentration_02']=mall['default_age_kid']
      df_audience_segments_kid['concentration_03']=mall['default_age_kid']
      df_audience_segments_kid['concentration_04']=mall['default_age_kid']
      df_audience_segments_kid['concentration_05']=mall['default_age_kid']
      df_audience_segments_kid['concentration_06']=mall['default_age_kid']
      df_audience_segments_kid['concentration_07']=mall['default_age_kid']
      df_audience_segments_kid['concentration_08']=mall['default_age_kid']
      df_audience_segments_kid['concentration_09']=mall['default_age_kid']
      df_audience_segments_kid['concentration_10']=mall['default_age_kid']
      df_audience_segments_kid['concentration_11']=mall['default_age_kid']
      df_audience_segments_kid['concentration_12']=mall['default_age_kid']
      df_audience_segments_kid['concentration_13']=mall['default_age_kid']
      df_audience_segments_kid['concentration_14']=mall['default_age_kid']
      df_audience_segments_kid['concentration_15']=mall['default_age_kid']
      df_audience_segments_kid['concentration_16']=mall['default_age_kid']
      df_audience_segments_kid['concentration_17']=mall['default_age_kid']
      df_audience_segments_kid['concentration_18']=mall['default_age_kid']
      df_audience_segments_kid['concentration_19']=mall['default_age_kid']
      df_audience_segments_kid['concentration_20']=mall['default_age_kid']
      df_audience_segments_kid['concentration_21']=mall['default_age_kid']
      df_audience_segments_kid['concentration_22']=mall['default_age_kid']
      df_audience_segments_kid['concentration_23']=mall['default_age_kid']

      df_audience_segments_kid['average_concentration']=mall['default_dem_male']


      df_audience_segments_young['target_id']=25

      df_audience_segments_young['concentration_00']=mall['default_age_young']
      df_audience_segments_young['concentration_01']=mall['default_age_young']
      df_audience_segments_young['concentration_02']=mall['default_age_young']
      df_audience_segments_young['concentration_03']=mall['default_age_young']
      df_audience_segments_young['concentration_04']=mall['default_age_young']
      df_audience_segments_young['concentration_05']=mall['default_age_young']
      df_audience_segments_young['concentration_06']=mall['default_age_young']
      df_audience_segments_young['concentration_07']=mall['default_age_young']
      df_audience_segments_young['concentration_08']=mall['default_age_young']
      df_audience_segments_young['concentration_09']=mall['default_age_young']
      df_audience_segments_young['concentration_10']=mall['default_age_young']
      df_audience_segments_young['concentration_11']=mall['default_age_young']
      df_audience_segments_young['concentration_12']=mall['default_age_young']
      df_audience_segments_young['concentration_13']=mall['default_age_young']
      df_audience_segments_young['concentration_14']=mall['default_age_young']
      df_audience_segments_young['concentration_15']=mall['default_age_young']
      df_audience_segments_young['concentration_16']=mall['default_age_young']
      df_audience_segments_young['concentration_17']=mall['default_age_young']
      df_audience_segments_young['concentration_18']=mall['default_age_young']
      df_audience_segments_young['concentration_19']=mall['default_age_young']
      df_audience_segments_young['concentration_20']=mall['default_age_young']
      df_audience_segments_young['concentration_21']=mall['default_age_young']
      df_audience_segments_young['concentration_22']=mall['default_age_young']
      df_audience_segments_young['concentration_23']=mall['default_age_young']

      df_audience_segments_young['average_concentration']=mall['default_age_young']


      df_audience_segments_adult['target_id']=26

      df_audience_segments_adult['concentration_00']=mall['default_age_adult']
      df_audience_segments_adult['concentration_01']=mall['default_age_adult']
      df_audience_segments_adult['concentration_02']=mall['default_age_adult']
      df_audience_segments_adult['concentration_03']=mall['default_age_adult']
      df_audience_segments_adult['concentration_04']=mall['default_age_adult']
      df_audience_segments_adult['concentration_05']=mall['default_age_adult']
      df_audience_segments_adult['concentration_06']=mall['default_age_adult']
      df_audience_segments_adult['concentration_07']=mall['default_age_adult']
      df_audience_segments_adult['concentration_08']=mall['default_age_adult']
      df_audience_segments_adult['concentration_09']=mall['default_age_adult']
      df_audience_segments_adult['concentration_10']=mall['default_age_adult']
      df_audience_segments_adult['concentration_11']=mall['default_age_adult']
      df_audience_segments_adult['concentration_12']=mall['default_age_adult']
      df_audience_segments_adult['concentration_13']=mall['default_age_adult']
      df_audience_segments_adult['concentration_14']=mall['default_age_adult']
      df_audience_segments_adult['concentration_15']=mall['default_age_adult']
      df_audience_segments_adult['concentration_16']=mall['default_age_adult']
      df_audience_segments_adult['concentration_17']=mall['default_age_adult']
      df_audience_segments_adult['concentration_18']=mall['default_age_adult']
      df_audience_segments_adult['concentration_19']=mall['default_age_adult']
      df_audience_segments_adult['concentration_20']=mall['default_age_adult']
      df_audience_segments_adult['concentration_21']=mall['default_age_adult']
      df_audience_segments_adult['concentration_22']=mall['default_age_adult']
      df_audience_segments_adult['concentration_23']=mall['default_age_adult']
      df_audience_segments_adult['average_concentration']=mall['default_age_adult']

      df_audience_segments_senior['target_id']=27

      df_audience_segments_senior['concentration_00']=mall['default_age_senior']
      df_audience_segments_senior['concentration_01']=mall['default_age_senior']
      df_audience_segments_senior['concentration_02']=mall['default_age_senior']
      df_audience_segments_senior['concentration_03']=mall['default_age_senior']
      df_audience_segments_senior['concentration_04']=mall['default_age_senior']
      df_audience_segments_senior['concentration_05']=mall['default_age_senior']
      df_audience_segments_senior['concentration_06']=mall['default_age_senior']
      df_audience_segments_senior['concentration_07']=mall['default_age_senior']
      df_audience_segments_senior['concentration_08']=mall['default_age_senior']
      df_audience_segments_senior['concentration_09']=mall['default_age_senior']
      df_audience_segments_senior['concentration_10']=mall['default_age_senior']
      df_audience_segments_senior['concentration_11']=mall['default_age_senior']
      df_audience_segments_senior['concentration_12']=mall['default_age_senior']
      df_audience_segments_senior['concentration_13']=mall['default_age_senior']
      df_audience_segments_senior['concentration_14']=mall['default_age_senior']
      df_audience_segments_senior['concentration_15']=mall['default_age_senior']
      df_audience_segments_senior['concentration_16']=mall['default_age_senior']
      df_audience_segments_senior['concentration_17']=mall['default_age_senior']
      df_audience_segments_senior['concentration_18']=mall['default_age_senior']
      df_audience_segments_senior['concentration_19']=mall['default_age_senior']
      df_audience_segments_senior['concentration_20']=mall['default_age_senior']
      df_audience_segments_senior['concentration_21']=mall['default_age_senior']
      df_audience_segments_senior['concentration_22']=mall['default_age_senior']
      df_audience_segments_senior['concentration_23']=mall['default_age_senior']
      df_audience_segments_senior['average_concentration']=mall['default_age_senior']

      
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


  print("adjusting holidays...")
  for m in malls: 
  

    for d_index, d in enumerate(holidays):


      print("adjusting day", d)



      query = "SELECT * from audience_impressions WHERE date = '{fecha}' AND mall_id = {mid}".format(fecha=d, mid = m)
      print(query)
      df_audience_impressions= pd.read_sql_query(query, engine)

      print(df_audience_impressions)

      df_audience_impressions['total_impressions']=df_audience_impressions['total_impressions']*adjustments[d_index]

      df_audience_impressions['impressions_00']=df_audience_impressions['impressions_00']*adjustments[d_index]
      df_audience_impressions['impressions_01']=df_audience_impressions['impressions_01']*adjustments[d_index]
      df_audience_impressions['impressions_02']=df_audience_impressions['impressions_02']*adjustments[d_index]
      df_audience_impressions['impressions_03']=df_audience_impressions['impressions_03']*adjustments[d_index]
      df_audience_impressions['impressions_04']=df_audience_impressions['impressions_04']*adjustments[d_index]
      df_audience_impressions['impressions_05']=df_audience_impressions['impressions_05']*adjustments[d_index]
      df_audience_impressions['impressions_06']=df_audience_impressions['impressions_06']*adjustments[d_index]
      df_audience_impressions['impressions_07']=df_audience_impressions['impressions_07']*adjustments[d_index]
      df_audience_impressions['impressions_08']=df_audience_impressions['impressions_08']*adjustments[d_index]
      df_audience_impressions['impressions_09']=df_audience_impressions['impressions_09']*adjustments[d_index]
      df_audience_impressions['impressions_10']=df_audience_impressions['impressions_10']*adjustments[d_index]
      df_audience_impressions['impressions_11']=df_audience_impressions['impressions_11']*adjustments[d_index]
      df_audience_impressions['impressions_12']=df_audience_impressions['impressions_12']*adjustments[d_index]
      df_audience_impressions['impressions_13']=df_audience_impressions['impressions_13']*adjustments[d_index]
      df_audience_impressions['impressions_14']=df_audience_impressions['impressions_14']*adjustments[d_index]
      df_audience_impressions['impressions_15']=df_audience_impressions['impressions_15']*adjustments[d_index]
      df_audience_impressions['impressions_16']=df_audience_impressions['impressions_16']*adjustments[d_index]
      df_audience_impressions['impressions_17']=df_audience_impressions['impressions_17']*adjustments[d_index]
      df_audience_impressions['impressions_18']=df_audience_impressions['impressions_18']*adjustments[d_index]
      df_audience_impressions['impressions_19']=df_audience_impressions['impressions_19']*adjustments[d_index]
      df_audience_impressions['impressions_20']=df_audience_impressions['impressions_20']*adjustments[d_index]
      df_audience_impressions['impressions_21']=df_audience_impressions['impressions_21']*adjustments[d_index]
      df_audience_impressions['impressions_22']=df_audience_impressions['impressions_22']*adjustments[d_index]
      df_audience_impressions['impressions_23']=df_audience_impressions['impressions_23']*adjustments[d_index]

      print("Adjusted: ")
      print(df_audience_impressions)


      query = "DELETE from audience_impressions WHERE date = '{fecha}' AND mall_id = {mid}".format(fecha=d, mid = m)
      print(query)
      mycursor.execute(query)
      mydb.commit()

      #update data

      df_audience_impressions.drop(columns=['id'], inplace=True, axis=1)
      df_audience_impressions.to_sql('audience_impressions', engine, if_exists='append', index=False)


  None


def get_not_production_campaign_containers():

  #get all container ids in mall campaigns 

  container_ids ='49411994,236129747,49412006,92701400,135550190'  #caontainer for mall campaigns
  notproduction_container_ids = []

  url_container_scoped= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398';

  #compose with parent containers
  url_container_scoped = url_container_scoped  +"&parent_container_ids=" +container_ids

  s=requests.get(url_container_scoped,headers={'Accept': 'application/json','Authorization': auth})
  data=json.loads(s.text)

  for n in data["container"]:
      if n['active']== True: #only active containers
        notproduction_container_ids.append(n['id'])
        #print(n['name']) 

  return notproduction_container_ids


def is_production_campaign(id):
  None

def list_malls(mycursor, country):

  #listado de malls
  if country=="SPAIN":
    container_ids="106135296"

  if country=="COLOMBIA":
    container_ids='120956285'

  if country=="PERU":
    container_ids='60141576'

  url_container_scoped= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398';

  #compose with parent container
  url_container_scoped = url_container_scoped  +"&parent_container_ids=" +container_ids



  s=requests.get(url_container_scoped,headers={'Accept': 'application/json','Authorization': auth})
  data=json.loads(s.text)

  excluded_folders = [60141576,357277484, 343438965,106135296, 49461537, 21003047, 120956285 ]

  for n in data["container"]:

    if not n['id'] in excluded_folders:

      if n['active']== True: #only active containers
        container_id = n['id']
        container_name= n['name']
        

        records={}

        #to loop for each display unit
        sql_select_malls= "SELECT id, name, screens from malls where broadsign_container_id='%s'" % (str(container_id))

        mycursor.execute(sql_select_malls)
        records= mycursor.fetchall()
        
        if len(records)==0:
          print("**** NOTE: no DB record for " + container_name)

        for row_0 in records:  #for each result

            #check number of screens for that folder an compare records
            screens_audience = row_0[2]

            url_display_unit_list=  'https://api.broadsign.com:10889/rest/display_unit/v12/by_container?domain_id=17244398' + "&container_id=" + str(container_id); 

            s=requests.get(url_display_unit_list,headers={'Accept': 'application/json','Authorization': auth})
            data2=json.loads(s.text)

            screen_count = 0

            for m in data2['display_unit']:
              screen_count = screen_count + m['host_screen_count']

            screens_broadsign= screen_count

            if row_0[2] != screen_count: 
              print("***** ERROR : screens number do not match for " + container_name + " - Screens audience " + str(row_0[2]) + " Screens broadsign " + str(screens_broadsign) )


            #check that audience schedules exist for each container 

            mall_id = row_0[0] 
            print("* " , str(container_name) , " MALL ID:", mall_id, " Screens broadsign:", screen_count, " Screens audience:", screens_audience)


def convert_audience_impressions_to_impression_multiplier(day_impressions_audience, mall_screens, du_bs_screens, hours_range):
  #convert audience impressions to impressions multiplier 
  #param  day impressions : impressiones por hora 09-24
  #       du_bs_screens : 
  #       hours


  print("Daily Impressions: ", day_impressions_audience)
  print("mall screens: ", mall_screens)
  print("broadsign Display unit screens: ", du_bs_screens)
  print("hour range: ", hours_range)
  print("Total Day Impressions: ", np.sum(day_impressions_audience))
  print("Avg Day Impressions per screen in mall: ", np.sum(day_impressions_audience)/mall_screens)

  #l1 = numpy.array(day_impressions_audience)
  h1 = np.array(hours_range)

  print(h1)

  l1 = day_impressions_audience*h1  #just to the hours running
  l1 = l1/mall_screens

  factor = 300 

  impression_multiplier = l1/factor

  #print("impression Multiplier array: ", impression_multiplier)
  return(impression_multiplier)

def calculate_day_audience_impressions(imp_multiplier, du_day_repetitions):

  #evently distributed impressions 9 -22
  return(np.sum(imp_multiplier*(du_day_repetitions/13)))

def get_concentrations(mycursor):

  #agafar les concentracions
  #sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 35) #target hombre
  sql_select_total_imp= "SELECT default_dem_male FROM malls WHERE id LIKE '%s'" %(str(mall_id)) #target hombre
  
  mycursor.execute(sql_select_total_imp)
  print(sql_select_total_imp)
  records_concentration = mycursor.fetchall()

  #if no data use default VALUE
  if mycursor.rowcount==0:
      print("No man concentration available for " + str(mall_name) +" found for " + day + ", setting to 0.45 ")
      concentration_male=0.46
  else:
      for rows_3 in records_concentration:
          concentration_male = rows_3[0]
          print("Concentracion male  " + str(mall_name) + " : " + str(rows_3[0]))

  #agafar les concentracions
  #print("**** concentracin mujeres*****")
  #sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 36) #target mujeres
  sql_select_total_imp= "SELECT default_dem_female FROM malls WHERE id LIKE '%s'" %(str(mall_id)) #target hombre
  mycursor.execute(sql_select_total_imp)
  records_concentration = mycursor.fetchall()

  #if no data use default VALUE
  if mycursor.rowcount==0:
      print("No woman concentration available for " + str(mall_name) + " id " + str(mall_id) +" found for " + day + ", setting to 0.55 ")
      concentration_female=0.54

  else:
      for rows_3 in records_concentration:
          concentration_female = rows_3[0]
          print("Concentracion female  " + str(mall_name) + " : " + str(rows_3[0]))


  #agafar les concentracions
  #print("**** concentracin child*****")
  #sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 24) #target hombre
  sql_select_total_imp= "SELECT default_age_kid FROM malls WHERE id LIKE '%s'" %(str(mall_id)) #target hombre
  mycursor.execute(sql_select_total_imp)
  records_concentration = mycursor.fetchall()

  #if no data use default VALUE
  if mycursor.rowcount==0:
      print("No child concentration available for " + str(mall_name) + " id " + str(mall_id) +" found for " + day + ", setting to 0.05 ")
      concentration_child=0.05

  else:
      for rows_3 in records_concentration:
          concentration_child = rows_3[0]
          print("Concentracion kid  " + str(mall_name) + " : " + str(rows_3[0]))



  #agafar les concentracions
  #print("**** concentracin young*****")
  #sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 25) #target hombre
  sql_select_total_imp= "SELECT default_age_young FROM malls WHERE id LIKE '%s'" %(str(mall_id)) #target hombre
  mycursor.execute(sql_select_total_imp)
  records_concentration = mycursor.fetchall()

  #if no data use default VALUE
  if mycursor.rowcount==0:
      print("No young concentration available for " + str(mall_name) + " id " + str(mall_id) +" found for " + day + ", setting to 0.4 ")
      concentration_young=0.39

  else:
      for rows_3 in records_concentration:
          concentration_young = rows_3[0]
          print("Concentracion young " + str(mall_name) + " : " + str(rows_3[0]))


  #agafar les concentracions
  #print("**** concentracin adult*****")
  #sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 26) #target hombre
  sql_select_total_imp= "SELECT default_age_adult FROM malls WHERE id LIKE '%s'" %(str(mall_id)) #target hombre
  mycursor.execute(sql_select_total_imp)
  records_concentration = mycursor.fetchall()

  #if no data use default VALUE
  if mycursor.rowcount==0:
      print("No adult concentration available for " + str(mall_name) + " id " + str(mall_id) + " found for " + day + ", setting to 0.4")
      concentration_adult=0.41

  else:
      for rows_3 in records_concentration:
          concentration_adult = rows_3[0]
          print("Concentracion adult  " + str(mall_name) + " : " + str(rows_3[0]))

  #agafar les concentracions
  #print("**** concentracion senior*****")
  #sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 27) #target hombre
  sql_select_total_imp= "SELECT default_age_senior FROM malls WHERE id LIKE '%s'" %(str(mall_id)) #target hombre
  mycursor.execute(sql_select_total_imp)
  records_concentration = mycursor.fetchall()

  #if no data use default VALUE
  if mycursor.rowcount==0:
      print("No senior concentration available for " + str(mall_name) + " id " + str(mall_id) + " found for " + day + ", setting to 0.15 ")
      concentration_senior=0.15

  else:
      for rows_3 in records_concentration:
          concentration_senior = rows_3[0]
          print("Concentracion senior " + str(mall_name) + " : " + str(rows_3[0]))

  return concentration_male, concentration_female, concentration_child, concentration_young, concentration_adult,concentration_senior


#*****************************************
#get arguments 
option = sys.argv[1]
try:
    country= sys.argv[2]
except:
    country = ""
try:
    arg_3 = sys.argv[3]
except:
    arg_3  =""

YEAR_TO_ANALYZE=2022



#URLS -----------
url_reservation_by_display_unit= 'https://api.broadsign.com:10889/rest/reservation/v20/by_du_folder?domain_id=17244398&current_only=false';
url_container_info= 'https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_display_unit_info= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398';
url_container_scoped_peru= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=60141576';
url_campaign_performance='https://api.broadsign.com:10889/rest/campaign_performance/v6/by_reservable_id?domain_id=17244398';
url_campaign_audience= 'https://api.broadsign.com:10889/rest/campaign_audience/v1/by_reservation_id?domain_id=17244398';
url_display_unit_performance="https://api.broadsign.com:10889/rest/display_unit_performance/v5/by_reservable_id?domain_id=17244398"
url_display_unit_audience= "https://api.broadsign.com:10889/rest/display_unit_audience/v1/by_reservation_id?domain_id=17244398"
url_container_id='https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_reservation_by_id= 'https://api.broadsign.com:10889/rest/reservation/v20/by_id?domain_id=17244398';
url_schedule_by_reservation= 'https://api.broadsign.com:10889/rest/schedule/v8/by_reservable?domain_id=17244398';

target_impressions=0

container_ids=[]
container_name=[]
reservation={}
malls={}

#database connector
mydb = mysql.connector.connect(
  host="ec2-52-18-248-109.eu-west-1.compute.amazonaws.com",
  #host="54.38.184.204",
  user="root",
  #user="iwall",
  #database="netmon",
  #passwd= "iwalldigitalsignage",
  passwd="SonaeRootMysql2021!",
  database="audience"
)

mycursor = mydb.cursor()

#Authorization
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";

print("\n \n")
print("IWALL CAMPAIGN AUDIENCE REPORT")
print("------------------------------")

if option == 'all':
    all_campaigns= True
elif option== 'id':
    campaign_id = sys.argv[3]
    all_campaigns=False
    try:
        if sys.argv[4]:
            target_impressions = sys.argv[4]
    except:
        target_impressions = 0
    print("Option selected: analyzing campaign by id")
elif option == 'name':
    campaign_name=sys.argv[3]
    all_campaigns=False
    print("Option selected: analyzing campaign by name")
elif option == 'reset_mall_model':
    all_campaigns=False
    reset_mall = sys.argv[3]
    if sys.argv[3] == "all":
        generate_default_audience_data(mycursor, reset_mall, 1)
    else:
        generate_default_audience_data(mycursor, reset_mall, 0)
    print("Option selected: resetting mall audience data")
    exit()
elif option == 'list_malls': 
    list_malls(mycursor, country)
    exit()
elif option =='mall_check':
    pre_check(country, auth, mycursor)
elif option == "find_campaign":
    find_campaign = sys.argv[3]
elif option == "delete_all":
    delete_all_campaign_data()
    exit()
elif option == "help":
    print("")
    print("Help menu ")
    print("**********")
    print("")
    print("bs-campaigns-report.py <option> <country> <additional1> <additional2>")
    print("\nOptions available:")
    print("------------------")
    print("all : analyze all campaigns in country. no additional options are used")
    print("id : analyze campaign by id . additional1 should be the campaign id. additional2 can be the target impressions that the campaign is expected to have")
    print("name : campaign report by name. value should be name of the campaign")
    print("reset_mall_maodel: will restablish the mall model . additional1=all will restablish all malls, else the container id ")
    print("list_malls")
    print("mall_check")
    print("find_campaign")
    exit()
else: 
    print("Options not recognized")
    exit()



print("********************")
print("Extracting CAMPAIGNS")
print("campaigns to analyze: ", option)

excluded_containers= get_not_production_campaign_containers()

container_ids=[]
container_name=[]
reservation={}
malls={}
reservations=[]


#campaign selection --- 

if (option == "all") or (option =="name") or (option == "id") or (option =="find_campaign") or (option =="finished") : 

    if country=="SPAIN":
        container_ids=["106135296"]

    if country=="COLOMBIA":
        container_ids=['120956285']

    if country=="PERU":
        container_ids=['60141576']


    #get reservation info from broadsign
    for m in container_ids:

        url_reservation_container=url_reservation_by_display_unit+"&container_ids=" +m
        #print(url_reservation_container)
        s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth})
        data=json.loads(s.text)

        with open('test.txt', 'w') as f:
            f.write(json.dumps(data))
        


        #print data
        for n in data["reservation"]:
            insert=1
            reservation["mall_container_id"]=m
            reservation["booking_state"]=str(n["booking_state"])
            reservation["saturation"]=str(n["saturation"])
            reservation["duration_msec"]=str(n["duration_msec"])
            reservation["start_time"]=str(n["start_time"])
            reservation["start_date"]=str(n["start_date"])
            fecha_inicio=datetime.datetime.strptime(str(n["start_date"]),"%Y-%m-%d")
            fecha_fin=datetime.datetime.strptime(str(n["end_date"]),"%Y-%m-%d")
            reservation["active"]="unknown"
            if fecha_inicio < datetime.datetime.today():
                    if fecha_fin > datetime.datetime.today():
                            reservation["active"]="Running"
            if fecha_inicio>datetime.datetime.today():
                    reservation["active"]="por emitir"
            if fecha_fin<datetime.datetime.today():
                    reservation["active"]="Emitida"
            delta=fecha_fin-fecha_inicio
            reservation["days"]=0
            reservation["days"]=delta.days+1
            reservation["end_time"]=str(n["end_time"])
            reservation["end_date"]=str(n["end_date"])
            reservation["campaign_id"]=str(n["id"])
            reservation["state"]=str(n["state"])
            #reservation["name"]=n["name"].encode('utf-8', errors ='ignore')
            reservation["name"]=n["name"]
            reservation["country"]=country
            reservation["last_updated"]=datetime.datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
            #reservation["mall"]=malls[m]
            name=n["name"].encode('utf-8', errors ='ignore')
            #if re.findall('\$(.*)\$',name):
            #    reservation["SAP_ID"]=re.findall('\$(.*)\$', name)[0]
            #else:
            reservation["SAP_ID"]="not set"


            #check if campaign is in production folders  and not mall internal campaigns
            if not n['container_id'] in excluded_containers:
                reservations.append(reservation)
                print("****adding campaign " + reservation['name'] )
            else:
                print("exlcuding campaign " + reservation['name'])
            
            reservation={}
    

    #convert reservations to dataframe            
    df_reservations = pd.DataFrame(reservations)
    print(df_reservations)
    #input()


    #campaign selection filters
    df_reservations['start_date']=pd.to_datetime(df_reservations['start_date'], format="%Y-%m-%d")
    df_reservations['end_date']=pd.to_datetime(df_reservations['end_date'], format="%Y-%m-%d")

    #add date and time columns for aggregation
    df_reservations=df_reservations.set_index('start_date')
    df_reservations['year']=df_reservations.index.year
    df_reservations['month']=df_reservations.index.month

    #today
    today = pd.to_datetime("today")


    #remove all programmatic campaigns
    df_reservations = df_reservations[~df_reservations['name'].str.contains("PROGRAMMATIC", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("AUTOPROMO", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("CORPORATIVO", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("CORPORATIVA", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("PROMO", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("TEST", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("OFICINA", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("FILLER", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("ANUAL 2021", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("ANUAL 2020", na = False) ]
    df_reservations = df_reservations[~df_reservations['name'].str.contains("FOTOS", na = False) ]


    if option == "find_campaign":     
        print("Finding campaigns from name ", find_campaign)
        # show all columns in pandas
        print("Results: *******")
        pd.set_option('display.max_columns', None)
        df_reservations = df_reservations[df_reservations['name'].str.contains(find_campaign, na = False) ]
        #print(df_reservations)
        exit()

    #select campaigns to analyze
    if all_campaigns:
        #print("Selecting current  year campaigns") 
        df_reservations = df_reservations[(df_reservations['year'] ==  YEAR_TO_ANALYZE)] 

    if option=="name":
        print("Getting campaigns from name ", sys.argv[3])
        df_reservations = df_reservations[df_reservations['name'].str.contains(sys.argv[3], na = False) ]
        df_reservations = df_reservations[(df_reservations['year'] ==YEAR_TO_ANALYZE)] 
        #print(df_reservations)

    if option=="id":
        print("Getting campaigns from id ", sys.argv[3])
        df_reservations = df_reservations[df_reservations['campaign_id'].str.contains(sys.argv[3], na = False) ]
        df_reservations = df_reservations[(df_reservations['year'] ==YEAR_TO_ANALYZE)] 
        #print(df_reservations)


    #removed already analyzed campaigns that have been finished
    sql_select_analyzed= "SELECT reservation_id, name from campaign_analysis where active='Emitida'"

    mycursor.execute(sql_select_analyzed)
    records_0= mycursor.fetchall()


    if (option =="all") or (option == "finished"):
        for row_0 in records_0:  #for each display unit

            print("Removing already analyzed campaign ",row_0[0], " with name ", row_0[1])
            df_reservations = df_reservations[~df_reservations['campaign_id'].str.contains(str(row_0[0]), na = False) ]

            if option =="finished":
                print("Only analyzing finished campaigns that have not been analyzed so far")
                df_reservations = df_reservations[(df_reservations['year'] == YEAR_TO_ANALYZE)] 
                df_reservations = df_reservations[(df_reservations['active'] =="Emitida")] 
      


    print("\n \n")
    print("Campaigns to analyze: ")
    print("----------------------")
    print(df_reservations[['name', 'year', 'month', 'campaign_id', 'end_date']])

    #analysis
    campaigns=df_reservations['campaign_id'].to_list()
    campaign_daily_performance={}
    campaign_daily_audience={};
    campaign_display_unit_performance={};
    campaign_display_unit_audience={};

    campaigns_name=df_reservations['name'].to_list()


print("Press a key to proceed...")
input()

print("\n  \n")
print("Beginning Analysis")
print("------------------")
for row in campaigns:  #for each campaign to analyze

  print("Analyzing reservation ID  " + str(row))
  reservation_id=row

  delete_campaign_data(row)

  campaign_name=df_reservations.loc[df_reservations['campaign_id'] == row].name[0]
  print("Campaign name in broadsign", campaign_name)

  #********* CHECK IF CAMPAIGN ORDER EXISTS *******************************************


  #***************** GET RESERVATION DATA from Broadsign ******************************
  #************************************************************************************

  url_reservation_by_id=url_reservation_by_id+"&ids=" +str(reservation_id);
  s=requests.get(url_reservation_by_id,headers={'Accept': 'application/json','Authorization': auth});
  data=json.loads(s.text)

  #print(data)

  #get reservation data
  for n in data["reservation"]:

    reservation["name"]=str(n["name"].encode('utf-8', errors ='ignore'))
    reservation["saturation"]=str(n["saturation"])
    reservation["duration_msec"]=str(n["duration_msec"])
    reservation["start_time"]=str(n["start_time"])
    reservation["start_date"]=str(n["start_date"])
    fecha_inicio=datetime.datetime.strptime(str(n["start_date"]),"%Y-%m-%d")
    fecha_fin=datetime.datetime.strptime(str(n["end_date"]),"%Y-%m-%d")
 
    delta=fecha_fin-fecha_inicio
    reservation["days"]=0
    reservation["days"]=delta.days+1
    reservation["end_time"]=str(n["end_time"])
    reservation["end_date"]=str(n["end_date"])


    #name=n["name"].encode('utf-8', errors ='ignore')
    name=n["name"]
    print("campaign name", name)

    if country=="SPAIN":
      if re.findall('\$(.*)\$',name):
          reservation["SAP_ID"]=re.findall('\$(.*)\$', name)[0]
      else:
          reservation["SAP_ID"]="not set"
      name=re.sub('\$(.*)\$','', name)

    if country=="PERU":
      if re.findall('\%(.*)\%',name):
          reservation["SAP_ID"]=re.findall('\%(.*)\%', name)[0]
      else:
          reservation["SAP_ID"]="not set"
      name=re.sub('\%(.*)\%','', name)

    if country=="COLOMBIA":
      if re.findall('\$(.*)\$',name):
          reservation["SAP_ID"]=re.findall('\$(.*)\$', name)[0]
      else:
          reservation["SAP_ID"]="not set"
      name=re.sub('\$(.*)\$','', name)
    

    print("SAP id", reservation["SAP_ID"])

    reservation["name"]=name

    schedule_start_date=""
    schedule_end_date=""
    schedule_days=0

    url_schedule=url_schedule_by_reservation+"&id="+str(n["id"])
    s=requests.get(url_schedule,headers={'Accept': 'application/json','Authorization': auth});
    data_schedules=json.loads(s.text)
    #print(data_schedules)
    num_schedules=0

    for o in data_schedules["schedule"]:

      if o["active"] == True:
        num_schedules=num_schedules +1
        schedule_fecha_inicio=datetime.datetime.strptime(str(o["start_date"]),"%Y-%m-%d")
        schedule_fecha_fin=datetime.datetime.strptime(str(o["end_date"]),"%Y-%m-%d")


        if schedule_start_date=="":
          schedule_start_date=schedule_fecha_inicio
        if schedule_end_date=="": 
          schedule_end_date=schedule_fecha_fin
        if schedule_fecha_inicio<=schedule_start_date:
          schedule_start_date=schedule_fecha_inicio
        if schedule_fecha_fin>=schedule_end_date:
          schedule_end_date=schedule_fecha_fin


    reservation["active"]="unknown"     
    if num_schedules>0:
      reservation["schedule_end_date"]=str(schedule_end_date)       
      reservation["schedule_start_date"]=str(schedule_start_date)
      delta=schedule_end_date-schedule_start_date
      schedule_days=delta.days+1
   
      if schedule_start_date <= datetime.datetime.today():
          if schedule_end_date >= datetime.datetime.today():
            reservation["active"]="Running"
      if schedule_start_date>=datetime.datetime.today():
        reservation["active"]="por emitir"
      if schedule_end_date<=datetime.datetime.today():
        reservation["active"]="Emitida"

    else:
      schedule_days=0
      reservation["schedule_end_date"]=str(n["end_date"])
      reservation["schedule_start_date"]=str(n["start_date"])
 

    print("")
    print("---------------------")
    print("Campaign:  ")
    print("---------------------")

    print("Name " + str(n["name"]))
    print("start:"+ str(n["start_date"]))
    print("end:"+ str(n["end_date"]))
    print("Campaign Days "+ str(reservation["days"]))


    #update campaign analysis
    sql= "INSERT INTO campaign_analysis (schedule_days, schedules, schedule_start_date, schedule_end_date, SAP_id, country, campaign, name,reservation_id, start_date, end_date, saturation, duration_msec, active, days, description,total_screens_order) VALUES (%s,%s,%s,%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val= (schedule_days,num_schedules,reservation["schedule_start_date"], reservation["schedule_end_date"],reservation["SAP_ID"], country, reservation["name"],reservation["name"],reservation_id,str(n["start_date"]),str(n["end_date"]),str(n["saturation"]),n["duration_msec"],reservation["active"],reservation["days"], "Broadsign data" , "null" )
    mycursor.execute(sql,val)
    mydb.commit()

    campaign_name=reservation["name"]


  
  #Performance report:


  print("")
  print("-------------------------------------")
  print("   Campaign Daily Performance:")
  print("-------------------------------------")
  url_campaign_performance=url_campaign_performance+"&reservable_id=" +str(reservation_id);
  s=requests.get(url_campaign_performance,headers={'Accept': 'application/json','Authorization': auth});
  data=json.loads(s.text)

  #print(data)
  #initialize data
  campaign_days=[] #delete all days

  repetition_distribution = []  #reset distribution of repetitions

  #analyze data
  for n in data["campaign_performance"]:

    if n["total"]>100:
        campaign_daily_performance["total_impressions"]=n["total_impressions"]
        campaign_daily_performance["played_on"]=str(n["played_on"])
        campaign_daily_performance["repetitions"]=n["total"]             #*******************************************************************
        campaign_daily_performance["reservation_id"]=n["reservable_id"]

        campaign_days.append(str(n["played_on"]));  #add days to array
        print(str(n["played_on"]) + " Repetitions: " + str(n["total"]) )

        #print campaign_daily_performance
        sql= "INSERT INTO campaign_daily_performance (country, campaign, total_impressions, played_on, repetitions, reservation_id) VALUES (%s,%s,%s,%s,%s,%s)"
        val= (country, campaign_name, n["total_impressions"],str(n["played_on"]),n["total"],n["reservable_id"])
        mycursor.execute(sql,val)
        mydb.commit()

        repetition_distribution.append(n["total"])

  #calculate repetition_distribution_normalization
  total_reps= sum(repetition_distribution)
  for reps_index, reps_value in enumerate(repetition_distribution):
        repetition_distribution[reps_index]=repetition_distribution[reps_index]/total_reps

  #display uit Performance
  #Performance reports:
  print("")
  print("-------------------------------------")
  print("      Display Unit Performance: ")
  print("-------------------------------------")
  url_display_unit_performance=url_display_unit_performance+"&reservable_id=" +str(reservation_id);
  #print url_display_unit_performance
  s=requests.get(url_display_unit_performance,headers={'Accept': 'application/json','Authorization': auth});
  data=json.loads(s.text)

  print("Display units in the campaign: "+ str(len(data["display_unit_performance"])))

  num_screens_total = 0
  num_plays_total = 0
  

  for n in data["display_unit_performance"]:
     campaign_display_unit_performance["total_impressions"]=n["total_impressions"]
     campaign_display_unit_performance["repetitions"]=n["total"]
     campaign_display_unit_performance["display_unit_id"]=n["display_unit_id"]
     #get mall name from this display unit
     url_display_unit_info=url_display_unit_info+"&ids=" +str(n["display_unit_id"]);
     s=requests.get(url_display_unit_info,headers={'Accept': 'application/json','Authorization': auth});
     
     try: 
        data_name=json.loads(s.text)
        for m in data_name["display_unit"]:
          url_container_id=url_container_id+"&ids=" +str(m["container_id"]);
          campaign_display_unit_performance["container_id"]=m["container_id"]
          campaign_display_unit_performance["screen_count"]=m["host_screen_count"]
          num_screens_total = num_screens_total + int(m["host_screen_count"])
          campaign_display_unit_performance["display_unit_name"]=str(m["name"].encode('utf-8', errors ='ignore'))

          s=requests.get(url_container_id,headers={'Accept': 'application/json','Authorization': auth});
          data_mall_name=json.loads(s.text)
          #print data_mall_name
          for o in data_mall_name["container"]:
              campaign_display_unit_performance["mall_name"]=o["name"].encode('utf-8', errors ='ignore')
              print("Repetitions for site "+ str(o["name"])+ " and Display unit " + str(m["name"]) + ": " +str(n["total"]) + " ( " + str(m["host_screen_count"]) +" screens)" )

        campaign_display_unit_performance["reservation_id"]=n["reservable_id"]
        campaign_display_unit_performance["repetitions"]=n["total"]

        num_plays_total = num_plays_total + (int(n["total"]))

        sql= "INSERT INTO campaign_display_unit_performance (campaign, display_unit_id, container_id, screen_count, display_unit_name, mall_name, total_impressions, repetitions, reservation_id) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s)"
        val= (campaign_name, n["display_unit_id"],m["container_id"],m["host_screen_count"],str(m["name"]),o["name"].encode('utf-8', errors ='ignore'),n["total_impressions"],n["total"],n["reservable_id"])
        mycursor.execute(sql,val)
        mydb.commit()




     except: 
        print("***********Error decoding JSON!!!!")


  #update campaign analysis
  #update de total number of screens that have emitted the campaign

  print("updating campaign analysis... target Impressions: ", target_impressions , " total screens : ", num_screens_total, " total plays: ", num_plays_total)
  sql= "UPDATE campaign_analysis SET  total_screens_order=%s, total_plays=%s, target_audience_impressions=%s WHERE reservation_id=%s"
  val= (num_screens_total,num_plays_total, target_impressions,reservation_id )
  
  mycursor.execute(sql,val)
  mydb.commit()    




  #print campaign_display_unit_performance
  print ("")
  print("-------------------------------------")
  print("     Display Unit Audience:          ")
  print("-------------------------------------")


  num_impressions_total = 0
  total_male =0
  total_female = 0
  total_kid=0
  total_young =0 
  total_adult =0
  total_senior = 0


  daily_totals=[]
  con_male=[]
  con_female=[]
  con_child=[]
  con_young=[]
  con_adult=[]
  con_senior=[]

  con_f_child=[]
  con_f_young=[]
  con_f_adult=[]
  con_f_senior=[]
  con_m_child=[]
  con_m_young=[]
  con_m_adult=[]
  con_m_senior=[]

  records_0={}
  #to loop for each display unit
  sql_select_containers= "SELECT container_id, repetitions, screen_count, display_unit_id, display_unit_name, mall_name from campaign_display_unit_performance where reservation_id='%s'" % (str(reservation_id))

  mycursor.execute(sql_select_containers)
  records_0= mycursor.fetchall()
  #print("Number of Display units to analyze: " + str(mycursor.rowcount))

  for row_0 in records_0:  #for each display unit
      print("+++++++++++++++++++++++++++++++")
      print("Display unit:" + str(row_0[4]))
      print("Container ID :" + str(row_0[0]));
      print("Number of repetitions: " + str(row_0[1]))
      print("Display unit screens:  " + str(row_0[2]))

      du_bs_screens=row_0[2]
      du_bs_repetitions=row_0[1]

      if du_bs_screens == 0:
        print ("No screens for this display unit. not using it....")
        continue 

      if du_bs_repetitions == 0:
        #no repetitions achieved for that DU , approximating
        print(" No repetitions from broadsign... approximating")
        du_bs_repetitions = 540*du_bs_screens*total_days


      #Extracting mall id and number of screens for malls table
      print("Getting information from malls database:")
      sql_select_getmall = "SELECT id, screens, default_screen_day_impressions, default_screen_day_views, name FROM malls WHERE broadsign_container_id='%s'" % (str(row_0[0]))
      mycursor.execute(sql_select_getmall)
      records_1= mycursor.fetchall()
      print("Number malls found: " + str(mycursor.rowcount))
      if mycursor.rowcount >= 1:
          for rows_1 in records_1:
              text="Display unit " + str(row_0[4]) + " belongs to Site " + str(rows_1[4]) + " with " + str(rows_1[1]) + " screens"
              print(text)
              mall_id= rows_1[0]
              mall_screens = rows_1[1]
              default_screen_day_impressions= rows_1[2]
              default_screen_day_views = rows_1[3]
              mall_name = rows_1[4]
      else:
          print("Error ************ - no container found in AUDIENCE MALLS DB!!!!!!!!!")
          mall_id = 0
          mall_screens = 0
          default_screen_day_impression = 10
          default_screen_day_views = 5
          mall_name = "NOT FOUND"
          continue
      print("")
      print("+++++++++++++++++++++++++++++++")

      total_days=len(campaign_days)
      total_du_campaign_impressions=0
      total_campaign_day_impressions=0
      total_con_male=0
      total_con_female=0
      total_con_child=0
      total_con_young=0
      total_con_adult=0
      total_con_senior=0

      total_con_f_child=0
      total_con_f_young=0
      total_con_f_adult=0
      total_con_f_senior=0
      total_con_m_child=0
      total_con_m_young=0
      total_con_m_adult=0
      total_con_m_senior=0

      f_child=0
      f_young=0
      f_adult=0
      f_senior=0
      m_child=0
      m_young=0
      m_adult=0
      m_senior=0

      daily_totals_2=[]
      con_male_2=[]
      con_female_2=[]
      con_child_2=[]
      con_young_2=[]
      con_adult_2=[]
      con_senior_2=[]

      con_f_child_2=[]
      con_f_young_2=[]
      con_f_adult_2=[]
      con_f_senior_2=[]
      con_m_child_2=[]
      con_m_young_2=[]
      con_m_adult_2=[]
      con_m_senior_2=[]

      print (" ")
      print ("Daily Analysis for " + mall_name + " - mall id: " + str(mall_id))
      print("----------------------------------------------------------------")

      day_formatted=""
      date_unformatted=[]


      print("Repetitions distribution: ", repetition_distribution)
      #input()

      #calculate daily impressions for that display unit and each day
      for day_index, day in enumerate(campaign_days):

          #try: 
            print("")
            date_unformatted=day.split("-")
            day_formatted=date_unformatted[0]+"-"+date_unformatted[1]+"-"+date_unformatted[2]
            print("Day: ",  day_formatted)

            # buscar numero de impresiones ese dia para ese display unit
            sql_select_total_imp= "SELECT * from audience_impressions WHERE date LIKE '%s' AND mall_id LIKE '%s'" %(str(day_formatted), str(mall_id))
            #print(sql_select_total_imp)
            mycursor.execute(sql_select_total_imp)
            records_day_impressions = mycursor.fetchall()


            #print(records_day_impressions)
            day_impressions_audience=records_day_impressions[0][12:25]
            #print(day_impressions_audience)

            #hours of campaign running ( now 9-24)
            hours_range=[1,1,1,1,1,1,1,1,1,1,1,1,1]
            if mycursor.rowcount==0:
                print("No audience data for mall " + str(mall_name) + " id " + str(mall_id) + " found for " + day + ", using default value "+ str(default_screen_day_impressions))
                day_impressions= default_screen_day_impressions
                day_impressions_audience= np.ones(15)*((day_impressions*row_0[2])/15)   #numero de pantallas del display unit

            imp_multiplier = convert_audience_impressions_to_impression_multiplier(day_impressions_audience, mall_screens, du_bs_screens, hours_range)
            imp_multiplier = imp_multiplier; 


            print("DU BS Repetitions: ", du_bs_repetitions, " total days: ", total_days)
            du_day_repetitions=du_bs_repetitions*repetition_distribution[day_index]

           

            du_day_campaign_impressions= calculate_day_audience_impressions(imp_multiplier, du_day_repetitions)

            du_day_campaign_impressions = int(du_day_campaign_impressions)

            print("Repeticiones: ", du_day_repetitions)
            print("Audience Impressions", du_day_campaign_impressions)


            num_impressions_total=num_impressions_total + du_day_campaign_impressions

            print("DU DAY impressions =",  int(du_day_campaign_impressions))

            concentration_male, concentration_female, concentration_child, concentration_young, concentration_adult, concentration_senior = get_concentrations(mycursor)         

            if du_day_campaign_impressions == 0:
                du_day_campaign_impressions = 1


            daily_totals_2.append(int(du_day_campaign_impressions))

            con_male_2.append(round(du_day_campaign_impressions*concentration_male))
            con_female_2.append(round(du_day_campaign_impressions*concentration_female))
            con_child_2.append(round(du_day_campaign_impressions*concentration_child))
            con_young_2.append(round(du_day_campaign_impressions*concentration_young))
            con_adult_2.append(round(du_day_campaign_impressions*concentration_adult))
            con_senior_2.append(round(du_day_campaign_impressions*concentration_senior))

            c_child=round(du_day_campaign_impressions*concentration_child)
            c_female=round(du_day_campaign_impressions*concentration_female)
            c_male=round(du_day_campaign_impressions*concentration_male)
            c_young=round(du_day_campaign_impressions*concentration_young)
            c_adult=round(du_day_campaign_impressions*concentration_adult)
            c_senior=round(du_day_campaign_impressions*concentration_senior)

            #print "Concentration " +str(c_child) + " "+ str(c_female) +" " + str(c_male) + " "+ str(c_adult) +" "+ str(c_young) +" " + str(c_senior)

            c_m_child=round(c_child*c_male/(c_male+c_female))
            c_m_young=round(c_young*c_male/(c_male+c_female))
            c_m_adult=round(c_adult*c_male/(c_male+c_female))
            c_m_senior=round(c_senior*c_male/(c_male+c_female))
            c_f_child=round(c_child*c_female/(c_male+c_female))
            c_f_young=round(c_young*c_female/(c_male+c_female))
            c_f_adult=round(c_adult*c_female/(c_male+c_female))
            c_f_senior=round(c_senior*c_female/(c_male+c_female))

            con_f_child_2.append(c_f_child)
            con_f_young_2.append(c_f_young)
            con_f_adult_2.append(c_f_adult)
            con_f_senior_2.append(c_f_senior)
            con_m_child_2.append(c_m_child)
            con_m_young_2.append(c_m_young)
            con_m_adult_2.append(c_m_adult)
            con_m_senior_2.append(c_m_senior)

            #print("Display unit repetitions para ese dia " + str(du_day_repetitions) + " de un total de " + str(row_0[1]) + ",  "+ str(total_days) + " dias de campana")
            #print("Impresiones ponderadas para dia " + day_formatted +": "+ str(du_day_campaign_impressions)+ " de un total de " +str(day_impressions) +" diarias del mall y " + str(du_day_impressions) + " del display unit en mall " + str(mall_id) )

            total_du_campaign_impressions=total_du_campaign_impressions+du_day_campaign_impressions

            total_con_male=total_con_male + round(du_day_campaign_impressions*concentration_male)
            total_con_female=total_con_female + round(du_day_campaign_impressions*concentration_female)
            total_con_child=total_con_child + round(du_day_campaign_impressions*concentration_child)
            total_con_young=total_con_young + round(du_day_campaign_impressions*concentration_young)
            total_con_adult=total_con_adult + round(du_day_campaign_impressions*concentration_adult)
            total_con_senior=total_con_senior + round(du_day_campaign_impressions*concentration_senior)


            total_male=total_male + round(du_day_campaign_impressions*concentration_male)
            total_female=total_female+ round(du_day_campaign_impressions*concentration_female)
            total_kid=total_kid + round(du_day_campaign_impressions*concentration_child)
            total_young=total_young + round(du_day_campaign_impressions*concentration_young)
            total_adult=total_adult + round(du_day_campaign_impressions*concentration_adult)
            total_senior=total_senior + round(du_day_campaign_impressions*concentration_senior)

            f_child=round(total_con_child*total_con_female/(total_con_male+total_con_female))
            f_young=round(total_con_young*total_con_female/(total_con_male+total_con_female))
            f_adult=round(total_con_adult*total_con_female/(total_con_male+total_con_female))
            f_senior=round(total_con_senior*total_con_female/(total_con_male+total_con_female))
            m_child=round(total_con_child*total_con_male/(total_con_male+total_con_female))
            m_young=round(total_con_young*total_con_male/(total_con_male+total_con_female))
            m_adult=round(total_con_adult*total_con_male/(total_con_male+total_con_female))
            m_senior=round(total_con_senior*total_con_male/(total_con_male+total_con_female))

          
          #except:

          #  screen_day_reps=0
           # factor_corrector=0
          #  print ("Error with display UNIT ******")


      try:         
        #update impressions in display unit performance
        sql= "UPDATE campaign_display_unit_performance  SET  total_impressions=%s WHERE display_unit_id=%s AND campaign=%s"
        val= (total_du_campaign_impressions,row_0[3],campaign_name)
        mycursor.execute(sql,val)
        mydb.commit()
        daily_totals.append(daily_totals_2)

        print(daily_totals)

        #update audience for display unit
        sql= "INSERT INTO campaign_display_unit_audience (country, campaign, display_unit_id, container_id, screen_count, display_unit_name, mall_name, female_child, female_young,female_adult,female_senior,female_unknown,male_child,male_young,male_adult,male_senior,male_unknown,unknown_child,unknown_young,unknown_adult,unknown_senior, unknown_unknown, male, female, kid, young, adult, senior, reservation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val= (country, campaign_name,row_0[3],row_0[0],row_0[2], str(row_0[4].encode('utf-8', errors ='ignore')),str(row_0[5].encode('utf-8', errors ='ignore')),f_child,f_young,f_adult,f_senior,0,m_child,m_young,m_adult,m_senior,0,0,0,0,0,0,total_con_male,total_con_female, total_con_child, total_con_young, total_con_adult, total_con_senior, reservation_id)
        mycursor.execute(sql,val)
        mydb.commit()
      except e:

        print("***********Error updating display unit performance")


      con_male.append(con_male_2)
      con_female.append(con_female_2)
      con_child.append(con_child_2)
      con_young.append(con_young_2)
      con_adult.append(con_adult_2)
      con_senior.append(con_senior_2)

      con_m_child.append(con_m_child_2)
      con_m_young.append(con_m_young_2)
      con_m_senior.append(con_m_senior_2)
      con_m_adult.append(con_m_adult_2)
      con_f_child.append(con_f_child_2)
      con_f_young.append(con_f_young_2)
      con_f_adult.append(con_f_adult_2)
      con_f_senior.append(con_f_senior_2)

      #print(" temp ------- ")
      #print daily_totals_2
      #print("")
      #print daily_totals

      print ("")


  print("Impressiones totales CAMPAÑA", num_impressions_total)
  #update campaign analysis
  #update de total number of screens that have emitted the campaign
  sql= "UPDATE campaign_analysis SET  total_audience_impressions=%s, total_male=%s, total_female=%s, total_kid=%s, total_young=%s, total_adult=%s, total_senior=%s WHERE reservation_id=%s"
  val= (int(num_impressions_total),int(total_male), int(total_female), int(total_kid), int(total_young), int(total_adult), int(total_senior), reservation_id )
  mycursor.execute(sql,val)
  mydb.commit()    


  print("")
  print("Daily totals: ")
  print(daily_totals)
  print("")

  try:

    t_matrix= [[daily_totals[j][i] for j in range(len(daily_totals))] for i in range(len(daily_totals[0]))]
    print("")
    print("Daily performance")
    print("-----------------")
    for index,i in enumerate(t_matrix):
        print(i)
        day_total=0
        for j in i:
            day_total=day_total+j

        try:     
          #print campaign_daily_performance
          sql= "UPDATE campaign_daily_performance SET total_impressions=%s WHERE played_on=%s AND reservation_id=%s"
          val= (day_total,campaign_days[index],str(reservation_id))
          mycursor.execute(sql,val)
          mydb.commit()
          sql= "INSERT INTO campaign_daily_audience (campaign, played_on, reservation_id) VALUES (%s, %s, %s) "
          val= (campaign_name, campaign_days[index],str(reservation_id))
          mycursor.execute(sql,val)
          mydb.commit()
        except: 
          print("Error updating Daily performance")

  except:
    print("Error updating Daily performance")


  try: 

    #matrius de concentraciones
    print("")
    print("Daily audience")
    print("-----------------")


    t_matrix= [[con_child[j][i] for j in range(len(con_child))] for i in range(len(con_child[0]))]
    print(t_matrix)
    for index,i in enumerate(t_matrix):
            #print i
            day_total=0
            for j in i:
                day_total=day_total+j
          #print campaign_daily_performance
            try:
                sql= "UPDATE campaign_daily_audience SET kid=%s WHERE played_on=%s AND reservation_id=%s"
                val= (day_total,campaign_days[index], str(reservation_id))
                mycursor.execute(sql,val)
                mydb.commit()
            except Exception as e:
                print("Error audience kid: ", str(e))

    t_matrix= [[con_young[j][i] for j in range(len(con_young))] for i in range(len(con_young[0]))]
    for index,i in enumerate(t_matrix):
          #print i
          day_total=0
          for j in i:
              day_total=day_total+j
          #print campaign_daily_performance
          try:
            sql= "UPDATE campaign_daily_audience SET young=%s WHERE played_on=%s AND reservation_id=%s"
            val= (day_total,campaign_days[index], str(reservation_id))
            mycursor.execute(sql,val)
            mydb.commit()
          except: 
            print("Error updating daily audience male_child")

    t_matrix= [[con_adult[j][i] for j in range(len(con_adult))] for i in range(len(con_adult[0]))]
    for index,i in enumerate(t_matrix):
          #print i
          day_total=0
          for j in i:
              day_total=day_total+j
          #print campaign_daily_performance
          try:
            sql= "UPDATE campaign_daily_audience SET adult=%s WHERE played_on=%s AND reservation_id=%s"
            val= (day_total,campaign_days[index], str(reservation_id))
            mycursor.execute(sql,val)
            mydb.commit()
          except: 
            print("Error updating daily audience male_child")

    t_matrix= [[con_senior[j][i] for j in range(len(con_senior))] for i in range(len(con_senior[0]))]
    for index,i in enumerate(t_matrix):
          #print i
          day_total=0
          for j in i:
              day_total=day_total+j
          #print campaign_daily_performance
          try:
            sql= "UPDATE campaign_daily_audience SET senior=%s WHERE played_on=%s AND reservation_id=%s"
            val= (day_total,campaign_days[index], str(reservation_id))
            mycursor.execute(sql,val)
            mydb.commit()
          except: 
            print("Error updating daily audience male_child")

    t_matrix= [[con_male[j][i] for j in range(len(con_male))] for i in range(len(con_male[0]))]
    for index,i in enumerate(t_matrix):
          #print i
          day_total=0
          for j in i:
              day_total=day_total+j
          #print campaign_daily_performance
          try:
            sql= "UPDATE campaign_daily_audience SET male=%s WHERE played_on=%s AND reservation_id=%s"
            val= (day_total,campaign_days[index], str(reservation_id))
            mycursor.execute(sql,val)
            mydb.commit()
          except: 
            print("Error updating daily audience male_child")


    t_matrix= [[con_female[j][i] for j in range(len(con_female))] for i in range(len(con_female[0]))]
    for index,i in enumerate(t_matrix):
          #print i
          day_total=0
          for j in i:
              day_total=day_total+j
          #print campaign_daily_performance
          try:
            sql= "UPDATE campaign_daily_audience SET female=%s WHERE played_on=%s AND reservation_id=%s"
            val= (day_total,campaign_days[index], str(reservation_id))
            mycursor.execute(sql,val)
            mydb.commit()
          except: 
            print("Error updating daily audience male_child")


    
  except Exception as e:
    print("Error audience: ", str(e))


mycursor.close()

if(mydb.is_connected()):
    mycursor.close()
    print("MySQL connection is closed")