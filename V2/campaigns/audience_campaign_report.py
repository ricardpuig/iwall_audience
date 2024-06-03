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
from statistics import mean
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import datetime
from dateutil.relativedelta import relativedelta

PONDERACION_DIARIA=0.57
DUPLICIDAD_2_SEMANAS=0.63
DUPLICIDAD_3_SEMANAS=0.53
DUPLICIDAD_4_SEMANAS=0.44
DUPLICIDAD_5_SEMANAS=0.34
CORRECCION_DUPLICIDAD=0.24


DUMMY_DAY_PLAYS = 2500
DUMMY_DISPLAY_UNIT_ID=141462004
DUMMY_CONTAINER_ID=229719301
DUMMY_SCREEN_COUNT=13
DUMMY_NAME="PLAZA IMPERIAL"
DUMMY_MALL_NAME="PLAZA IMPERIAL"
DUMMY_PLAYS=2500*9

dummy_data = 0


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
  user="root",
  passwd="SonaeRootMysql2021!",
  database="audience"
)

mycursor = mydb.cursor()
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";

print("\n")
print("IWALL CAMPAIGN AUDIENCE REPORT")
print("------------------------------")

campaign_id = sys.argv[1]
country = sys.argv[2]

if campaign_id:

  campaign_daily_performance={}
  campaign_daily_audience={};
  campaign_display_unit_performance={};
  campaign_display_unit_audience={};

  print("Analyzing reservation ID  " + str(campaign_id))
  reservation_id=campaign_id
  delete_campaign_data(reservation_id)

  url_reservation_by_id=url_reservation_by_id+"&ids=" +str(reservation_id);
  s=requests.get(url_reservation_by_id,headers={'Accept': 'application/json','Authorization': auth});
  data=json.loads(s.text)

  #get reservation data
  print("Getting reservation information....")
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

    print("Name: " + str(n["name"]))
    print("start: "+ str(n["start_date"]))
    print("end: "+ str(n["end_date"]))
    print("Campaign Days "+ str(reservation["days"]))

    #update campaign analysis
    sql= "INSERT INTO campaign_analysis (schedule_days, schedules, schedule_start_date, schedule_end_date,  country, campaign, name,reservation_id, start_date, end_date, saturation, duration_msec, active, days, description,total_screens_order) VALUES (%s,%s,%s,%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val= (schedule_days,num_schedules,reservation["schedule_start_date"], reservation["schedule_end_date"],country, reservation["name"],reservation["name"],reservation_id,str(n["start_date"]),str(n["end_date"]),str(n["saturation"]),n["duration_msec"],reservation["active"],reservation["days"], "Broadsign data" , "null" )
    mycursor.execute(sql,val)
    mydb.commit()

    campaign_name=reservation["name"]


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
        campaign_daily_performance["repetitions"]=n["total"]            #*******************************************************************
        campaign_daily_performance["reservation_id"]=n["reservable_id"]

        campaign_days.append(str(n["played_on"]));  #add days to array
        print(str(n["played_on"]) + " Repetitions: " + str(n["total"]) )

        if dummy_data==1:
          total_day_plays = n["total_impressions"] + DUMMY_DAY_PLAYS
        else: 
          total_day_plays = n["total_impressions"]
           

        #print campaign_daily_performance
        sql= "INSERT INTO campaign_daily_performance (country, campaign, total_impressions, played_on, repetitions, reservation_id) VALUES (%s,%s,%s,%s,%s,%s)"
        val= (country, campaign_name,total_day_plays,str(n["played_on"]),n["total"],n["reservable_id"])
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

  if dummy_data==1:
    #dummy DU
    sql= "INSERT INTO campaign_display_unit_performance (campaign, display_unit_id, container_id, screen_count, display_unit_name, mall_name, total_impressions, repetitions, reservation_id) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s)"
    val= (campaign_name, DUMMY_DISPLAY_UNIT_ID ,DUMMY_CONTAINER_ID,DUMMY_SCREEN_COUNT,DUMMY_NAME,DUMMY_MALL_NAME, 0 ,DUMMY_PLAYS ,n["reservable_id"])
    mycursor.execute(sql,val)
    mydb.commit()

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
      screen_count=row_0[2]

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
      sql_select_getmall = "SELECT id, screens, default_screen_day_impressions, default_screen_day_views, name, dwell_time, multiplicador_medio FROM malls WHERE (broadsign_container_id='%s' OR subnet1_broadsign_container_id='%s' OR subnet2_broadsign_container_id='%s' OR subnet3_broadsign_container_id='%s') AND active = 1" % (str(row_0[0]),str(row_0[0]),str(row_0[0]),str(row_0[0]))
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
              default_day_multiplier = rows_1[6]
              mall_name = rows_1[4]
              dwell_time = rows_1[5]
      else:
          print("Error ************ - no container found in AUDIENCE MALLS DB!!!!!!!!!")
          mall_id = 0
          mall_screens = 0
          default_screen_day_impression = 10
          default_screen_day_views = 5
          mall_name = "NOT FOUND"
          dwell_time = 12
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
            sql_select_total_imp= "SELECT * from audience_multipliers WHERE date LIKE '%s' AND mall_id LIKE '%s'" %(str(day_formatted), str(mall_id))
            mycursor.execute(sql_select_total_imp)
            records_day_multipliers = mycursor.fetchall()


            #print(records_day_impressions)
            try:
              day_multipliers=records_day_multipliers[0][12:25]
              print(day_multipliers)

            except:
              day_multipliers = [0] * (25-12)
            #print(day_impressions_audience)


            #hours of campaign running ( now 9-24)
            hours_range=[1,1,1,1,1,1,1,1,1,1,1,1,1]
            if mycursor.rowcount==0:
                print("No audience data for mall " + str(mall_name) + " id " + str(mall_id) + " found for " + day + ", using default multiplier value "+ str(default_day_multiplier))
                day_multipliers= default_day_multiplier
                day_impressions_audience= np.ones(15)*((day_multipliers*screen_count)*(540/15))   #numero de pantallas del display unit
            
            print("DU BS Repetitions: ", du_bs_repetitions, " total days: ", total_days)
            print("Distribución repeticiones : ", repetition_distribution)
            
            du_day_repetitions=du_bs_repetitions*repetition_distribution[day_index]
            print("Repeticiones dia: ", du_day_repetitions)

            day_multipliers = np.array(day_multipliers)
            day_repetitions = np.ones(13)*((du_day_repetitions/13))

            print(day_multipliers)
            print(day_repetitions)


            du_day_campaign_impressions= np.sum(day_multipliers*day_repetitions)
            print(du_day_campaign_impressions)


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

  

  #
  if campaign_id:

    #calculate unique users with inspide data
    print("\n")
    print("Calculating unique users")
    
    sql_select_days= "SELECT played_on from campaign_daily_performance where reservation_id='%s'" % (str(reservation_id))
    mycursor.execute(sql_select_days)
    records_days= mycursor.fetchall()


    all_visits_count = 0
    num_days = len(records_days)
    if num_days<=7: #1 semana
      duplicity_percentage = 1
    if num_days >7 and num_days <=14:
      duplicity_percentage = DUPLICIDAD_2_SEMANAS
    if num_days >14 and num_days <=21 :
      duplicity_percentage = DUPLICIDAD_3_SEMANAS
    if num_days >21 and num_days <=28 :
      duplicity_percentage = DUPLICIDAD_4_SEMANAS
    if num_days >28 and num_days<=35: 
      duplicity_percentage = DUPLICIDAD_5_SEMANAS
    if num_days >35:
      duplicity_percentage = CORRECCION_DUPLICIDAD


    print("Duplicity percentage : ", duplicity_percentage)

    for row_day in records_days:

      print("calculating unique users for day ", row_day[0] )
      campaign_day=row_day[0]
      container_id_list=[]   # evitar duplicados por varios display units 
      
      sql_select_containers= "SELECT container_id, mall_name, total_unique_visits, display_unit_id from campaign_display_unit_performance where reservation_id='%s'" % (str(reservation_id))
      mycursor.execute(sql_select_containers)
      records_containers= mycursor.fetchall()
      for row_container in records_containers:
        print("Container ",row_container[0], "  mall name ", row_container[1])
        if not  row_container[0] in  container_id_list:
          if row_container[2] is None:
            container_visits = 0
          else:
            container_visits= int(row_container[2])
          container_id_list.append(row_container[0])
          sql_select_mall= "SELECT id, name from malls where broadsign_container_id='%s'" % (str(row_container[0]))
          mycursor.execute(sql_select_mall)
          records_mall= mycursor.fetchall()
          for records_mall in records_mall:
            print("found mall id ", records_mall[0], " name ", records_mall[1])
            #get inspide visits
            sql_select_visits= "SELECT visits from inspide_data where mall_id='%s' and DATE(start_date)='%s'" % (str(records_mall[0]),row_day[0] )
            mycursor.execute(sql_select_visits)
            records_visits= mycursor.fetchall()
            daily_visits=0
            for records_visit in records_visits:
              print("Visits: ", row_day[0], " : ", records_visit[0])
              daily_visits=daily_visits + int(records_visit[0])
            #calculate average and apply IMC extrapolation 
            daily_unique = daily_visits*PONDERACION_DIARIA
            print(" unique daily:", daily_unique )
            all_visits_count = all_visits_count + daily_unique
            container_visits= container_visits + daily_unique
            if container_visits > 0: 
              sql= "UPDATE campaign_display_unit_performance SET  total_unique_visits=%s WHERE reservation_id=%s AND container_id=%s AND display_unit_id=%s"
              val= (str(int(container_visits)),str(reservation_id), str(row_container[0]), str(row_container[3]))              
              mycursor.execute(sql,val)
              mydb.commit()    
    
    sql_select_containers= "SELECT container_id, mall_name, total_unique_visits, display_unit_id from campaign_display_unit_performance where reservation_id='%s'" % (str(reservation_id))
    mycursor.execute(sql_select_containers)
    records_containers= mycursor.fetchall()
    for row_container in records_containers:
      if not row_container[2] is None: 
              sql= "UPDATE campaign_display_unit_performance SET  total_unique_visits=%s WHERE reservation_id=%s AND container_id=%s AND display_unit_id=%s"
              val= (str(int(row_container[2]*duplicity_percentage)),str(reservation_id), str(row_container[0]), str(row_container[3]))              
              mycursor.execute(sql,val)
              mydb.commit()    

    sql= "UPDATE campaign_analysis SET  total_unique_visits=%s WHERE reservation_id=%s"
    val= (str(int(all_visits_count*duplicity_percentage)),str(reservation_id))
            
    mycursor.execute(sql,val)
    mydb.commit()    


mycursor.close()

if(mydb.is_connected()):
    mycursor.close()
    print("MySQL connection is closed")