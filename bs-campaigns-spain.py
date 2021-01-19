import requests
import json
import re
import mysql.connector
import pandas as pd
import numpy as np
import logging as log
from datetime import datetime, timedelta
from datetime import date


print ("Broadsign Campaign SPAIN- POLL \n ")

#URLS -----------

url_reservation_by_display_unit= 'https://api.broadsign.com:10889/rest/reservation/v20/by_du_folder?domain_id=17244398&current_only=false';
url_container_info= 'https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_display_unit_info= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398';
url_container_scoped_colombia= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=120956285';
url_container_scoped_spain= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=49461537';
url_campaign_performance='https://api.broadsign.com:10889/rest/campaign_performance/v6/by_reservable_id?domain_id=17244398';
url_campaign_audience= 'https://api.broadsign.com:10889/rest/campaign_audience/v1/by_reservation_id?domain_id=17244398';
url_display_unit_performance="https://api.broadsign.com:10889/rest/display_unit_performance/v5/by_reservable_id?domain_id=17244398"
url_display_unit_audience= "https://api.broadsign.com:10889/rest/display_unit_audience/v1/by_reservation_id?domain_id=17244398"
url_container_id='https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_reservation_by_id= 'https://api.broadsign.com:10889/rest/reservation/v20/by_id?domain_id=17244398';


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
  passwd="sonaeRootMysql2017",
  database="audience"
)

mycursor = mydb.cursor()

#Authorization
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";


print("Extracting current month SPANISH CAMPAIGNS")

# poll container IDs
r=requests.get(url_container_scoped_spain, headers={'Accept': 'application/json','Authorization': auth});
data= json.loads(r.text)

container_ids=[]
container_name=[]
reservation={}
malls={}

#print data

for k in data["container"]:
        #print(k["name"].encode('utf-8', errors ='ignore'))
        #container_name = k["name"] + " id = " + str(k["id"])
        
        if str(k["active"]) == "True":
            container_ids.append(str(k["id"]))
            container_name.append(k["name"].encode('utf-8', errors ='ignore'))
            malls[str(k["id"])]=k["name"].encode('utf-8', errors ='ignore')

#print(malls)
#print(container_name)
#print(container_ids)

reservations=[]

container_ids=['49461537']

for m in container_ids:
        url_reservation_container=url_reservation_by_display_unit+"&container_ids=" +m
        #print(url_reservation_container)
        s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth})
        data=json.loads(s.text)

        #print data
        for n in data["reservation"]:
                insert=1
                reservation["mall_container_id"]=m
                reservation["booking_state"]=str(n["booking_state"])
                reservation["saturation"]=str(n["saturation"])
                reservation["duration_msec"]=str(n["duration_msec"])
                reservation["start_time"]=str(n["start_time"])
                reservation["start_date"]=str(n["start_date"])
                fecha_inicio=datetime.strptime(str(n["start_date"]),"%Y-%m-%d")
                fecha_fin=datetime.strptime(str(n["end_date"]),"%Y-%m-%d")
                reservation["active"]="unknown"
                if fecha_inicio < datetime.today():
                        if fecha_fin > datetime.today():
                                reservation["active"]="Running"
                if fecha_inicio>datetime.today():
                        reservation["active"]="por emitir"
                if fecha_fin<datetime.today():
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
                reservation["country"]="SPAIN"
                reservation["last_updated"]=datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
                reservation["mall"]=malls[m]
                name=n["name"].encode('utf-8', errors ='ignore')
                #if re.findall('\$(.*)\$',name):
                #    reservation["SAP_ID"]=re.findall('\$(.*)\$', name)[0]
                #else:
                reservation["SAP_ID"]="not set"

                reservations.append(reservation)
                
                reservation={}
            

df_reservations = pd.DataFrame(reservations)

#filter last month campaigns

df_reservations['start_date']=pd.to_datetime(df_reservations['start_date'], format="%Y-%m-%d")
df_reservations['end_date']=pd.to_datetime(df_reservations['end_date'], format="%Y-%m-%d")

#add date and time columns for aggregation
df_reservations=df_reservations.set_index('end_date')
df_reservations['year']=df_reservations.index.year
df_reservations['month']=df_reservations.index.month

today = pd.to_datetime("today")
current_month=today.month
current_year=today.year

#analyze only current month campaigns (based on end_date)
df_reservations = df_reservations[(df_reservations['year'] ==current_year)] 
df_reservations = df_reservations[(df_reservations['month'] ==current_month)] 

#remove all programmatic campaigns
df_reservations = df_reservations[~df_reservations['name'].str.contains("PROGRAMMATIC", na = False) ]


#print(df_reservations['campaign_id'])
print(df_reservations)



'''
Analyze campaigns 

'''
campaigns=df_reservations['campaign_id'].to_list()


campaign_daily_performance={}
campaign_daily_audience={};
campaign_display_unit_performance={};
campaign_display_unit_audience={};


for row in campaigns:  #for each campaign to analyze


       print("Analizing reservation ID  " + str(row))
       reservation_id=row


       print("deleting previous campaign results...")

       query="DELETE FROM campaign_analysis WHERE reservation_id={id}".format(
            id=row   
       )
       mycursor.execute(query)
       mydb.commit()

       query="DELETE FROM campaign_display_unit_performance WHERE reservation_id={id}".format(
            id=row   
       )
       mycursor.execute(query)
       mydb.commit()

       query="DELETE FROM campaign_daily_performance WHERE reservation_id={id}".format(
            id=row   
       )
       mycursor.execute(query)
       mydb.commit()

       query="DELETE FROM campaign_daily_audience WHERE reservation_id={id}".format(
            id=row   
       )
       mycursor.execute(query)
       mydb.commit()

       query="DELETE FROM campaign_display_unit_audience WHERE reservation_id={id}".format(
            id=row   
       )
       mycursor.execute(query)
       mydb.commit()


       campaign_name=df_reservations.loc[df_reservations['campaign_id'] == row].name[0]

       print(campaign_name)


       #***************** GET RESERVATION DATA from Broadsign ******************************
       url_reservation_by_id=url_reservation_by_id+"&ids=" +str(reservation_id);
       s=requests.get(url_reservation_by_id,headers={'Accept': 'application/json','Authorization': auth});
       data=json.loads(s.text)

       print(data)

       try:

           for n in data["reservation"]:
               reservation["name"]=str(n["name"].encode('utf-8', errors ='ignore'))
               reservation["saturation"]=str(n["saturation"])
               reservation["duration_msec"]=str(n["duration_msec"])
               reservation["start_time"]=str(n["start_time"])
               reservation["start_date"]=str(n["start_date"])
               fecha_inicio=datetime.strptime(str(n["start_date"]),"%Y-%m-%d")
               fecha_fin=datetime.strptime(str(n["end_date"]),"%Y-%m-%d")
               reservation["active"]="unknown"
               if fecha_inicio < datetime.today():
                   if fecha_fin > datetime.today():
                       reservation["active"]="Running"
               if fecha_inicio>datetime.today():
                   reservation["active"]="por emitir"
               if fecha_fin<datetime.today():
                   reservation["active"]="Emitida"
               delta=fecha_fin-fecha_inicio
               reservation["days"]=0
               reservation["days"]=delta.days+1
               reservation["end_time"]=str(n["end_time"])
               reservation["end_date"]=str(n["end_date"])

               print("")
               print("---------------------")
               print("Broadsign Campaign:  ")
               print("---------------------")

               print("Name " + str(n["name"]))
               print("start:"+ str(n["start_date"]))
               print("end:"+ str(n["end_date"]))
               print("Campaign Days "+ str(reservation["days"]))

               sql= "INSERT INTO campaign_analysis (country, campaign, name,reservation_id, start_date, end_date, saturation, duration_msec, active, days, description,total_screens_order) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
               val= ("SPAIN", campaign_name,campaign_name,reservation_id,str(n["start_date"]),str(n["end_date"]),str(n["saturation"]),n["duration_msec"],reservation["active"],reservation["days"], "Broadsign data" , "null" )
               mycursor.execute(sql,val)
               mydb.commit()




           #Performance reports:
           print ("")
           print("-------------------------------------")
           print("   Campaign Daily Performance:")
           print("-------------------------------------")
           url_campaign_performance=url_campaign_performance+"&reservable_id=" +str(reservation_id);
           s=requests.get(url_campaign_performance,headers={'Accept': 'application/json','Authorization': auth});
           data=json.loads(s.text)

           #initialize data
           campaign_days=[] #delete all days

           #analyze data
           for n in data["campaign_performance"]:

              if n["total"]>100:
                  campaign_daily_performance["total_impressions"]=n["total_impressions"]
                  campaign_daily_performance["played_on"]=str(n["played_on"])
                  campaign_daily_performance["repetitions"]=n["total"]
                  campaign_daily_performance["reservation_id"]=n["reservable_id"]

                  campaign_days.append(str(n["played_on"]));  #add days to array
                  print(str(n["played_on"]) + " Repetitions: " + str(n["total"]) )

                  #print campaign_daily_performance
                  sql= "INSERT INTO campaign_daily_performance (country, campaign, total_impressions, played_on, repetitions, reservation_id) VALUES (%s,%s,%s,%s,%s,%s)"
                  val= ("SPAIN", campaign_name, n["total_impressions"],str(n["played_on"]),n["total"],n["reservable_id"])
                  mycursor.execute(sql,val)
                  mydb.commit()



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

           for n in data["display_unit_performance"]:
               campaign_display_unit_performance["total_impressions"]=n["total_impressions"]
               campaign_display_unit_performance["repetitions"]=n["total"]
               campaign_display_unit_performance["display_unit_id"]=n["display_unit_id"]
               #get mall name from this display unit
               url_display_unit_info=url_display_unit_info+"&ids=" +str(n["display_unit_id"]);
               s=requests.get(url_display_unit_info,headers={'Accept': 'application/json','Authorization': auth});
               data_name=json.loads(s.text)
               for m in data_name["display_unit"]:
                    url_container_id=url_container_id+"&ids=" +str(m["container_id"]);
                    campaign_display_unit_performance["container_id"]=m["container_id"]
                    campaign_display_unit_performance["screen_count"]=m["host_screen_count"]
                    campaign_display_unit_performance["display_unit_name"]=str(m["name"].encode('utf-8', errors ='ignore'))

                    s=requests.get(url_container_id,headers={'Accept': 'application/json','Authorization': auth});
                    data_mall_name=json.loads(s.text)
                    #print data_mall_name
                    for o in data_mall_name["container"]:
                        campaign_display_unit_performance["mall_name"]=o["name"].encode('utf-8', errors ='ignore')
                        print("Repetitions for site "+ str(o["name"])+ " and Display unit " + str(m["name"]) + ":" +str(n["total"]) + " ( " + str(m["host_screen_count"]) +" screens)" )

               campaign_display_unit_performance["reservation_id"]=n["reservable_id"]
               campaign_display_unit_performance["repetitions"]=n["total"]

               sql= "INSERT INTO campaign_display_unit_performance (campaign, display_unit_id, container_id, screen_count, display_unit_name, mall_name, total_impressions, repetitions, reservation_id) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s)"
               val= (campaign_name, n["display_unit_id"],m["container_id"],m["host_screen_count"],str(m["name"]),o["name"].encode('utf-8', errors ='ignore'),n["total_impressions"],n["total"],n["reservable_id"])
               mycursor.execute(sql,val)
               mydb.commit()


           #print campaign_display_unit_performance
           print ("")
           print("-------------------------------------")
           print("     Display Unit Audience:          ")
           print("-------------------------------------")


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
                #calculate daily impressions for that display unit and each day
                for day in campaign_days:

                    print("")
                    date_unformatted=day.split("-")
                    day_formatted=date_unformatted[0]+"-"+date_unformatted[1]+"-"+date_unformatted[2]
                    print("Day: ",  day_formatted)

                    # buscar numero de impresiones ese dia para ese display unit
                    sql_select_total_imp= "SELECT total_impressions, total_views from audience_impressions WHERE date LIKE '%s' AND mall_id LIKE '%s'" %(str(day_formatted), str(mall_id))
                    mycursor.execute(sql_select_total_imp)
                    records_day_impressions = mycursor.fetchall()
                    day_impressions = 0
                    print("Model Impressions / Views : ", records_day_impressions)

                    #if no data use default VALUE
                    if mycursor.rowcount==0:
                        print("No audience data for mall " + str(mall_name) + " id " + str(mall_id) + " found for " + day + ", using default value "+ str(default_screen_day_impressions))
                        day_impressions= default_screen_day_impressions
                        du_day_impressions= day_impressions*row_0[2]   #numero de pantallas del display unit
                    else:
                        for rows_2 in records_day_impressions:
                            day_impressions = rows_2[0]
                            if day_impressions == 0:
                                day_impressions = 100 #un valor minimo para evitar divisiones por zero.
                            #print("Impresiones totales del mall " + str(mall_id) + " : " + str(rows_2[0]))
                            du_day_impressions=int((day_impressions/mall_screens)*row_0[2])    #ponderamos al numero de pantallas del display unit
                            #print("Impresiones totales" + day_formatted + " para display unit " + row_0[4] + " = "+ str(du_day_impressions) +" (" + str(rows_2[0]) + " totales)" )
                            #print("")

                    #agafar les concentracions
                    sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 35) #target hombre
                    mycursor.execute(sql_select_total_imp)
                    records_concentration = mycursor.fetchall()

                    #if no data use default VALUE
                    if mycursor.rowcount==0:
                        print("No man concentration available for " + str(mall_name) +" found for " + day + ", setting to 0,45 ")
                        concentration_male=0.46
                    else:
                        for rows_3 in records_concentration:
                            concentration_male = rows_3[0]
                            print("Concentracion male  " + str(mall_id) + " : " + str(rows_3[0]))

                    #agafar les concentracions
                    #print("**** concentracin mujeres*****")
                    sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 36) #target mujeres
                    mycursor.execute(sql_select_total_imp)
                    records_concentration = mycursor.fetchall()

                    #if no data use default VALUE
                    if mycursor.rowcount==0:
                        print("No woman concentration available for " + str(mall_name) + " id " + str(mall_id) +" found for " + day + ", setting to 0.55 ")
                        concentration_female=0.54

                    else:
                        for rows_3 in records_concentration:
                            concentration_female = rows_3[0]
                            print("Concentracion female  " + str(mall_id) + " : " + str(rows_3[0]))


                    #agafar les concentracions
                    #print("**** concentracin child*****")
                    sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 24) #target hombre
                    mycursor.execute(sql_select_total_imp)
                    records_concentration = mycursor.fetchall()

                    #if no data use default VALUE
                    if mycursor.rowcount==0:
                        print("No child concentration available for " + str(mall_name) + " id " + str(mall_id) +" found for " + day + ", setting to 0.05 ")
                        concentration_child=0.05

                    else:
                        for rows_3 in records_concentration:
                            concentration_child = rows_3[0]
                            print("Concentracion kid  " + str(mall_id) + " : " + str(rows_3[0]))



                    #agafar les concentracions
                    #print("**** concentracin young*****")
                    sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 25) #target hombre
                    mycursor.execute(sql_select_total_imp)
                    records_concentration = mycursor.fetchall()

                    #if no data use default VALUE
                    if mycursor.rowcount==0:
                        print("No young concentration available for " + str(mall_name) + " id " + str(mall_id) +" found for " + day + ", setting to 0.4 ")
                        concentration_young=0.39

                    else:
                        for rows_3 in records_concentration:
                            concentration_young = rows_3[0]
                            print("Concentracion young " + str(mall_id) + " : " + str(rows_3[0]))


                    #agafar les concentracions
                    #print("**** concentracin adult*****")
                    sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 26) #target hombre
                    mycursor.execute(sql_select_total_imp)
                    records_concentration = mycursor.fetchall()

                    #if no data use default VALUE
                    if mycursor.rowcount==0:
                        print("No adult concentration available for " + str(mall_name) + " id " + str(mall_id) + " found for " + day + ", setting to 0 .4")
                        concentration_adult=0.41

                    else:
                        for rows_3 in records_concentration:
                            concentration_adult = rows_3[0]
                            print("Concentracion adult  " + str(mall_id) + " : " + str(rows_3[0]))

                    #agafar les concentracions
                    #print("**** concentracion senior*****")
                    sql_select_total_imp= "SELECT average_concentration FROM audience_segments WHERE datetime LIKE '%s' AND mall_id LIKE '%s' AND target_id='%s'" %(str(day_formatted), str(mall_id), 27) #target hombre
                    mycursor.execute(sql_select_total_imp)
                    records_concentration = mycursor.fetchall()

                    #if no data use default VALUE
                    if mycursor.rowcount==0:
                        print("No senior concentration available for " + str(mall_name) + " id " + str(mall_id) + " found for " + day + ", setting to 0.15 ")
                        concentration_senior=0.15

                    else:
                        for rows_3 in records_concentration:
                            concentration_senior = rows_3[0]
                            print("Concentracion senior " + str(mall_id) + " : " + str(rows_3[0]))


                    #total_mall_impressions= total_mall_impressions + day_impressions



                    #numero de impresiones para pantallas del display unit
                    du_day_repetitions=row_0[1]/total_days  #numero de repeticiones para ese dia, dividimos total entre numero de dias de la campana
                    #calculo del factor corrector de impresiones en base a las repeticiones de ese dia
                    screen_day_reps = du_day_repetitions/row_0[2]
                    factor_corrector=0.0003*screen_day_reps+0.0004
                    print("Campaign_repetitions for that day por pantalla: "+ str(screen_day_reps) + " factor: "+ str(factor_corrector))

                    #corregimos las impressiones para esa campaign
                    #*********************************************
                    du_day_campaign_impressions=round(du_day_impressions*factor_corrector)

                    daily_totals_2.append(du_day_campaign_impressions)

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
                    print("Impresiones ponderadas para dia " + day_formatted +": "+ str(du_day_campaign_impressions)+ " de un total de " +str(day_impressions) +" diarias del mall y " + str(du_day_impressions) + " del display unit en mall " + str(mall_id) )

                    total_du_campaign_impressions=total_du_campaign_impressions+du_day_campaign_impressions

                    total_con_male=total_con_male + round(du_day_campaign_impressions*concentration_male)
                    total_con_female=total_con_female + round(du_day_campaign_impressions*concentration_female)
                    total_con_child=total_con_child + round(du_day_campaign_impressions*concentration_child)
                    total_con_young=total_con_young + round(du_day_campaign_impressions*concentration_young)
                    total_con_adult=total_con_adult + round(du_day_campaign_impressions*concentration_adult)
                    total_con_senior=total_con_senior + round(du_day_campaign_impressions*concentration_senior)

                    f_child=round(total_con_child*total_con_female/(total_con_male+total_con_female))
                    f_young=round(total_con_young*total_con_female/(total_con_male+total_con_female))
                    f_adult=round(total_con_adult*total_con_female/(total_con_male+total_con_female))
                    f_senior=round(total_con_senior*total_con_female/(total_con_male+total_con_female))
                    m_child=round(total_con_child*total_con_male/(total_con_male+total_con_female))
                    m_young=round(total_con_young*total_con_male/(total_con_male+total_con_female))
                    m_adult=round(total_con_adult*total_con_male/(total_con_male+total_con_female))
                    m_senior=round(total_con_senior*total_con_male/(total_con_male+total_con_female))


                #update impressions in display unit performance
                sql= "UPDATE campaign_display_unit_performance  SET  total_impressions=%s WHERE display_unit_id=%s AND campaign=%s"
                val= (total_du_campaign_impressions,row_0[3],campaign_name)
                mycursor.execute(sql,val)
                mydb.commit()
                daily_totals.append(daily_totals_2)

                #update audience for display unit
                sql= "INSERT INTO campaign_display_unit_audience (country, campaign, display_unit_id, container_id, screen_count, display_unit_name, mall_name, female_child, female_young,female_adult,female_senior,female_unknown,male_child,male_young,male_adult,male_senior,male_unknown,unknown_child,unknown_young,unknown_adult,unknown_senior, unknown_unknown, reservation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val= ("SPAIN", campaign_name,row_0[3],row_0[0],row_0[2], str(row_0[4].encode('utf-8', errors ='ignore')),str(row_0[5].encode('utf-8', errors ='ignore')),f_child,f_young,f_adult,f_senior,0,m_child,m_young,m_adult,m_senior,0,0,0,0,0,0,reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()

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


           t_matrix= [[daily_totals[j][i] for j in range(len(daily_totals))] for i in range(len(daily_totals[0]))]

           print("")
           print("Daily performance")
           print("-----------------")
           for index,i in enumerate(t_matrix):
                print(i)
                day_total=0
                for j in i:
                    day_total=day_total+j
                #print campaign_daily_performance
                sql= "UPDATE campaign_daily_performance SET total_impressions=%s WHERE played_on=%s AND reservation_id=%s"
                val= (day_total,campaign_days[index],str(reservation_id))
                mycursor.execute(sql,val)
                mydb.commit()
                sql= "INSERT INTO campaign_daily_audience (campaign, played_on, reservation_id) VALUES (%s, %s, %s) "
                val= (campaign_name, campaign_days[index],str(reservation_id))
                mycursor.execute(sql,val)
                mydb.commit()


           #matrius de concentraciones
           print("")
           print("Daily audience")
           print("-----------------")

           t_matrix= [[con_m_child[j][i] for j in range(len(con_m_child))] for i in range(len(con_m_child[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET male_child=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index], str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()




           t_matrix= [[con_m_young[j][i] for j in range(len(con_m_young))] for i in range(len(con_m_young[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET male_young=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index], str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()


           t_matrix= [[con_m_senior[j][i] for j in range(len(con_m_senior))] for i in range(len(con_m_senior[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET male_senior=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index], str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()



           t_matrix= [[con_m_adult[j][i] for j in range(len(con_m_adult))] for i in range(len(con_m_adult[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET male_adult=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index],str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()


           t_matrix= [[con_f_child[j][i] for j in range(len(con_f_child))] for i in range(len(con_f_child[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET female_child=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index],str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()


           t_matrix= [[con_f_young[j][i] for j in range(len(con_f_young))] for i in range(len(con_f_young[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET female_young=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index],str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()


           t_matrix= [[con_f_senior[j][i] for j in range(len(con_f_senior))] for i in range(len(con_f_senior[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET female_senior=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index],str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()


           t_matrix= [[con_f_adult[j][i] for j in range(len(con_f_adult))] for i in range(len(con_f_adult[0]))]
           for index,i in enumerate(t_matrix):
                  #print i
                  day_total=0
                  for j in i:
                      day_total=day_total+j
                  #print campaign_daily_performance
                  sql= "UPDATE campaign_daily_audience SET female_adult=%s WHERE played_on=%s AND reservation_id=%s"
                  val= (day_total,campaign_days[index],str(reservation_id))
                  mycursor.execute(sql,val)
                  mydb.commit()



           url_display_unit_audience=url_display_unit_audience+"&reservation_id=" +str(reservation_id)
           print(url_display_unit_audience)
           s=requests.get(url_display_unit_audience,headers={'Accept': 'application/json','Authorization': auth});
           data=json.loads(s.text)

           print("Audience display unit data in broadsign "+ str(len(data["display_unit_audience"])))

           for n in data["display_unit_audience"]:
               campaign_display_unit_audience["female_adult"]=n["female_adult"]
               campaign_display_unit_audience["female_senior"]=n["female_senior"]
               campaign_display_unit_audience["female_unknown"]=n["female_unknown"]
               campaign_display_unit_audience["female_child"]=n["female_child"]
               campaign_display_unit_audience["female_young"]=n["female_young"]
               campaign_display_unit_audience["male_adult"]=n["male_adult"]
               campaign_display_unit_audience["male_senior"]=n["male_senior"]
               campaign_display_unit_audience["male_unknown"]=n["male_unknown"]
               campaign_display_unit_audience["male_young"]=n["male_young"]
               campaign_display_unit_audience["unknown_child"]=n["unknown_child"]
               campaign_display_unit_audience["unknown_senior"]=n["unknown_senior"]
               campaign_display_unit_audience["unknown_young"]=n["unknown_young"]
               campaign_display_unit_audience["unknown_adult"]=n["unknown_adult"]
               campaign_display_unit_audience["unknown_unknown"]=n["unknown_unknown"]
               campaign_display_unit_audience["reservation_id"]=n["reservation_id"]
               campaign_display_unit_audience["display_unit_id"]=n["display_unit_id"]
               url_display_unit_info=url_display_unit_info+"&ids=" +str(n["display_unit_id"]);
               s=requests.get(url_display_unit_info,headers={'Accept': 'application/json','Authorization': auth});
               data_name=json.loads(s.text)
               #print "***********"
               #print data_name
               for m in data_name["display_unit"]:
                    url_container_id=url_container_id+"&ids=" +str(m["container_id"]);
                    campaign_display_unit_audience["container_id"]=m["container_id"]
                    campaign_display_unit_audience["screen_count"]=m["host_screen_count"]
                    campaign_display_unit_audience["display_unit_name"]=str(m["name"])

                    s=requests.get(url_container_id,headers={'Accept': 'application/json','Authorization': auth});
                    data_mall_name=json.loads(s.text)
                    #print "******* *******"
                    #print data_mall_name
                    for o in data_mall_name["container"]:
                        campaign_display_unit_audience["mall_name"]=o["name"]

               sql= "INSERT INTO campaign_display_unit_audience (campaign, display_unit_id, container_id, screen_count, display_unit_name, mall_name, female_child, female_young,female_adult,female_senior,female_unknown,male_child,male_young,male_adult,male_senior,male_unknown,unknown_child,unknown_young,unknown_adult,unknown_senior, unknown_unknown, reservation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
               val= (campaign_name, n["display_unit_id"],m["container_id"],m["host_screen_count"],str(m["name"]),o["name"],n["female_child"],n["female_young"],n["female_adult"],n["female_senior"],n["female_unknown"],n["male_child"],n["male_young"],n["male_adult"],n["male_senior"],n["male_unknown"],n["unknown_child"],n["unknown_young"],n["unknown_adult"],n["unknown_senior"],n["unknown_unknown"],n["reservation_id"])
               mycursor.execute(sql,val)
               mydb.commit()


               print(campaign_display_unit_audience)

       except:
               print("Error analyzing campaign")
               print("deleting previous campaign results...")

               query="DELETE FROM campaign_analysis WHERE reservation_id={id}".format(
                    id=row   
               )
               mycursor.execute(query)
               mydb.commit()

               query="DELETE FROM campaign_display_unit_performance WHERE reservation_id={id}".format(
                    id=row   
               )
               mycursor.execute(query)
               mydb.commit()

               query="DELETE FROM campaign_daily_performance WHERE reservation_id={id}".format(
                    id=row   
               )
               mycursor.execute(query)
               mydb.commit()

               query="DELETE FROM campaign_daily_audience WHERE reservation_id={id}".format(
                    id=row   
               )
               mycursor.execute(query)
               mydb.commit()

               query="DELETE FROM campaign_display_unit_audience WHERE reservation_id={id}".format(
                    id=row   
               )
               mycursor.execute(query)
               mydb.commit()



mycursor.close()

if(mydb.is_connected()):
    mycursor.close()
    print("MySQL connection is closed")
