import requests
import json
import re
import mysql.connector
from datetime import datetime
from datetime import date
import sys
import pandas as pd
from sqlalchemy import create_engine
import pymysql


#fechas de análisis de campañas
start_date= "2023-01-01"
end_date= "2024-12-31"

excluded_containers=['49461537', '21003047', '106135296','553165291', '553165135', '553165137']

#*****************************************
# Argumento PAIS
campaigns_to_analyze = sys.argv[1]

print ("Broadsign Campaign- POLL \n ")

#URLS -----------
url_reservation_by_display_unit= 'https://api.broadsign.com:10889/rest/reservation/v20/by_du_folder?domain_id=17244398&current_only=false&start_date='+start_date+'&end_date='+end_date;
url_container_info= 'https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_display_unit_info= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398';
url_container_scoped_peru= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=60141576';
url_container_scoped_spain= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=106135296';
url_container_scoped_colombia= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=120956285';
url_container_scoped_led_spain= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=21003047';
url_schedule_by_reservation= 'https://api.broadsign.com:10889/rest/schedule/v8/by_reservable?domain_id=17244398'


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

mycursor.execute("DELETE FROM broadsign_reservations")
mycursor.execute("DELETE FROM occupancy")

#Authorization
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";


#reset data
container_ids=[]
container_name=[]
reservation={}
malls={}
reservations=[]

if campaigns_to_analyze=="SPAIN":

    print("SPAIN CAMPAIGNS *******")
    mycursor.execute("DELETE FROM reservations WHERE country='SPAIN'")

    # poll container IDs
    r=requests.get(url_container_scoped_spain, headers={'Accept': 'application/json','Authorization': auth});
    data= json.loads(r.text)

    #print data
    for k in data["container"]:
            print("container name = ", k["name"].encode('utf-8', errors ='ignore') , " id = " , str(k["id"]))
            if str(k["active"]) == "True":
                    container_ids.append(str(k["id"]))
                    container_name.append(k["name"].encode('utf-8', errors ='ignore'))
                    malls[str(k["id"])]=k["name"].encode('utf-8', errors ='ignore')

    #print container_ids
    #print container_name

    print("\n")
    name=""

   
    for idx, m in enumerate(container_ids):

            if m in excluded_containers:
                print("Excluded container: ", container_name[idx])
                print("\n")
                continue

            url_reservation_container=url_reservation_by_display_unit+"&container_ids=" +m;
            print("Analyzing mall reservations from: ",  container_name[idx]);
            s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth});
            data=json.loads(s.text)

            reservations=[]

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
                    reservation["name"]=n["name"].encode('utf-8', errors ='ignore')
                    reservation["country"]="SPAIN"
                    #reservation["last_updated"]=datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
                    reservation["mall"]=malls[m]
                    name=n["name"]
                    if re.findall('\$(.*)\$',name):
                            reservation["SAP_ID"]=re.findall('\$(.*)\$', name)[0]
                    else:
                            reservation["SAP_ID"]="not found"
                    schedule_start_date=""
                    schedule_end_date=""
                    schedule_days=0

                    url_schedule=url_schedule_by_reservation+"&id="+str(n["id"])
                    s=requests.get(url_schedule,headers={'Accept': 'application/json','Authorization': auth});
                    data_schedules=json.loads(s.text)
                    num_schedules=0

                    #print("*")
                    for o in data_schedules["schedule"]:
                        if o["active"] == True:
                            num_schedules=num_schedules +1
                            schedule_fecha_inicio=datetime.strptime(str(o["start_date"]),"%Y-%m-%d")
                            schedule_fecha_fin=datetime.strptime(str(o["end_date"]),"%Y-%m-%d")

                            if schedule_start_date=="":
                                schedule_start_date=schedule_fecha_inicio
                            if schedule_end_date=="": 
                                schedule_end_date=schedule_fecha_fin
                            if schedule_fecha_inicio<=schedule_start_date:
                                schedule_start_date=schedule_fecha_inicio
                            if schedule_fecha_fin>=schedule_end_date:
                                schedule_end_date=schedule_fecha_fin

                    #print(schedule_start_date)
                    #print(schedule_end_date)

                    if num_schedules>0:

                        reservation["schedule_end_date"]=str(schedule_end_date)       
                        reservation["schedule_start_date"]=str(schedule_start_date)
                        delta=schedule_end_date-schedule_start_date
                        schedule_days=delta.days+1

                    else:

                        schedule_days=0
                        reservation["schedule_end_date"]=None
                        reservation["schedule_start_date"]=None

                    reservation["schedule_days"]=schedule_days
                    reservation["num_schedules"]=num_schedules

                    
                    #print(reservation["schedule_start_date"])
                    #print(reservation["schedule_end_date"])
                    #print("---")


                    sql= "INSERT INTO broadsign_reservations ( schedule_days, num_schedules, schedule_start_date, schedule_end_date, name, booking_state, mall_container_id, duration_msec, saturation, start_time,start_date, end_time, end_date, state, country, active, mall, campaign_id,SAP_ID,days) VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val= (reservation["schedule_days"],reservation["num_schedules"],reservation["schedule_start_date"], reservation["schedule_end_date"],  reservation["name"],reservation["booking_state"],reservation["mall_container_id"],reservation["duration_msec"],reservation["saturation"],reservation["start_time"],reservation["start_date"],reservation["end_time"],reservation["end_date"],reservation["state"],reservation["country"],reservation["active"],reservation["mall"],reservation["campaign_id"],reservation["SAP_ID"],reservation["days"])
                    mycursor.execute(sql,val)
                    mydb.commit()
                    reservations.append(reservation)
                    reservation={}
            
            df_reservations= pd.DataFrame(reservations)
            print(df_reservations)

            print(list(df_reservations))

            #df_reservations = df_reservations.drop('column_name', axis=1)
            #df_reservations.to_sql('broadsign_reservations', engine, if_exists='append', index=False)

            print("\n")
            







