import requests
import json
import re
import mysql.connector
from datetime import datetime
from datetime import date
import sys



#*****************************************
#get argument
campaigns_to_analyze = sys.argv[1]


print ("Broadsign Campaign- POLL \n ")

#URLS -----------

url_reservation_by_display_unit= 'https://api.broadsign.com:10889/rest/reservation/v20/by_du_folder?domain_id=17244398&current_only=false';
url_container_info= 'https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_display_unit_info= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398';

url_container_scoped_peru= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=60141576';
url_container_scoped_spain= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=49461537';
url_container_scoped_colombia= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=120956285';

url_container_scoped_led_spain= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=21003047';


url_schedule_by_reservation= 'https://api.broadsign.com:10889/rest/schedule/v8/by_reservable?domain_id=17244398'


container_ids=[]
container_name=[]
reservation={}
malls={}


#database connector
mydb = mysql.connector.connect(
  host="ec2-52-18-248-109.eu-west-1.compute.amazonaws.com",
  user="root",
  passwd="sonaeRootMysql2017",
  database="audience"
)

mycursor = mydb.cursor()

#Authorization
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";



if campaigns_to_analyze=="peru":

    print("PERU CAMPAIGNS \n")

    # poll container IDs
    r=requests.get(url_container_scoped_peru, headers={'Accept': 'application/json','Authorization': auth});
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
    name=""


    today=datetime.now()


    url_reservation_container=""


    mycursor.execute("DELETE FROM reservations WHERE country='PERU'")
    #mycursor.execute("DELETE FROM reservations")

    sql_insert=""


    for m in container_ids:
        url_reservation_container=url_reservation_by_display_unit+"&container_ids=" +m;
        print(url_reservation_container);
        s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth});
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
                reservation["name"]=n["name"].encode('utf-8', errors ='ignore')
                reservation["country"]="PERU"
                reservation["last_updated"]=datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
                reservation["mall"]=malls[m]
                name=n["name"]
                
                if re.findall('\%(.*)\%',name):
                    reservation["SAP_ID"]=re.findall('\%(.*)\%', name)[0]
                else:
                    reservation["SAP_ID"]="not found"
                
                schedule_start_date=""
                schedule_end_date=""
                schedule_days=0

                url_schedule=url_schedule_by_reservation+"&id="+str(n["id"])
                s=requests.get(url_schedule,headers={'Accept': 'application/json','Authorization': auth});
                data_schedules=json.loads(s.text)
                num_schedules=0

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
                    #print(o["name"])
                    #print(o["active"])

                if num_schedules>0:

                        reservation["schedule_end_date"]=str(schedule_end_date)       
                        reservation["schedule_start_date"]=str(schedule_start_date)
                        delta=schedule_end_date-schedule_start_date
                        schedule_days=delta.days+1

                else:

                        schedule_days=0
                        reservation["schedule_end_date"]=None
                        reservation["schedule_start_date"]=None

                


                sql= "INSERT INTO reservations (schedule_days, schedules, schedule_start_date, schedule_end_date, name, booking_state, container_id, duration, saturation, start_time,start_date, end_time, end_date, state, country, active, mall, campaign_id,SAP_ID,days) VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val= (schedule_days,num_schedules,reservation["schedule_start_date"], reservation["schedule_end_date"],  reservation["name"],reservation["booking_state"],reservation["mall_container_id"],reservation["duration_msec"],reservation["saturation"],reservation["start_time"],reservation["start_date"],reservation["end_time"],reservation["end_date"],reservation["state"],reservation["country"],reservation["active"],reservation["mall"],reservation["campaign_id"],reservation["SAP_ID"],reservation["days"])
                mycursor.execute(sql,val)
                mydb.commit()
                reservation={}

        print("\n")



container_ids=[]
container_name=[]
reservation={}
malls={}



if campaigns_to_analyze=="colombia":

    print("COLOMBIA CAMPAIGNS")


    mycursor.execute("DELETE FROM reservations WHERE country='COLOMBIA'")

    # poll container IDs
    r=requests.get(url_container_scoped_colombia, headers={'Accept': 'application/json','Authorization': auth});
    data= json.loads(r.text)



 


    #print data

    for k in data["container"]:
            print("container name = ", k["name"].encode('utf-8', errors ='ignore') , " id = " , str(k["id"]))
            if str(k["active"]) == "True":
                    container_ids.append(str(k["id"]))
                    container_name.append(k["name"].encode('utf-8', errors ='ignore'))
                    malls[str(k["id"])]=k["name"].encode('utf-8', errors ='ignore')



    for m in container_ids:
            url_reservation_container=url_reservation_by_display_unit+"&container_ids=" +m;
            print(url_reservation_container);
            s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth});
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
                    reservation["name"]=n["name"].encode('utf-8', errors ='ignore')
                    reservation["country"]="COLOMBIA"
                    reservation["last_updated"]=datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
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

                    
                    if num_schedules>0:

                        reservation["schedule_end_date"]=str(schedule_end_date)       
                        reservation["schedule_start_date"]=str(schedule_start_date)
                        delta=schedule_end_date-schedule_start_date
                        schedule_days=delta.days+1

                    else:

                        schedule_days=0
                        reservation["schedule_end_date"]=None
                        reservation["schedule_start_date"]=None

                    #print(reservation["schedule_start_date"])
                    #print(reservation["schedule_end_date"])
                    #print("---")


                    sql= "INSERT INTO reservations (schedule_days, schedules, schedule_start_date, schedule_end_date, name, booking_state, container_id, duration, saturation, start_time,start_date, end_time, end_date, state, country, active, mall, campaign_id,SAP_ID,days) VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val= (schedule_days,num_schedules,reservation["schedule_start_date"], reservation["schedule_end_date"],  reservation["name"],reservation["booking_state"],reservation["mall_container_id"],reservation["duration_msec"],reservation["saturation"],reservation["start_time"],reservation["start_date"],reservation["end_time"],reservation["end_date"],reservation["state"],reservation["country"],reservation["active"],reservation["mall"],reservation["campaign_id"],reservation["SAP_ID"],reservation["days"])
                    mycursor.execute(sql,val)
                    mydb.commit()
                    reservation={}
            print("\n")





container_ids=[]
container_name=[]
reservation={}
malls={}



if campaigns_to_analyze=="spain":



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
    name=""






    for m in container_ids:
            url_reservation_container=url_reservation_by_display_unit+"&container_ids=" +m;
            print(url_reservation_container);
            s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth});
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
                    reservation["name"]=n["name"].encode('utf-8', errors ='ignore')
                    reservation["country"]="SPAIN"
                    reservation["last_updated"]=datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
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

                    #print(reservation["schedule_start_date"])
                    #print(reservation["schedule_end_date"])
                    #print("---")


                    sql= "INSERT INTO reservations (schedule_days, schedules, schedule_start_date, schedule_end_date, name, booking_state, container_id, duration, saturation, start_time,start_date, end_time, end_date, state, country, active, mall, campaign_id,SAP_ID,days) VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val= (schedule_days,num_schedules,reservation["schedule_start_date"], reservation["schedule_end_date"],  reservation["name"],reservation["booking_state"],reservation["mall_container_id"],reservation["duration_msec"],reservation["saturation"],reservation["start_time"],reservation["start_date"],reservation["end_time"],reservation["end_date"],reservation["state"],reservation["country"],reservation["active"],reservation["mall"],reservation["campaign_id"],reservation["SAP_ID"],reservation["days"])
                    mycursor.execute(sql,val)
                    mydb.commit()
                    reservation={}

            print("\n")






    container_ids=[]
    container_name=[]
    reservation={}
    malls={}






    # poll container IDs
    r=requests.get(url_container_scoped_led_spain, headers={'Accept': 'application/json','Authorization': auth});
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
    name=""


    for m in container_ids:
            url_reservation_container=url_reservation_by_display_unit+"&container_ids=" +m;
            print(url_reservation_container);
            s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth});
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
                    reservation["name"]=n["name"].encode('utf-8', errors ='ignore')
                    reservation["country"]="SPAIN"
                    reservation["last_updated"]=datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
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

                    #print(reservation["schedule_start_date"])
                    #print(reservation["schedule_end_date"])
                    #print("---")


                    sql= "INSERT INTO reservations (schedule_days, schedules, schedule_start_date, schedule_end_date, name, booking_state, container_id, duration, saturation, start_time,start_date, end_time, end_date, state, country, active, mall, campaign_id,SAP_ID,days) VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val= (schedule_days,num_schedules,reservation["schedule_start_date"], reservation["schedule_end_date"],  reservation["name"],reservation["booking_state"],reservation["mall_container_id"],reservation["duration_msec"],reservation["saturation"],reservation["start_time"],reservation["start_date"],reservation["end_time"],reservation["end_date"],reservation["state"],reservation["country"],reservation["active"],reservation["mall"],reservation["campaign_id"],reservation["SAP_ID"],reservation["days"])
                    mycursor.execute(sql,val)
                    mydb.commit()
                    reservation={}
            print("\n")
