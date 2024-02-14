#ADMOBILIZE IMPORT CAMERA DATA

import requests
import json
import csv
import re
import sys
import ssl
import mysql.connector
from datetime import datetime
from datetime import date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import pandas as pd
import json
from sqlalchemy import create_engine



#URLS endpoints----------
url_bms_api= 'https://sauteriberica.cloud/VisionCenterApiService/api/DataObject?options.objectId='


#database connector
mydb = mysql.connector.connect(
  host="ec2-52-18-248-109.eu-west-1.compute.amazonaws.com",
  user="root",
  passwd="SonaeRootMysql2021!",
  database="audience"
)

mycursor = mydb.cursor()

engine = create_engine("mysql+mysqlconnector://{user}:{pw}@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/{db}"
                       .format(user="root",
                               pw="SonaeRootMysql2021!",
                               db="audience"))



jobs=[]

# Obtener la fecha actual
fecha_actual = datetime.now()


# Calcular la fecha de ayer
fecha_ayer = fecha_actual - timedelta(days=1)

#fecha_week_ago = fecha_actual - timedelta(days=7)

# Formatear el inicio y el final de la fecha de ayer
inicio_ayer = fecha_ayer.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S-00:00")
#inicio_ayer = fecha_week_ago.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S-00:00")
fin_ayer = fecha_ayer.replace(hour=23, minute=59, second=59, microsecond=999999).strftime("%Y-%m-%dT%H:%M:%S-00:00")


print ("******************************************************")
print ("******************************************************")
print ("******************************************************")
print ("            ADMOBILIZE CONNECT                           ")
print ("******************************************************")
print ("******************************************************")
print ("******************************************************")
print ("")

print("Logging in Spain data")

url = 'https://auth.admobilize.com/v2/accounts/-/sessions'
payload = open("admobilize_credentials_spain.json")
headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
r = requests.post(url, data=payload, headers=headers)
data=json.loads(r.text)


access_token= data['accessToken']
print("Access token:", access_token)

#get device status 
url_device_status="https://devicemanagementapi.admobilize.com/v2/projects/P856a08a042a4e12bd8e7e1e2e9369a1/devices"
auth = "Bearer "+ access_token
s=requests.get(url_device_status,headers={'Accept': 'application/json','Authorization': auth});
data=json.loads(s.text)
#print(data)

device_info={}

for device in data['devices']:

    device_info={}
    jobs=[]
    device_info['name']= device['displayName']
    device_info['status']= device['state']['status']
    device_info['platform']= device['state']['platform']
    device_info['updateTime']= device['state']['updateTime']
    device_info['id']=device['id']

    #update DB
    print("updating sensor status")

    try:
        sql= "UPDATE admobilize_sensors SET  admobilize_deviceName=%s, admobilize_status=%s, admobilize_platform=%s, admobilize_updateTime=%s  WHERE deviceId=%s"
        val= (device_info['name'],device_info['status'], device_info['platform'],device_info['updateTime'], device_info['id'] )
        
        mycursor.execute(sql,val)
        mydb.commit()   
    except: 
        print("Error updating sensor status in DB device ",  device_info['name'] )   


print ("******************************************************")
print("Logging in Peru data")
print ("******************************************************")
jobs=[]
url = 'https://auth.admobilize.com/v2/accounts/-/sessions'
payload = open("admobilize_credentials_peru.json")
headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
r = requests.post(url, data=payload, headers=headers)
data=json.loads(r.text)


access_token= data['accessToken']
print("Access token:", access_token)

#get device status 
url_device_status="https://devicemanagementapi.admobilize.com/v2/projects/P8ddc58d54ed4ee1b65c2936035a3bf8/devices"
auth = "Bearer "+ access_token
s=requests.get(url_device_status,headers={'Accept': 'application/json','Authorization': auth});
data=json.loads(s.text)
#print(data)

device_info={}

for device in data['devices']:

    device_info={}
    jobs=[]
    device_info['name']= device['displayName']
    device_info['status']= device['state']['status']
    device_info['platform']= device['state']['platform']
    device_info['updateTime']= device['state']['updateTime']
    device_info['id']=device['id']

    #update DB
    print("updating sensor status")

    try:
        sql= "UPDATE admobilize_sensors SET  admobilize_deviceName=%s, admobilize_status=%s, admobilize_platform=%s, admobilize_updateTime=%s  WHERE deviceId=%s"
        val= (device_info['name'],device_info['status'], device_info['platform'],device_info['updateTime'], device_info['id'] )
        
        mycursor.execute(sql,val)
        mydb.commit()   
    except: 
        print("Error updating sensor status in DB device ",  device_info['name'] )    






print ("******************************************************")
print(" Logging in Colombia data")
print ("******************************************************")

jobs=[]
url = 'https://auth.admobilize.com/v2/accounts/-/sessions'
payload = open("admobilize_credentials_colombia.json")
headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
r = requests.post(url, data=payload, headers=headers)
data=json.loads(r.text)


access_token= data['accessToken']
print("Access token:", access_token)

#get device status 
url_device_status="https://devicemanagementapi.admobilize.com/v2/projects/P9af8598495440689416402ec87cc403/devices"
auth = "Bearer "+ access_token
s=requests.get(url_device_status,headers={'Accept': 'application/json','Authorization': auth});
data=json.loads(s.text)
print(data)

device_info={}

for device in data['devices']:

    device_info={}
    jobs=[]
    device_info['name']= device['displayName']
    device_info['status']= device['state']['status']
    device_info['platform']= device['state']['platform']
    device_info['updateTime']= device['state']['updateTime']
    device_info['id']=device['id']

    #update DB
    print("updating sensor status")

    try:
        sql= "UPDATE admobilize_sensors SET  admobilize_deviceName=%s, admobilize_status=%s, admobilize_platform=%s, admobilize_updateTime=%s  WHERE deviceId=%s"
        val= (device_info['name'],device_info['status'], device_info['platform'],device_info['updateTime'], device_info['id'] )
        
        mycursor.execute(sql,val)
        mydb.commit()   
    except: 
        print("Error updating sensor status in DB device ",  device_info['name'] )    



mycursor.close()

if(mydb.is_connected()):
    mycursor.close()
    print("MySQL connection is closed")