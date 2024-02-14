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


    print("Getting data from device " + str(device_info['id']) )
    data_body= {}
    data_body['startTime']=inicio_ayer
    data_body['endTime']=fin_ayer
    data_body['productId']= "facev2"
    data_body['timezone']= "Europe/Madrid"
    data_body['deviceIds']= [device_info['id']]
    
    data_b= json.dumps(data_body)

    url_device_data="https://datagatewayapi.admobilize.com/v1alpha1/jobs"
    auth = "Bearer "+ access_token
    headers = {'content-type': 'application/json', 'Authorization': auth}
    r = requests.post(url_device_data, data=data_b, headers=headers)
    print(r.text)

    try:
        data=json.loads(r.text)
        jobs.append(data)

    except:
        print("Error getting data")

    for job in jobs: 
        while job['status']=="RUNNING":
            time.sleep(5)
            #check status again
            s=requests.get("https://datagatewayapi.admobilize.com/v1alpha1/jobs/" + job['jobId'],headers={'Accept': 'application/json','Authorization': auth});
            data=json.loads(s.text)
            #print(data)
            job['status']=data['status']
        if job['status']=="DONE":
            print("Job ", job['jobId'], " terminado")
            s=requests.get(" https://datagatewayapi.admobilize.com/v1alpha1/jobs/"+ job['jobId']+"/results",headers={'Accept': 'application/json','Authorization': auth});
            data=json.loads(s.text)
            #print(data)
            # Convertir la cadena JSON en una lista de diccionarios
            datapoints = json.loads(data['datapoints'])
            next_page_token= data['nextPageToken']
            
            print("Analyzing data from " + str(device_info['id']) )
            # Crear un DataFrame de pandas con esa lista
            df_datapoints = pd.DataFrame(datapoints)
            #print(df_datapoints.columns)
            print("datapoints: " + str(df_datapoints.shape[0]))
            print("Next page token: " + next_page_token)

            if len(df_datapoints)>0:


                while next_page_token !="":
                    #load next page
                    print("loading more data")
                    s=requests.get(" https://datagatewayapi.admobilize.com/v1alpha1/jobs/"+ job['jobId']+"/results?pageToken="+next_page_token,headers={'Accept': 'application/json','Authorization': auth});
                    data=json.loads(s.text)
                    # Convertir la cadena JSON en una lista de diccionarios
                    datapoints = json.loads(data['datapoints'])
                    df_datapoints_new = pd.DataFrame(datapoints)
                    # Añadir df_nuevo a df_original
                    df_datapoints = pd.concat([df_datapoints, df_datapoints_new], ignore_index=True)
                    next_page_token= data['nextPageToken']
                    

                print("datapoints: " + str(df_datapoints.shape[0]))
                print("Next page token: " + next_page_token)

                print(df_datapoints.head())

                # Convertir la columna "timestamp" a datetime
                df_datapoints['timestamp'] = pd.to_datetime(df_datapoints['timestamp'])
            
                # Crear una nueva columna con la fecha y la hora (sin minutos ni segundos para agrupar por hora exacta)
                df_datapoints['fecha_hora'] = df_datapoints['timestamp'].dt.floor('H')

                # Agrupar los datos por la nueva columna "fecha_hora"
                df_aggregated_data = df_datapoints.groupby('fecha_hora').agg(
                    impresiones=('isImpression', 'sum'),
                    vistas=('isView', 'sum'),
                    media_dwellTime=('dwellTime', 'mean'),
                    media_sessionTime=('sessionTime', 'mean')
                ).reset_index()

                df_aggregated_data['deviceId']= device_info['id']
                df_aggregated_data['deviceName']= device_info['name']
                df_aggregated_data['media_dwellTime'] = df_aggregated_data['media_dwellTime'].round(1)
                df_aggregated_data['media_sessionTime'] = df_aggregated_data['media_sessionTime'].round(1)

                df_aggregated_data.to_sql("admobilize_impression_data", con=engine, if_exists='append', index=False)
            
                print(df_aggregated_data)


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


    print("Getting data from device " + str(device_info['id']) )
    data_body= {}
    data_body['startTime']=inicio_ayer
    data_body['endTime']=fin_ayer
    data_body['productId']= "facev2"
    data_body['timezone']= "America/Lima"
    data_body['deviceIds']= [device_info['id']]

    print(data_body)
    
    data_b= json.dumps(data_body)

    url_device_data="https://datagatewayapi.admobilize.com/v1alpha1/jobs"
    auth = "Bearer "+ access_token
    headers = {'content-type': 'application/json', 'Authorization': auth}
    r = requests.post(url_device_data, data=data_b, headers=headers)
    print(r.text)
    try:
        data=json.loads(r.text)
        jobs.append(data)

    except:
        print("Error getting data")

    try:
        df_datapoints.drop(df_datapoints.index, inplace=True)
    except:
        None

    try:   
        df_aggregated_data.drop(df_aggregated_data.index, inplace=True)
    except:
        None
    try:    
        df_datapoints_new.drop(df_datapoints_new.index, inplace=True)
    except:
        None

    for job in jobs: 
        while job['status']=="RUNNING":
            time.sleep(5)
            #check status again
            s=requests.get("https://datagatewayapi.admobilize.com/v1alpha1/jobs/" + job['jobId'],headers={'Accept': 'application/json','Authorization': auth});
            data=json.loads(s.text)
            #print(data)
            job['status']=data['status']
        if job['status']=="DONE":


            print("Job ", job['jobId'], " terminado")
            print("datapoints: " + str(df_datapoints.shape[0]))
            
            print("Getting data from job id ", job['jobId'] )
            s=requests.get(" https://datagatewayapi.admobilize.com/v1alpha1/jobs/"+ job['jobId']+"/results",headers={'Accept': 'application/json','Authorization': auth});
            data=json.loads(s.text)
            #print(data)
            # Convertir la cadena JSON en una lista de diccionarios
            datapoints = json.loads(data['datapoints'])
            next_page_token= data['nextPageToken']
            print("datapoints: " + str(df_datapoints.shape[0]))

            # Crear un DataFrame de pandas con esa lista
            df_datapoints = pd.DataFrame(datapoints)
            print(df_datapoints.head())

            if len(df_datapoints)>0:

                #print(df_datapoints.columns)
                print("datapoints: " + str(df_datapoints.shape[0]))
                #print("Next page token: " + next_page_token)

                while next_page_token !="":
                    #load next page
                    print("loading more data")
                    s=requests.get(" https://datagatewayapi.admobilize.com/v1alpha1/jobs/"+ job['jobId']+"/results?pageToken="+next_page_token,headers={'Accept': 'application/json','Authorization': auth});
                    data=json.loads(s.text)
                    # Convertir la cadena JSON en una lista de diccionarios
                    datapoints = json.loads(data['datapoints'])
                    df_datapoints_new = pd.DataFrame(datapoints)
                    print("datapoints: " + str(df_datapoints.shape[0]))
                    print(df_datapoints_new.head())
                    # Añadir df_nuevo a df_original
                    df_datapoints = pd.concat([df_datapoints, df_datapoints_new], ignore_index=True)
                    next_page_token= data['nextPageToken']
                    
                print("datapoints: " + str(df_datapoints.shape[0]))

                print(df_datapoints.head())

                # Convertir la columna "timestamp" a datetime
                df_datapoints['timestamp'] = pd.to_datetime(df_datapoints['timestamp'])
            
                # Crear una nueva columna con la fecha y la hora (sin minutos ni segundos para agrupar por hora exacta)
                df_datapoints['fecha_hora'] = df_datapoints['timestamp'].dt.floor('H')

                # Agrupar los datos por la nueva columna "fecha_hora"
                df_aggregated_data = df_datapoints.groupby('fecha_hora').agg(
                    impresiones=('isImpression', 'sum'),
                    vistas=('isView', 'sum'),
                    media_dwellTime=('dwellTime', 'mean'),
                    media_sessionTime=('sessionTime', 'mean')
                ).reset_index()

                df_aggregated_data['deviceId']= device_info['id']
                df_aggregated_data['deviceName']= device_info['name']
                df_aggregated_data['media_dwellTime'] = df_aggregated_data['media_dwellTime'].round(1)
                df_aggregated_data['media_sessionTime'] = df_aggregated_data['media_sessionTime'].round(1)

                df_aggregated_data.to_sql("admobilize_impression_data", con=engine, if_exists='append', index=False)
            
                print(df_aggregated_data)





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

    print("Getting data from device " + str(device_info['id']) )
    data_body= {}
    data_body['startTime']=inicio_ayer
    data_body['endTime']=fin_ayer
    data_body['productId']= "facev2"
    data_body['timezone']= "America/Bogota"
    data_body['deviceIds']= [device_info['id']]
    
    data_b= json.dumps(data_body)

    url_device_data="https://datagatewayapi.admobilize.com/v1alpha1/jobs"
    auth = "Bearer "+ access_token
    headers = {'content-type': 'application/json', 'Authorization': auth}
    r = requests.post(url_device_data, data=data_b, headers=headers)
    #print(r.text)
    try:
        data=json.loads(r.text)
        jobs.append(data)

    except:
        print("Error getting data")

    for job in jobs: 
        while job['status']=="RUNNING":
            time.sleep(5)
            #check status again
            s=requests.get("https://datagatewayapi.admobilize.com/v1alpha1/jobs/" + job['jobId'],headers={'Accept': 'application/json','Authorization': auth});
            data=json.loads(s.text)
            #print(data)
            job['status']=data['status']
        if job['status']=="DONE":
            print("Job ", job['jobId'], " terminado")
            s=requests.get(" https://datagatewayapi.admobilize.com/v1alpha1/jobs/"+ job['jobId']+"/results",headers={'Accept': 'application/json','Authorization': auth});
            data=json.loads(s.text)
            #print(data)
            # Convertir la cadena JSON en una lista de diccionario

            datapoints = json.loads(data['datapoints'])
            next_page_token= data['nextPageToken']
            # Crear un DataFrame de pandas con esa lista
            df_datapoints = pd.DataFrame(datapoints)

            if len(df_datapoints)>0:

                #print(df_datapoints.columns)
                print("datapoints: " + str(df_datapoints.shape[0]))
                print("Next page token: " + next_page_token)

                while next_page_token !="":
                    #load next page
                    print("loading more data")
                    s=requests.get(" https://datagatewayapi.admobilize.com/v1alpha1/jobs/"+ job['jobId']+"/results?pageToken="+next_page_token,headers={'Accept': 'application/json','Authorization': auth});
                    data=json.loads(s.text)
                    # Convertir la cadena JSON en una lista de diccionarios
                    datapoints = json.loads(data['datapoints'])
                    df_datapoints_new = pd.DataFrame(datapoints)
                    # Añadir df_nuevo a df_original
                    df_datapoints = pd.concat([df_datapoints, df_datapoints_new], ignore_index=True)
                    next_page_token= data['nextPageToken']
                    
                print("datapoints: " + str(df_datapoints.shape[0]))
                print("Next page token: " + next_page_token)

                print(df_datapoints.head())

                # Convertir la columna "timestamp" a datetime
                df_datapoints['timestamp'] = pd.to_datetime(df_datapoints['timestamp'])
            
                # Crear una nueva columna con la fecha y la hora (sin minutos ni segundos para agrupar por hora exacta)
                df_datapoints['fecha_hora'] = df_datapoints['timestamp'].dt.floor('H')

                # Agrupar los datos por la nueva columna "fecha_hora"
                df_aggregated_data = df_datapoints.groupby('fecha_hora').agg(
                    impresiones=('isImpression', 'sum'),
                    vistas=('isView', 'sum'),
                    media_dwellTime=('dwellTime', 'mean'),
                    media_sessionTime=('sessionTime', 'mean')
                ).reset_index()

                df_aggregated_data['deviceId']= device_info['id']
                df_aggregated_data['deviceName']= device_info['name']
                df_aggregated_data['media_dwellTime'] = df_aggregated_data['media_dwellTime'].round(1)
                df_aggregated_data['media_sessionTime'] = df_aggregated_data['media_sessionTime'].round(1)

                df_aggregated_data.to_sql("admobilize_impression_data", con=engine, if_exists='append', index=False)
            
                print(df_aggregated_data)


mycursor.close()

if(mydb.is_connected()):
    mycursor.close()
    print("MySQL connection is closed")