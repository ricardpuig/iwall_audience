# Get Current player status and information from Broadsign and export to Programmatic platforms
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

url_field_report= "https://api.broadsign.com:10889/rest/field_report/v4/scoped?domain_id=17244398"
url_player_status= 'https://api.broadsign.com:10889/rest/monitor_poll/v2?domain_id=17244398'
url_host_by_id= 'https://api.broadsign.com:10889/rest/host/v14/by_id?domain_id=17244398'
url_container_info= 'https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398'
url_incident_report= "https://api.broadsign.com:10889/rest/incident/v3/by_id?domain_id=17244398"
url_display_unit_by_id="https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398"
url_display_unit_type="https://api.broadsign.com:10889/rest/display_unit_type/v6/by_id?domain_id=17244398"
url_content_to_play="https://api.broadsign.com:10889/rest/content/v11/for_hosts?"
url_content_names="https://api.broadsign.com:10889/rest/content/v11/by_id?domain_id=17244398"
url_monitor_poll="https://api.broadsign.com:10889/rest/monitor_poll/v2/by_client_resource_id?domain_id=17244398"
url_reservation_by_id="https://api.broadsign.com:10889/rest/reservation/v22/by_display_unit?domain_id=17244398"
url_host="https://api.broadsign.com:10889/rest/host/v17/by_id?domain_id=17244398"

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
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";

if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

if country=="SPAIN":
        container_ids=["21393898"]   #SPAIN PLAYER CONTAINER ID
		#container_ids=['136035622']
elif country=="COLOMBIA":
        container_ids=['135518539']  #COLOMBIA PLAYER CONTAINER ID
		#container_ids=['145094982']
elif country=="PERU":
        container_ids=['53704276']
else:
	print("Country Missing, exiting....")
	exit(1)

print("Extracting Player Information")

player_field_report={}
field_report=[]

for m in container_ids:

	url_field_report=url_field_report+"&parent_container_ids=" +m
	print("Field Report request: ", url_field_report)
	s=requests.get(url_field_report,headers={'Accept': 'application/json','Authorization': auth})
	data=json.loads(s.text)
	
	for n in data["field_report"]:
		fr=n['field_report']
		if fr: 
			try: 
				if re.search('Player Id:',fr):
					player_id=re.findall('Player Id: (.*)\n', fr)[0]
				elif re.search('Player Id :',fr):
					player_id=re.findall('Player Id : (.*)\n', fr)[0]
			except:
				player_id=None      

			print("player ID:", player_id)
			
			if player_id:
				print("**Player ID found")
				player_field_report['device_id']= "IWALLINSHOP:" + str(player_id)
				player_field_report['screen_id']= player_id
				player_field_report['reach_screen_id']= "broadsign.com:" + str(player_id)
				player_field_report['player_id']=player_id
			
			try: 
				if re.search('Player Version :',fr):
					player_version=re.findall('Player Version :(.*)\n', fr)[0]
				elif re.search('Player Version:',fr):
					player_version=re.findall('Player Version:(.*)\n', fr)[0]
			except: 
				player_version=None
			
			if player_version:
				print("**Player version found")
				player_field_report['player_version']=player_version.strip()

			try: 
				if re.search('OS Version :',fr):
					os_version=re.findall('OS Version :(.*)\n', fr)[0]
				elif re.search('OS Version:',fr):
					os_version=re.findall('OS Version: (.*)\n', fr)[0]
			except: 
				os_version=None
			if os_version:
				print("**OS Version found")
				player_field_report['os_version']=os_version.strip()


			try: 
				if re.search('Screen Resolution :',fr):
					screen_resolution=re.findall('Screen Resolution :(.*)\n', fr)[0]
				elif re.search('Screen Resolution:',fr):
					screen_resolution=re.findall('Screen Resolution:(.*)\n', fr)[0]
				elif re.search('Screen 1 resolution :',fr):
					screen_resolution=re.findall('Screen 1 resolution :(.*)\n', fr)[0]
				elif re.search('Screen 1 resolution:',fr):
					screen_resolution=re.findall('Screen 1 resolution:(.*)\n', fr)[0]

			except: 
				screen_resolution= None

	

			if screen_resolution:
				print("**Screen Resolution found")
				player_field_report['screen_resolution']=screen_resolution.strip()

			try: 
				if re.search('Display Unit Id :',fr):
					display_unit_id=re.findall('Display Unit Id :(.*)\n', fr)[0]
				elif re.search('Display Unit Id:',fr):	
					display_unit_id=re.findall('Display Unit Id:(.*)\n', fr)[0]
			except: 
				display_unit_id=None

			display_unit_id=display_unit_id.strip()

			#monitor poll
			try:
					url_monitor_poll_info=url_monitor_poll+"&client_resource_id="+player_id
					print(url_monitor_poll_info)

					s=requests.get(url_monitor_poll_info,headers={'Accept': 'application/json','Authorization': auth})
					data6=json.loads(s.text)

					for m in data6["monitor_poll"]:
						poll_last_utc=m['poll_last_utc']					
			except: 
					public_ip=""
					private_ip=""
					poll_last_utc=""

			player_field_report['poll_last_utc']=poll_last_utc
			print("host details")

			try:
					url_hosts=url_host+"&ids="+player_id
					print(url_hosts)
					s=requests.get(url_hosts,headers={'Accept': 'application/json','Authorization': auth})
					data8=json.loads(s.text)
					print(data8)
					for m in data8["host"]:
						player_active=m['active']
						player_name=m['name']
						player_screens=m['nscreens']
						
			except: 

					player_active=""
					player_name=""
					player_screens=""
					player_mac1=""
					player_mac2=""
					
			if player_active == False:
				print("Player not active, moving on")
				continue

			player_field_report['player_active']=player_active
			player_field_report['player_name']=player_name
			player_field_report['player_screens']=player_screens

			if "#NP#" in player_name:
				print("Player not for programmatic export - moving on")
				continue
			if player_name=="":
				print("Unkown player name - moving on")
				continue

		
			if display_unit_id:

				print("**Display Unit ID found")
				player_field_report['display_unit_id']=display_unit_id

				try:
					url_display_unit_by_ids=url_display_unit_by_id+"&ids="+display_unit_id

					s=requests.get(url_display_unit_by_ids,headers={'Accept': 'application/json','Authorization': auth})
					data2=json.loads(s.text)
					print(data2)
					for m in data2["display_unit"]:
						du_name=m['name']
						screens = m['host_screen_count']
						du_active = m['active']
						du_container = m['container_id']
						geolocation = m['geolocation']
						zipcode= m['zipcode']
						address=m['address']
						du_type_id= m['display_unit_type_id']

				except: 
					name=""
					screens=""
					du_container=""


				player_field_report['display_unit_name']=du_name
				player_field_report['display_unit_screen_count']=screens	
				player_field_report['geolocation']=geolocation
				player_field_report['longitude']= re.search('\((.*)\,', geolocation).group(1)
				player_field_report['latitude']=re.search('\,(.*)\)', geolocation).group(1)
				player_field_report['zipcode']=zipcode
				player_field_report['address']=address
				player_field_report['display_unit_type_id']=du_type_id
		
				#info for diplay unit type
				try:
					url_display_unit_types=url_display_unit_type+"&ids="+str(du_type_id)
					print(url_display_unit_types)

					s=requests.get(url_display_unit_types,headers={'Accept': 'application/json','Authorization': auth})
					data3=json.loads(s.text)
					print(data3)
					orientation=""
					res_height=""
					res_width=""
					display_type_name=""
					for m in data3["display_unit_type"]:
						orientation=m['orientation']
						res_height = m['res_height']
						res_width = m['res_width']
						display_type_name= m['name']
					
				except:
					print("Error getting display type info")
					orientation=""
					res_height=""
					res_width=""
					display_type_name=""
				
				
				player_field_report['display_type_res_width']=res_width
				player_field_report['display_type_res_height']=res_height
				player_field_report['display_type_name']=display_type_name

				if "#RES_" in player_name:
					player_field_report['ad_resolution']=re.search('\#RES_(.*)\#', player_name).group(1)
				else:
					player_field_report['ad_resolution']=str(res_width)+"x"+str(res_height)

				

				container_name=""
				container_active=""
				try:
					url_container_infos=url_container_info+"&ids="+ str(du_container)

					print(url_container_infos)
					
					s=requests.get(url_container_infos,headers={'Accept': 'application/json','Authorization': auth})
					data3=json.loads(s.text)
					print(data3)
					for m in data3["container"]:
						container_name=m['name']
						container_active = m['active']
						container_id = m['id']
			
				except: 
					container_name=""
					container_active=""

				player_field_report['container_name']=container_name
				player_field_report['site_id']=container_id
				player_field_report['programmatic_name']=country + "-" + container_name + "-" + str(player_screens) + " screens"	



			field_report.append(player_field_report)
			print("\n\n\nPlayer Report:\n----------------")

			print(json.dumps(player_field_report, indent = 2))			
			player_field_report={}


df_field_report = pd.DataFrame(field_report)
df_field_report['reach_publisher_id']=37
df_field_report['allowed_ad_types:id']="html, video, images"
df_field_report['audience_data_sources:id']="PORTAL-Inspide-telco data-cam data-admobilize"
df_field_report['floor_cpm_eur']=10.0
df_field_report['min_ad_duration_sec']=10.0
df_field_report['max_ad_duration_sec']=30.0
df_field_report['country']=country



'''
df_field_report['dma_code']=""
df_field_report['tags:id']="11571"
df_field_report['bearing_direction']=""
df_field_report['diagonal_size']=""
df_field_report['diagonal_size_units']=""
df_field_report['venue_types:id']="60"

df_field_report['audience_data_sources:id']="4"
df_field_report['screen_img_url']=""
df_field_report['min_ad_duration']=10.0
df_field_report['max_ad_duration']=30.0
df_field_report['demography_type']="basic"
df_field_report['average_weekly_impressions']=15000.0
df_field_report['internal_publisher_screen_id']=""
df_field_report['hivestack_id']=""
df_field_report['vistar_id']="" 
df_field_report['allows_motion']=0
df_field_report['ox_enabled']=0
df_field_report['floor_cpm_usd']=""
df_field_report['floor_cpm_cad']=""
df_field_report['floor_cpm_eur']=16.0
df_field_report['floor_cpm_gbp']=""
df_field_report['floor_cpm_aud']=""
'''


#reach export
#df_reach_export= df_field_report[['publisher_id', 'device_id', 'programmatic_name', 'screen_resolution', 'latitude', 'longitude', 'dma_code', 'tags:id', 'address', 'bearing_direction', \
#									'diagonal_size','diagonal_size_units', 'venue_types:id', 'allowed_ad_types:id', 'audience_data_sources:id', 'screen_img_url' , 'min_ad_duration','max_ad_duration'  ,\
#									'demography_type', 'total_weekday_morning', 'total_weekday_afternoon','total_weekday_night','total_night','total_weekend', 'average_weekly_impressions', 'internal_publisher_screen_id', 'hivestack_id','vistar_id', 'allows_motion',  'ox_enabled',\
#										'floor_cpm_usd','floor_cpm_cad', 'floor_cpm_eur', 	'floor_cpm_gbp', 'floor_cpm_aud' ]]


#df_reach_export=df_reach_export.rename(columns={'programmatic_name': 'name', 'screen_resolution': 'resolution'})

df_reach_export= df_field_report
df_reach_export.to_csv('programmatic_export.csv', index=False, sep= ",")

try:
	sql= "DELETE FROM programmatic_players WHERE country='%s'" % (country)
	mycursor.execute(sql)
	mydb.commit()
except: 
	print("Table not exists")

df_reach_export.to_sql('programmatic_players', engine, if_exists='append', index=False)

#hivestack export

















