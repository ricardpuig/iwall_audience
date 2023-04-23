# Get Current player status and information from Broadsign and update AUDIENCE player information
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

#filtering players:
du_container_blacklist=[]
du_id_whitelist=[]
temporary_players=[]
wifi_players=[]
sunday_du_container_blacklist=[]
morning_blacklist_players=[]
critical_players=[]
player_container_blacklist=[]
player_id_blacklist=[]

def load_filtering_players():
	#load player filters
    sql_select = "SELECT broadsign_id, type from player_monitoring_filtering"
    mycursor.execute(sql_select)
    records= mycursor.fetchall()    
    for row_0 in records:  #for each result
		#load wifi players
        if row_0[1] =="WIFI_PLAYER":
           wifi_players.append(row_0[0])
        if row_0[1] =="DU_CONTAINER_ID_BLACKLIST":
           du_container_blacklist.append(row_0[0])
        if row_0[1] =="DU_ID_WHITELIST":
           du_id_whitelist.append(row_0[0])
        if row_0[1] =="TEMPORARY_PLAYER":
           temporary_players.append(row_0[0])
        if row_0[1] =="CRITICAL_PLAYER":
           critical_players.append(row_0[0])
        if row_0[1] =="PLAYER_CONTAINER_ID_BLACKLIST":
           player_container_blacklist.append(row_0[0])
        if row_0[1] =="PLAYER_ID_BLACKLIST":
           player_id_blacklist.append(row_0[0])

def alarms_check(df_report):

	email_to_send="rpuig@iwallinshop.com"
	client = Courier(auth_token="pk_prod_6S1S6BVGGXMEB5Q8TSVHDN59NY8D")
	auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";
	report_template= "63S3SQJ5EA4YAYH10N88VCJJE6QT"
	message_to_send =""

	df_duplicated= df_report[df_report.duplicated(['display_unit_id'], keep=False)]
	if len(df_duplicated)>0: 
		message_to_send= "Display unit duplicada en varios players: " + str(df_duplicated['player_id'].to_list())

	message_to_send= message_to_send + "\n"

	#check player version
	df_versions=df_report.loc[df_report['player_version'].str.startswith("13.0.")]
	if len(df_versions)>0: 
		message_to_send= message_to_send + "\n" + "Player Version too old (13.0): " + str(df_versions['player_id'].to_list())
	df_versions=df_report.loc[df_report['player_version'].str.startswith("12.0")]
	if len(df_versions)>0: 
		message_to_send= message_to_send + "\n" + "Player Version too old (12): " + str(df_versions['player_id'].to_list())
	df_versions=df_report.loc[df_report['player_version'].str.startswith("11.1")]
	if len(df_versions)>0: 
		message_to_send= message_to_send + "\n" + "Player Version too old (11): " + str(df_versions['player_id'].to_list())
	
	message_to_send= message_to_send + "\n"
	#check os version
	df_versions=df_report.loc[df_report['os_version'].str.contains("XP")]
	if len(df_versions)>0: 
		message_to_send= message_to_send + "\n" + "Player OS version too old: " + str(df_versions['player_id'].to_list())

	message_to_send= message_to_send + "\n"

	#check player screens
	df_versions=df_report.loc[df_report['player_screens']==0]
	if len(df_versions)>0: 
		message_to_send= message_to_send + "\n" + "Player screens empty in players: " + str(df_versions['player_id'].to_list())

	message_to_send= message_to_send + "\n"
	#check geo coord
	df_geo=df_report.loc[df_report['latitude'].isin(["", 0])]
	if len(df_geo)>0: 
		message_to_send= message_to_send + "\n" + "Coordinate empty in players: " + str(df_geo['player_id'].to_list())

	df_geo=df_report.loc[df_report['longitude'].isin(["", 0])]
	if len(df_geo)>0: 
		message_to_send= message_to_send + "\n" + "Coordinate empty in players:  " + str(df_geo['player_id'].to_list())

	query = "SELECT * from malls"

	df_malls = pd.read_sql_query(query, engine)
	list_container_ids= df_malls['broadsign_container_id'].to_list()
	print(list_container_ids)
	
	df_containers=df_report.loc[~df_report['display_unit_container_id'].isin(list_container_ids)]
	if len(df_containers)>0: 
		message_to_send= message_to_send + "\n" + "Player not in mall DB:  " + str(df_containers['player_id'].to_list())
	
	#check player last call
	resp = client.send_message(
				message={
					"to": {
						"email": email_to_send,
					},
					"template": report_template,
						"data": {
							"message_type" : "Player Inventory Update: ",
							"message": message_to_send
						},
				})
	print(resp['requestId'])
	None

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

#Authorization
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";

if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

if country=="SPAIN":
        container_ids=["21393898"]
		#container_ids=['136035622']
elif country=="COLOMBIA":
        container_ids=['135518539']
		#container_ids=['145094982']	
elif country=="PERU":
        container_ids=['53704276']
else:
	print("Country Missing, exiting....")
	exit(1)

print("Extracting Player Information")

player_field_report={}
field_report=[]

load_filtering_players()

for m in container_ids:

	url_field_report=url_field_report+"&parent_container_ids=" +m
	print("Field Report request: ", url_field_report)
	#print(url_reservation_container)
	s=requests.get(url_field_report,headers={'Accept': 'application/json','Authorization': auth})
	data=json.loads(s.text)
	print(data)
	
	for n in data["field_report"]:
		fr=n['field_report']
		if fr: 
			print(fr)
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
				player_field_report['player_id']=player_id
			try: 
				if re.search('Hostname :',fr):
					hostname=re.findall('Hostname :(.*)\n', fr)[0]
				elif re.search('Hostname:',fr):
					hostname=re.findall('Hostname:(.*)\n', fr)[0]
			except: 
				hostname=None
			if hostname:
				print("**Hostname found")
				player_field_report['hostname']=hostname
			try: 
				if re.search('Player Version :',fr):
					player_version=re.findall('Player Version :(.*)\n', fr)[0]
				elif re.search('Player Version:',fr):
					player_version=re.findall('Player Version:(.*)\n', fr)[0]
			except: 
				player_version=None
			
			frame_id=False
			try: 
				if re.search('currently playing',fr):
					frame_id=re.findall('Frame (.*) currently', fr)[0]
			except: 
				frame_id=None
			if frame_id:
				player_field_report['frame_id']=frame_id

			if player_version:
				print("**Player version found")
				player_field_report['player_version']=player_version
			try: 
				if re.search('OS Version :',fr):
					os_version=re.findall('OS Version :(.*)\n', fr)[0]
				elif re.search('OS Version:',fr):
					os_version=re.findall('OS Version: (.*)\n', fr)[0]
			except: 
				os_version=None
			if os_version:
				print("**OS Version found")
				player_field_report['os_version']=os_version
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
				player_field_report['screen_resolution']=screen_resolution
			try: 
				if re.search('Chromium Version :',fr):
					chromium_version=re.findall('Chromium Version :(.*)\n', fr)[0]
				elif re.search('Chromium Version:',fr):
					chromium_version=re.findall('Chromium Version:(.*)\n', fr)[0]
			except: 
				chromium_version=None
			if chromium_version:
				print("**Chromium version found")
				player_field_report['chromium_version']=chromium_version

			try: 
				if re.search('Display Unit Id :',fr):
					display_unit_id=re.findall('Display Unit Id :(.*)\n', fr)[0]
				elif re.search('Display Unit Id:',fr):	
					display_unit_id=re.findall('Display Unit Id:(.*)\n', fr)[0]
			except: 
				display_unit_id=None

			display_unit_id=display_unit_id.strip()
			#monitor poll

			url_monitor_poll_info=url_monitor_poll+"&client_resource_id="+player_id
			print(url_monitor_poll_info)

			try:
					s=requests.get(url_monitor_poll_info,headers={'Accept': 'application/json','Authorization': auth})
					data6=json.loads(s.text)
					print(data6)

					for m in data6["monitor_poll"]:
						public_ip=m['public_ip']
						private_ip=m['private_ip']
						poll_last_utc=m['poll_last_utc']			
			except: 

					public_ip=""
					private_ip=""
					poll_last_utc=""

			print("DU*********** ", display_unit_id)

			player_field_report['public_ip']=public_ip
			player_field_report['private_ip']=private_ip
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
						player_mac1=m['primary_mac_address']
						player_mac2=m['secondary_mac_address']		
						player_container_id=m['container_id']		
			except: 

					player_active=""
					player_name=""
					player_screens=""
					player_mac1=""
					player_mac2=""
					
			player_field_report['player_active']=player_active
			player_field_report['player_name']=player_name
			player_field_report['player_screens']=player_screens
			player_field_report['player_mac1']=player_mac1
			player_field_report['player_mac2']=player_mac2
			player_field_report['player_container_id']=player_container_id

			if display_unit_id:


				print("**Display Unit ID found")
				player_field_report['display_unit_id']=display_unit_id

				print("**Getting number of current campaigns for this player")
				try:
					campaign_names =""
					number_of_campaigns= 0
					url_reservation_by_ids=url_reservation_by_id+"&current_only=True&display_unit_id="+display_unit_id

					s=requests.get(url_reservation_by_ids,headers={'Accept': 'application/json','Authorization': auth})
					data7=json.loads(s.text)
					for m in data7["reservation"]:
						if m['active']==True:
							if campaign_names.find(m['name'])==-1:
								campaign_names=campaign_names + " * " + m['name']
								number_of_campaigns= number_of_campaigns +1				

				except: 

					campaign_names=""
					number_of_campaigns=0


				player_field_report['current_campaigns']=number_of_campaigns

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

				except: 

					name=""
					screens=""
					du_container=""


				player_field_report['display_unit_name']=du_name
				player_field_report['display_unit_container_id']=du_container
				
				player_field_report['display_unit_active']=du_active
				player_field_report['geolocation']=geolocation
				player_field_report['longitude']= re.search('\((.*)\,', geolocation).group(1)
				player_field_report['latitude']=re.search('\,(.*)\)', geolocation).group(1)
				player_field_report['zipcode']=zipcode
				player_field_report['address']=address

				
				#info for diplay unit type

				try:
					url_display_unit_types=url_display_unit_type+"&ids="+display_unit_id
					print(url_display_unit_types)

					s=requests.get(url_display_unit_types,headers={'Accept': 'application/json','Authorization': auth})
					data3=json.loads(s.text)
					print(data3)
					for m in data3["display_unit_type"]:
						orientation=m['orientation']
						res_height = m['res_height']
						res_width = m['res_width']
						display_type_name= m['name']
				except:
					orientation=""
					res_height=""
					res_width=""
					display_type_name=""
				
				#player_field_report['orientation']=orientation
				#player_field_report['res_height']=res_height
				#player_field_report['res_width']=res_width
				#player_field_report['display_type_name']=display_type_name
				#extract container name

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
			
				except: 

					container_name=""
					container_active=""

				player_field_report['container_name']=container_name
				player_field_report['programmatic_name']=country + "-" + container_name + "-" + str(player_screens) + " screens"	

			if (str(player_field_report['player_id']) in player_id_blacklist)or (str(player_field_report['player_container_id']) in player_container_blacklist)or (str(player_field_report['display_unit_container_id']) in du_container_blacklist) or (player_field_report['player_id'] in temporary_players):
				print("Player not being monitored")
			
			else:
				#add mall id 
				#load player filters
				player_field_report['mall_id']=0
				player_field_report['commments']=""
				player_field_report['country']=country
				
				if player_field_report['display_unit_container_id']!="": 
					sql_select = "SELECT id from malls where broadsign_container_id ={id}".format(id=player_field_report['display_unit_container_id'])
					print(sql_select)
					mycursor.execute(sql_select)
					records= mycursor.fetchall()
					if len(records)==0:
						player_field_report["commments"]="no mall id found in DB for this player"
					if len(records)>1:
						player_field_report["commments"]="more than 1 mall id found in DB for this player"
					for row in records:  
						player_field_report['mall_id']=row[0]
					
				field_report.append(player_field_report)
			print("------------ \nPlayer Report:\n--------------------")

			print(player_field_report)
			player_field_report={}


	df_field_report = pd.DataFrame(field_report)
	alarms_check(df_field_report)
	#show(df_field_report)
	#send to DB

	query="DELETE FROM players where country='%s'" % (country)
	mycursor.execute(query)
	mydb.commit()
	df_field_report.to_sql('players', engine, if_exists='append', index=False)

