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
from pandasgui import show

#HOURLY DEFAULT MULTIPLIERS ( will be modulated by inspide data) List for each day of the week.
IMP_10 = [1,1,1,1,1,1,1] 
IMP_11 = [1,1,1,1,1,1,1]
IMP_12 = [1,1,1,1,1,1,1]
IMP_13 = [1,1,1,1,1,1,1]
IMP_14 = [1,1,1,1,1,1,1]
IMP_15 = [1,1,1,1,1,1,1]
IMP_16 = [1,1,1,1,1,1,1]
IMP_17 = [1,1,1,1,1,1,1]
IMP_18 = [1,1,1,1,1,1,1]
IMP_19 = [1,1,1,1,1,1,1]
IMP_20 = [1,1,1,1,1,1,1]
IMP_21 = [1,1,1,1,1,1,1]

START_DATE_AUDIENCE_DATA="2022-01-01"
END_DATE_AUDIENCE_DATA="2023-12-31"



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


def generate_audience_data(df_field_report):

	#broadsign
	df_field_report['start_date'] = START_DATE_AUDIENCE_DATA
	df_field_report['end_date'] = END_DATE_AUDIENCE_DATA

	reach_screens=df_field_report[['screen_id']]

	screens_id= df_field_report["screen_id"].values.tolist()
	for screen_id in screens_id: 
		print("Create audience data for Screen ", screen_id)
		audience_data_basic= {"col1":[1, 2, 3], "col2":[4, 5, 6], "col3":[7, 8, 9]}
		df_audience_screen = pd.DataFrame(data)



	print(reach_screens)
	input()
	return reach_screens, reach_screens








	df_reach_audience_export_1=df_field_report_weekday_morning[['screen_id','start_date','end_date', 'start_time', 'end_time', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'demography_type', 'total_weekday_morning',\
											  'males', 'females', 'males_12', 'males_18', 'males_25', 'males_35', 'males_45', 'males_55', 'males_65','females_12', 'females_18', 'females_25', 'females_35', 'females_45', 'females_55', 'females_65' ]]
	df_reach_audience_export_1=df_reach_audience_export_1.rename(columns={'total_weekday_morning': 'total'})

	df_reach_audience_export_2=df_field_report_weekday_afternoon[['screen_id','start_date','end_date', 'start_time', 'end_time', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'demography_type', 'total_weekday_afternoon',\
											  'males', 'females', 'males_12', 'males_18', 'males_25', 'males_35', 'males_45', 'males_55', 'males_65','females_12', 'females_18', 'females_25', 'females_35', 'females_45', 'females_55', 'females_65' ]]
	df_reach_audience_export_2=df_reach_audience_export_2.rename(columns={'total_weekday_afternoon': 'total'})

	df_reach_audience_export_3=df_field_report_weekday_night[['screen_id','start_date','end_date', 'start_time', 'end_time', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'demography_type', 'total_weekday_night',\
											  'males', 'females', 'males_12', 'males_18', 'males_25', 'males_35', 'males_45', 'males_55', 'males_65','females_12', 'females_18', 'females_25', 'females_35', 'females_45', 'females_55', 'females_65' ]]
	df_reach_audience_export_3=df_reach_audience_export_3.rename(columns={'total_weekday_night': 'total'})

	df_reach_audience_export_4=df_field_report_night[['screen_id','start_date','end_date', 'start_time', 'end_time', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'demography_type', 'total_night',\
											  'males', 'females', 'males_12', 'males_18', 'males_25', 'males_35', 'males_45', 'males_55', 'males_65','females_12', 'females_18', 'females_25', 'females_35', 'females_45', 'females_55', 'females_65' ]]
	df_reach_audience_export_4=df_reach_audience_export_4.rename(columns={'total_night': 'total'})

	df_reach_audience_export_5=df_field_report_weekend[['screen_id','start_date','end_date', 'start_time', 'end_time', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'demography_type', 'total_weekend',\
											  'males', 'females', 'males_12', 'males_18', 'males_25', 'males_35', 'males_45', 'males_55', 'males_65','females_12', 'females_18', 'females_25', 'females_35', 'females_45', 'females_55', 'females_65' ]]
	df_reach_audience_export_5=df_reach_audience_export_5.rename(columns={'total_weekend': 'total'})

	df_reach_audience_export=df_reach_audience_export_1.copy()
	df_reach_audience_export = pd.concat([df_reach_audience_export_2, df_reach_audience_export_3,df_reach_audience_export_4, df_reach_audience_export_5], ignore_index=True)


whitelist_players=[]

def load_whitelist_players():
	#load player filters
    sql_select = "SELECT player_id from programmatic_player_whitelist"
    mycursor.execute(sql_select)
    records= mycursor.fetchall()    
    for row_0 in records:  #for each result
        whitelist_players.append(row_0[0])




if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

if country=="SPAIN":
        #container_ids=["21393898"]
		container_ids=['136035622']
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

load_whitelist_players()

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
			
			print("white:", whitelist_players)
			if not int(player_id) in whitelist_players:
				print("PLayer not in white list- not processing")
				continue
			
			if player_id:
				print("**Player ID found")
				player_field_report['device_id']= "broadsign.com:" + str(player_id)
				player_field_report['screen_id']= "broadsign.com:" + str(player_id)
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

			'''
			try: 
				disk_usg=re.findall('Disk Usage (.*)\n', fr)[0]
			except: 
				disk_usg=None
			if disk_usg:
				player_field_report['disk_usage']=disk_usg

			'''

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
						public_ip=m['public_ip']
						private_ip=m['private_ip']
						poll_last_utc=m['poll_last_utc']

					
			except: 

					public_ip=""
					private_ip=""
					poll_last_utc=""

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



			#content to play
			print("Content scheduled to play now")
			try:

					url_content_to_play_list=url_content_to_play+"ids="+player_id+"&current_day="+ sys.argv[2]
					print(url_content_to_play_list)

					s=requests.get(url_content_to_play_list,headers={'Accept': 'application/json','Authorization': auth})
					data4=json.loads(s.text)
					print(data4)
					content_names = ""
					num_contents=0

					for m in data4["host"]:
						content_ids=m['content_ids']
						print("Contents: ", content_ids)

						num_contents=len(content_ids.split(','))

						url_content_names_list=url_content_names+"&ids="+content_ids
						print(url_content_names_list)
						s=requests.get(url_content_names_list,headers={'Accept': 'application/json','Authorization': auth})
						data5=json.loads(s.text)
						#print(data5)
						for n in data5["content"]:
							content_names=content_names + " * "+ n['name']
			except: 

					content_names=""
					num_contents=0

			print("Contents: ", content_names)

		
			#player_field_report['contents']=content_names
			#player_field_report['num_contents']=num_contents



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
					number_of_campaigns=""
		
				#player_field_report['campaign_names']=campaign_names
				#player_field_report['number_of_campaigns']=number_of_campaigns



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
				player_field_report['screen_count']=screens	
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
				player_field_report['container_active']=container_active	
				player_field_report['programmatic_name']=country + "-" + container_name + "-" + str(player_screens) + " screens"	



				#impressions multiplier
				player_field_report['total_weekday_morning']=player_screens
				player_field_report['total_weekday_afternoon']=player_screens*2
				player_field_report['total_weekday_night']=player_screens*3
				player_field_report['total_night']=player_screens*0
				player_field_report['total_weekend']=player_screens*5


			field_report.append(player_field_report)
			print("------------ \nPlayer Report:\n--------------------")


			#build audience data 

			print(player_field_report)
			player_field_report={}
			



	df_field_report = pd.DataFrame(field_report)
	df_field_report['publisher_id']=37
	df_field_report['dma_code']=""
	df_field_report['tags:id']="11571"
	df_field_report['bearing_direction']=""
	df_field_report['diagonal_size']=""
	df_field_report['diagonal_size_units']=""
	df_field_report['venue_types:id']="60"
	df_field_report['allowed_ad_types:id']="1:2:4"
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

	df_reach_export= df_field_report[['publisher_id', 'device_id', 'programmatic_name', 'screen_resolution', 'latitude', 'longitude', 'dma_code', 'tags:id', 'address', 'bearing_direction', \
										'diagonal_size','diagonal_size_units', 'venue_types:id', 'allowed_ad_types:id', 'audience_data_sources:id', 'screen_img_url' , 'min_ad_duration','max_ad_duration'  ,\
										'demography_type', 'total_weekday_morning', 'total_weekday_afternoon','total_weekday_night','total_night','total_weekend', 'average_weekly_impressions', 'internal_publisher_screen_id', 'hivestack_id','vistar_id', 'allows_motion',  'ox_enabled',\
											'floor_cpm_usd','floor_cpm_cad', 'floor_cpm_eur', 	'floor_cpm_gbp', 'floor_cpm_aud' ]]
	df_reach_export=df_reach_export.rename(columns={'programmatic_name': 'name', 'screen_resolution': 'resolution'})

	df_reach_audience_export, df_hivestack_audience_export = generate_audience_data(df_field_report)
		
	#df_field_report.to_sql('player_status', con = engine, if_exists = 'append', chunksize = 1000)
	df_field_report.to_csv('player_status.csv', index=False, sep= ";")
	df_reach_export.to_csv('reach_export.csv', index=False, sep= ",")
	df_reach_audience_export.to_csv('reach_audience_export.csv', index=False, sep= ",")

	show(df_field_report)
	show(df_reach_export)
	show(df_reach_audience_export)












