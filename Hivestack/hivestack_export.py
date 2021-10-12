# Get Current player status and information from Broadsign
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

url_field_report= "https://api.broadsign.com:10889/rest/field_report/v4/by_domain_id?domain_id=17244398"


url_player_status= 'https://api.broadsign.com:10889/rest/monitor_poll/v2?domain_id=17244398'
url_host_by_id= 'https://api.broadsign.com:10889/rest/host/v14/by_id?domain_id=17244398'
url_container_info= 'https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398'
url_incident_report= "https://api.broadsign.com:10889/rest/incident/v3/by_id?domain_id=17244398"
url_display_unit_by_id="https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398"








engine = create_engine("mysql+pymysql://{user}:{pw}@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/{db}"
                       .format(user="root",
                               pw="sonaeRootMysql2017",
                               db="audience"))

#Authorization
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";

print("Extracting Player Information")

player_field_report={}
field_report=[]

#print(url_reservation_container)
s=requests.get(url_field_report,headers={'Accept': 'application/json','Authorization': auth})
data=json.loads(s.text)


for n in data["field_report"]:
	fr=n['field_report']
	if fr: 
		print(fr)

		try: 
			player_id=re.findall('Player Id : (.*)\n', fr)[0]
		except: 
			player_id=None
		if player_id:
			player_field_report['Screen ID']=player_id

		try: 
			screen_resolution=re.findall('Screen Resolution : (.*)\n', fr)[0]
			screen_resolution_x=screen_resolution.split("x")[0]
		except: 
			screen_resolution_x=None


		if screen_resolution_x:
			player_field_report['screen_width']=screen_resolution_x


		try: 
			screen_resolution=re.findall('Screen Resolution : (.*)\n', fr)[0]
			screen_resolution_y=screen_resolution.split("x")[1]
		except: 
			screen_resolution_y=None


		if screen_resolution_y:
			player_field_report['screen_height']=screen_resolution_y



		try: 
			display_unit_id=re.findall('Display Unit Id: (.*)\n', fr)[0]
		except: 
			display_unit_id=None

		if display_unit_id:
			player_field_report['display_unit_id']=display_unit_id



			try:

				url_display_unit_by_ids=url_display_unit_by_id+"&ids="+display_unit_id

				s=requests.get(url_display_unit_by_ids,headers={'Accept': 'application/json','Authorization': auth})
				data2=json.loads(s.text)
				for m in data2["display_unit"]:
					du_name=m['name']
					screens = m['host_screen_count']
					du_active = m['active']
					du_container = m['container_id']
			except: 

				name=""
				screens=""
				du_container=""


			player_field_report['display_unit_name']=du_name
			player_field_report['screen_count']=screens	
			player_field_report['display_unit_active']=du_active


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


		field_report.append(player_field_report)

		player_field_report={}

df_field_report = pd.DataFrame(field_report)


df_field_report.to_csv('hivestack_players', index=True)







