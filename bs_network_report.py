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
			player_field_report['player_id']=player_id

		try: 
			hostname=re.findall('Hostname : (.*)\n', fr)[0]
		except: 
			hostname=None
		if hostname:
			player_field_report['hostname']=hostname

		try: 
			player_version=re.findall('Player Version : (.*)\n', fr)[0]
		except: 
			player_version=None
		if player_version:
			player_field_report['player_version']=player_version

		try: 
			os_version=re.findall('OS Version : (.*)\n', fr)[0]
		except: 
			os_version=None
		if os_version:
			player_field_report['os_version']=os_version

		try: 
			screen_resolution=re.findall('Screen Resolution : (.*)\n', fr)[0]
		except: 
			screen_resolution=None
		if screen_resolution:
			player_field_report['screen_resolution']=screen_resolution

		try: 
			chromium_version=re.findall('Chromium Version : (.*)\n', fr)[0]
		except: 
			chromium_version=None
		if chromium_version:
			player_field_report['chromium_version']=chromium_version

		try: 
			disk_usg=re.findall('Disk Usage : (.*)\n', fr)[0]
		except: 
			disk_usg=None
		if disk_usg:
			player_field_report['disk_usage']=disk_usg


		try: 
			display_unit_id=re.findall('Display Unit Id: (.*)\n', fr)[0]
		except: 
			display_unit_id=None
		if display_unit_id:
			player_field_report['display_unit_id']=display_unit_id



			try:

				url_display_unit_by_id=url_display_unit_by_id+"&ids="+display_unit_id

				s=requests.get(url_display_unit_by_id,headers={'Accept': 'application/json','Authorization': auth})
				data2=json.loads(s.text)
				for m in data2["display_unit"]:
					name=m['name']
					screens = m['host_screen_count']
			except: 

				name=""
				screens=""


			player_field_report['display_unit_name']=name
			player_field_report['screen_count']=screens	

		field_report.append(player_field_report)

		player_field_report={}

df_field_report = pd.DataFrame(field_report)

df_field_report.to_sql('player_status', con = engine, if_exists = 'append', chunksize = 1000)

	#get DU name from player ID







