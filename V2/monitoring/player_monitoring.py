# Get Current player status and information from Broadsign
from dis import dis
from threading import local
from time import time
import requests
import json
import re
import pymysql
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import numpy as np
import logging as log
from datetime import datetime, timedelta
from datetime import date
import sys
import pytz
from trycourier import Courier 

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

email_to_send="rpuig@iwallinshop.com"
client = Courier(auth_token="pk_prod_6S1S6BVGGXMEB5Q8TSVHDN59NY8D")
auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";
report_template= "8Y3M47W2PFMNS0HMPN3F6CB0QSYM"
alarm_template  = "Q7X9P35YYQ4PBPH91GM56A31D63Y"

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

#player lists
black_players=[]
no_screens_players=[]
missing_players=[]
offline_players=[]
no_campaigns_players=[]
main_player_status = []
temp_wifi_players =[]
discarded_players =[]


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


def report_valid():
	player_status_report =""
	valid = False
	current_time_madrid=  datetime.now(pytz.timezone('Europe/Madrid'))
	if current_time_madrid.hour ==11 :
		player_status_report= "MORNING"
		valid= True
	current_time_colombia=  datetime.now(pytz.timezone('America/Bogota'))
	if current_time_madrid.hour ==11 :
		player_status_report="MORNING"
		valid= True
	current_time_peru=  datetime.now(pytz.timezone('America/Lima'))
	if current_time_madrid.hour ==11 :
		player_status_report= "MORNING"
		valid= True

	if current_time_madrid.hour ==14 :
		player_status_report= "AFTERNOON"
		valid= True
	current_time_colombia=  datetime.now(pytz.timezone('America/Bogota'))
	if current_time_madrid.hour ==14 :
		player_status_report= "AFTERNOON"
		valid= True
	current_time_peru=  datetime.now(pytz.timezone('America/Lima'))
	if current_time_madrid.hour ==14 :
		player_status_report= "AFTERNOON"
		valid= True


	if current_time_madrid.hour ==18:
		player_status_report= "NIGHT"
		valid= True
	current_time_colombia=  datetime.now(pytz.timezone('America/Bogota'))
	if current_time_madrid.hour ==18:
		player_status_report="NIGHT"
		valid= True
	current_time_peru=  datetime.now(pytz.timezone('America/Lima'))
	if current_time_madrid.hour ==18 :
		player_status_report= "NIGHT"
		valid= True
	return valid, player_status_report


def checkin_alarm(player_field_report):

	time_missing=""
	if  player_field_report['last_checkin_min']>60:
		if player_field_report['last_checkin_min']>5000:
			time_missing=str(round((player_field_report['last_checkin_min']/60)/24,1))+ " dias!. \n La última vez en línea fue : " + str(player_field_report['last_checkin_time']) 	
		else:
			time_missing=str(round(player_field_report['last_checkin_min']/60,1))+ " horas!. \n La última vez en línea fue : " + str(player_field_report['last_checkin_time']) 
	else:
		time_missing=str(round(player_field_report['last_checkin_min'],1)) + " minutos!"
	

	resp = client.send_message(
				message={
					"to": {
						"email": email_to_send,
					},
					"template": alarm_template,
						"data": {
							"mall": player_field_report['container_name'] + "- " + str(player_field_report["player_screens"]) + " pant",
							"title": "MISSING! ",
							"message" : "Player no ha dado señales desde hace " + time_missing,
							"player" : player_field_report['player_name']
						},
				})
	print(resp['requestId'])

def blackscreen_alarm(player_field_report):
	resp = client.send_message(
				message={
					"to": {
						"email": email_to_send,
					},
					"template": alarm_template,
						"data": {
							"mall": player_field_report['container_name'] + "- " + str(player_field_report["player_screens"]) + " pant",
							"title": "BLACK!",
							"message" : "Player posiblemente en negro ",
							"player" : player_field_report['player_name']
						},
				})
	print(resp['requestId'])

def no_contents_alarm(player_field_report):
	resp = client.send_message(
				message={
					"to": {
						"email": email_to_send,
					},
					"template": alarm_template,
						"data": {
							"mall": player_field_report['container_name'] + "- " + str(player_field_report["player_screens"]) + " pant",
							"title": "NO CONTENT!",
							"message" : "Player posiblemente sin contenido ",
							"player" : player_field_report['player_name']
						},
				})
	print(resp['requestId'])

def insert_alarm_db(player_field_report, alarm_type):

	
	query="INSERT INTO player_health (player_id,player_name,mall_name, alarm, alarm_type) VALUES (\"{p1}\",\"{p2}\", \"{p3}\", \"{p4}\", \"{p5}\")".format(
            p1= str(player_field_report['player_id']),
            p2= player_field_report['player_name'],
            p4 =datetime.now().strftime("%Y-%m-%d %H:%M:%S") ,
			p3 =player_field_report["container_name"] ,
			p5 = alarm_type
    )
	engine.execute(query)


def alarms_check(player_field_report):

	print("Alarms check: ")
	print("Player Field Report:  " , player_field_report)



	if (str(player_field_report['player_id']) in player_id_blacklist)or (str(player_field_report['player_container_id']) in player_container_blacklist)or (str(player_field_report['du_container_id']) in du_container_blacklist) or (player_field_report['player_id'] in wifi_players) or (player_field_report['player_id'] in temporary_players):
		print("Alarms for this player disabled")
		discarded_players.append(player_field_report['player_id'])

	else:
		if player_field_report['last_checkin_min']>60 and player_field_report['last_checkin_min']<6000 and player_field_report['player_screens']>0:
			print("Sending Alarm for late check in")
			missing_players.append(player_field_report['container_name']+ " : " + player_field_report['player_name'] + "\n ( hace " + str(player_field_report['last_checkin_min']) + " min )")
			#checkin_alarm(player_field_report)
			insert_alarm_db(player_field_report, "MISSING")
		if player_field_report['num_frames']=='0' and player_field_report['last_checkin_min']<60 and player_field_report['player_screens']>0:
			print("Num frames 0, probably in black ")
			black_players.append(player_field_report['container_name']+ " : " + player_field_report['player_name'])
			blackscreen_alarm(player_field_report)
			#insert_alarm_db(player_field_report, "BLACK")
		if player_field_report['player_screens']==0 :
			print("No screens in player")
			no_screens_players.append(player_field_report['container_name']+ " : " + player_field_report['player_name'])
		if player_field_report['last_checkin_min']>6000 and player_field_report['player_screens']>0:
			offline_players.append( player_field_report['container_name']+ "\n Player: " + player_field_report['player_name'] + "\n ( desde " + player_field_report['last_checkin_time'] + ")")
		if player_field_report['num_contents']==0 and player_field_report['player_screens']>0:
			no_campaigns_players.append(player_field_report['container_name']+ " : " + player_field_report['player_name'])
			#no_contents_alarm(player_field_report)

		if player_field_report['display_unit_id'] in du_id_whitelist:
			main_player_status.append(player_field_report['container_name']+ "\n Player:  " + player_field_report['player_name'] + "( Checkin " + str(player_field_report['last_checkin_min']) + " min ago)" + " \n Contents playing: \n" + player_field_report['contents'])

	if player_field_report['player_id'] in critical_players:
		main_player_status.append(player_field_report['container_name']+ "\n Player:  " + player_field_report['player_name'] + "( Checkin " + str(player_field_report['last_checkin_min']) + " min ago)" + " \n Contents playing: \n" + player_field_report['contents'])

	if player_field_report['player_id'] in wifi_players:
		temp_wifi_players.append("(WIFI) "+ player_field_report['container_name']+ "\n Player:  " + player_field_report['player_name'] + " \n ( Last Connected: el " + player_field_report['last_checkin_time'] + ")") 

	if player_field_report['player_id'] in temporary_players:
		temp_wifi_players.append("(TEMPORAL) " + player_field_report['container_name']+ "\n Player:  " + player_field_report['player_name'] + "\n ( Last Connected el " + player_field_report['last_checkin_time'] + ")") 



	print("Black: ", black_players)
	print("No Screens: ", no_screens_players)
	print("Missing: ", missing_players)
	print("offline: ", offline_players)
	print("No campaigns: ", no_campaigns_players)
	

def daily_report(report_type):
	print("Sending report")

	message1= ""
	for p1 in missing_players:
		message1 = message1 + "\n\n" + p1

	message2= ""
	for p1 in offline_players:
		message2 = message2 + "\n\n" + p1

	message3= ""
	for p1 in temp_wifi_players:
		message3 = p1 + "\n\n" + message3

	message4= ""
	for p1 in main_player_status:
		message4 = message4 + "\n\n" + p1

	message5= ""
	for p1 in black_players:
		message5 = message5 + "\n\n" + p1

	message6= ""
	for p1 in no_screens_players:
		message6 = message6 + "\n\n" + p1

	message7= "Total players analizados: "+ str(len(field_report))  + ". Players descartados: " + str(len(discarded_players))
	

	server_time= datetime.now()
	server_time=server_time.astimezone(pytz.timezone('utc'))
	fecha=server_time.astimezone(pytz.timezone('Europe/Madrid')).strftime("%d %b, %Y a las %H:%M")

	resp = client.send_message(
				message={
					"to": {
						"email": email_to_send,
					},
					"template": report_template,
						"data": {
							"title": report_type,
							"fecha": fecha,
							"message1" : message1,
							"message2" : message2,
							"message3" : message3,
							"message4" : message4,
							"message5" : message5,
							"message6" : message6,
							"message7" : message7,
						},
				})
	print(resp['requestId'])

if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

if country=="SPAIN":
	container_ids=["21393898"]
	email_to_send="dept_tecnico@iwallinshop.com"
elif country=="COLOMBIA":
	container_ids=['135518539']
	email_to_send="dept_tecnico_colombia@iwallinshop.com"
elif country=="PERU":
	container_ids=['53704276']
	email_to_send="dept_tecnico_peru@iwallinshop.com"
else:
	print("Country Missing, exiting....")
	exit(1)


print("Player Status Report - IWALL")
print("Getting ", country , " player status")
print("loading player filters from db")
load_filtering_players()

print("initialising field report")
player_field_report={}
field_report=[]

valid_report, report_type= report_valid()
valid_report=True



for m in container_ids:
		
		url_field_report=url_field_report+"&parent_container_ids=" +m
		
		#print("Field Report request: ", url_field_report)
		#print(url_reservation_container)
		s=requests.get(url_field_report,headers={'Accept': 'application/json','Authorization': auth})
		data=json.loads(s.text)
		

		for n in data["field_report"]:

			fr=n['field_report']
			if fr: 
				#print(fr)
				try: 
					if re.search('Player Id:',fr):
						player_id=re.findall('Player Id: (.*)\n', fr)[0]
					elif re.search('Player Id :',fr):
						player_id=re.findall('Player Id : (.*)\n', fr)[0]
				except:
					player_id=None      
			
				if player_id:
					print("**Player ID found")
					player_field_report['device_id']= "broadsign.com:" + str(player_id)
					player_field_report['player_id']=player_id

				try: 
					if re.search('Local Time:',fr):
						local_time=re.findall('Local Time: (.*) \(', fr)[0]
					elif re.search('Local Time :',fr):
						local_time=re.findall('Local Time : (.*) \(', fr)[0]
				except:
					local_time=None      
			
				if local_time:

					if len(local_time)>21:
						local_time=local_time[:-6]
					print("**Local Time:", local_time)
					dt_localtime = datetime.strptime(local_time, "%Y-%m-%dT%H:%M:%S")
					print(dt_localtime)
					player_field_report['last_checkin_time']= dt_localtime.strftime("%d %b, %Y a las %H:%M")
					print("unware time object", dt_localtime)
					dt_localtime = dt_localtime.replace(tzinfo=pytz.timezone('Etc/GMT-1'))
					print("localized time zone", dt_localtime)
					server_time= datetime.now()
					server_time=server_time.astimezone(pytz.timezone('utc'))
					dt_localtime=dt_localtime.astimezone(pytz.UTC)
					print("hora  espain", server_time.astimezone(pytz.timezone('Europe/Madrid')).strftime("%d %b, %Y a las %H:%M"))
					
					print("UTC converted", dt_localtime)
					print("Broadsign time: ", dt_localtime, " Server time:", server_time)
					print("last check in (minutes)	: ", (server_time - dt_localtime).total_seconds()/60)
					player_field_report['last_checkin_min']= int((server_time - dt_localtime).total_seconds()/60)
					
					
				try: 
					if re.search('Started On:',fr):
						started_on=re.findall('Started On: (.*) \(', fr)[0]
					elif re.search('Started On :',fr):
						started_on=re.findall('Started On : (.*) \(', fr)[0]
				except:
					started_on=None      
			
				if started_on:
					print("**Started")
					player_field_report['started_time']= started_on
					print("Started:  ", started_on)

					server_time= datetime.now()
					server_time=server_time.astimezone(pytz.timezone('utc'))
					
					dt_localtime = datetime.strptime(started_on, "%Y-%m-%dT%H:%M:%S")
					dt_localtime=dt_localtime.replace(tzinfo=pytz.timezone('Etc/GMT-1'))
					dt_localtime=dt_localtime.astimezone(pytz.UTC)


					print("Started ( minutes)	: ", int((server_time - dt_localtime).total_seconds()/60))
					player_field_report['started_min']= int((server_time - dt_localtime).total_seconds()/60)
					

				try: 
					if re.search('Number of frames',fr):
						num_frames=re.findall('Number of frames: (.*)', fr)[0]
				except:
					num_frames=None      
			
				if num_frames:
					print("**Num_frames")
					player_field_report['num_frames']= num_frames
					print("num active frames ", num_frames)

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

				
				try: 
					disk_usg=re.findall('Disk Usage (.*)\n', fr)[0]
				except: 
					disk_usg=None
				if disk_usg:
					player_field_report['disk_usage']=disk_usg


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



				#content to play
				print("Content scheduled to play now")
				try:

						url_content_to_play_list=url_content_to_play+"ids="+player_id+"&current_day="+ datetime.now().strftime("%Y/%m/%d")
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
		
				player_field_report['contents']=content_names
				player_field_report['num_contents']=num_contents

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
			
					player_field_report['campaign_names']=campaign_names
					player_field_report['number_of_campaigns']=number_of_campaigns


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
					player_field_report['latitude']= re.search('\((.*)\,', geolocation).group(1)
					player_field_report['longitude']=re.search('\,(.*)\)', geolocation).group(1)
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
					du_container_id=0

					try:
						url_container_infos=url_container_info+"&ids="+ str(du_container)

						print(url_container_infos)
						
						s=requests.get(url_container_infos,headers={'Accept': 'application/json','Authorization': auth})
						data3=json.loads(s.text)
						print(data3)
						for m in data3["container"]:
							container_name=m['name']
							container_active = m['active']
							du_container_id=m['id']
				
					except: 

						container_name=""
						container_active=""
						du_container_id=0

					player_field_report['container_name']=container_name
					player_field_report['du_container_id']=du_container_id
					player_field_report['container_active']=container_active	
					player_field_report['programmatic_name']=country + "-" + container_name + "-" + str(player_screens) + " screens"	


				alarms_check(player_field_report)
			

				field_report.append(player_field_report)
				print("------------ \nPlayer Report:\n--------------------")
				print(player_field_report)

				player_field_report={}
				

		daily_report(report_type)

		df_field_report = pd.DataFrame(field_report)


		#df_field_report.to_sql('player_status', con = engine, if_exists = 'append', chunksize = 1000)
		df_field_report.to_csv('player_status.csv', index=False, sep= ";")

		#get DU name from player ID







