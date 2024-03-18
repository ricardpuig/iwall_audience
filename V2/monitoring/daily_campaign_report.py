import requests
import json
import re
import mysql.connector
from datetime import datetime
import pandas as pd
from datetime import date
import sys
from sqlalchemy import create_engine
import pymysql
import numpy as np
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


report_template= "5NKZS4D4C54R69Q4NCNP3KPYBACD"
report_template_comercial= "PQS4244E5J45VRHDVHV6BE0JV9HS"

client = Courier(auth_token="pk_prod_6S1S6BVGGXMEB5Q8TSVHDN59NY8D")
url_reservation_by_display_unit= 'https://api.broadsign.com:10889/rest/reservation/v20/by_du_folder?domain_id=17244398&current_only=false';
url_container_info= 'https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_display_unit_info= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398';
url_container_scoped_peru= 'https://api.broadsign.com:10889/rest/container/v9/scoped?domain_id=17244398&parent_container_ids=60141576';
url_campaign_audience= 'https://api.broadsign.com:10889/rest/campaign_audience/v1/by_reservation_id?domain_id=17244398';
url_display_unit_audience= "https://api.broadsign.com:10889/rest/display_unit_audience/v1/by_reservation_id?domain_id=17244398"
url_container_id='https://api.broadsign.com:10889/rest/container/v9/by_id?domain_id=17244398';
url_reservation_by_id= 'https://api.broadsign.com:10889/rest/reservation/v20/by_id?domain_id=17244398';
url_schedule_by_reservation= 'https://api.broadsign.com:10889/rest/schedule/v8/by_reservable?domain_id=17244398';
url_display_unit_by_reservation= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_reservable?domain_id=17244398';


def daily_report(daily_report):

	message1_header= """
				<style>
					table {
						width: 100%;
						border-collapse: collapse;
					}
					th, td {
						border: 1px solid black;
						padding: 8px;
						text-align: left;
					}
					th {
						background-color: #f2f2f2;
					}
				</style>
					
				<table>
					<tr>
						<th>Empiezan hoy</th>
						<th></th>
						<th></th>
					</tr>
			"""
	message1_table="".join(
				f"<tr><td>{daily_report['num_campaigns']}</td><td></td><td></td></tr>"
	)

	message1 = ""
	
	if daily_report['num_campaigns']>0:

		message2_header= """
				<style>
						table {
							width: 100%;
							border-collapse: collapse;
						}
						th, td {
							border: 1px solid black;
							padding: 8px;
							text-align: left;
						}
						th {
							background-color: #f2f2f2;
						}
					</style>
						
					<table>
						<tr>
							<th>Campaña</th>
							<th>Display units playing/total</th>
							<th>No reportando plays</th>
						</tr>
						
				"""
		
		message2_table="".join(
					f"<tr><td>{campaign['name']}</td><td>{campaign['playing_display_units']}/{campaign['display_units']}</td><td>{campaign['error']}</td></tr>"
					for campaign in daily_report['campaigns']
		)

		message2 = message2_header + message2_table + "</table>"
	else:
		message2 = "Hoy no empiezan campañas"


	if daily_report['new_schedules']>0:
		message3_header="""
					<style>
							table {
								width: 100%;
								border-collapse: collapse;
							}
							th, td {
								border: 1px solid black;
								padding: 8px;
								text-align: left;
							}
							th {
								background-color: #f2f2f2;
							}
						</style>
							
						<table>
							<tr>
								<th>Nueva schedule</th>
								<th>Schedule</th>
								<th>Pases emitidos</th>
							</tr>
							
					"""
		try:
			message3_table="".join(
							f"<tr><td>{schedule['campaign_schedule_name']}</td><td>{schedule['campaign_new_schedule']}</td><td>{schedule['today_plays']}</td></tr>"
							for schedule in daily_report['schedules']
				)
		except: 
			message3_table="".join(
							f"<tr><td>{schedule['campaign_schedule_name']}</td><td>{schedule['campaign_new_schedule']}</td><td>No data</td></tr>"
							for schedule in daily_report['schedules']
				)

		message3 = message3_header + message3_table + "</table>"
	else:
		message3 = "Hoy no empiezan schedules"


	message4=""
	message5= "generado por IWALL IRIS"
	

	server_time= datetime.now()
	server_time=server_time.astimezone(pytz.timezone('utc'))
	fecha=server_time.astimezone(pytz.timezone('Europe/Madrid')).strftime("%d %b, %Y a las %H:%M")

	resp = client.send_message(
				message={
					"to": {
						"email": "dept_tecnico@iwallinshop.com",
					},
					"template": report_template,
						"data": {
							"fecha": fecha,
							"message1" : message1,
							"message2" : message2,
							"message3" : message3,
							"message4" : message4,
							"message5" : message5
						},
				})
	print(resp['requestId'])

	#enviar mail comercial
	resp = client.send_message(
				message={
					"to": [
						{
						"email": "comercialspain@iwallinshop.com",
						},
						{
						"email": "scano@iwallinshop.com",
						},
						{
						"email": "rpuig@iwallinshop.com",
						},
					],	
					"template": report_template_comercial,
						"data": {
							"fecha": fecha,
							"message3" : message3,
							"message5" : message5
						},
				})
	print(resp['requestId'])




def daily_campaign_analysis(db_connection, country, df_campaigns):

	#report general
	today_campaigns_report={}
	today_campaigns_details=[]

	#load all SPAIN campaigns
	#sql_select_reservations= "SELECT * from broadsign_reservations where country='%s' " % (country)
	#df = pd.read_sql(sql_select_reservations, con=db_connection)

	df=df_campaigns
	
	#df['schedule_start_date']=pd.to_datetime(df['schedule_start_date'], format="%Y-%m-%d")
	#df['schedule_end_date']=pd.to_datetime(df['schedule_end_date'], format="%Y-%m-%d")
	
	fecha_actual= date.today()
	print("Fecha Actual: ", fecha_actual)
	today_campaigns_report['fecha']=fecha_actual.strftime("%Y-%m-%d")

	print(df)
	#campañas que inician hoy
	df_hoy = df[df['start_date'] == fecha_actual.strftime("%Y-%m-%d")]

	print(df_hoy)

	# Iterar por cada fila
	today_campaigns_id=df_hoy['campaign_id'].unique()
	print(today_campaigns_id)

	today_campaigns_report['num_campaigns']=len(today_campaigns_id)
	
	#Authorization
	auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";

	for c_id in today_campaigns_id:

		campaign_details={}
		print("\n ")
		print("Analyzing reservation ID  " + str(c_id))

		print("-------------------------------------")
		print("   Campaign Details:")
		print("-------------------------------------")

		reservation={}
		url_reservation_by_id= 'https://api.broadsign.com:10889/rest/reservation/v20/by_id?domain_id=17244398';
		url_reservation_by_id=url_reservation_by_id+"&ids=" +str(c_id);
		s=requests.get(url_reservation_by_id,headers={'Accept': 'application/json','Authorization': auth});
		data=json.loads(s.text)

		for n in data["reservation"]:

			#reservation["name"]=str(n["name"].encode('utf-8', errors ='ignore'))
			reservation["name"]=n["name"]
			reservation["saturation"]=str(n["saturation"])
			reservation["duration_msec"]=str(n["duration_msec"])
			reservation["start_time"]=str(n["start_time"])
			reservation["start_date"]=str(n["start_date"])
			fecha_inicio=datetime.strptime(str(n["start_date"]),"%Y-%m-%d")
			fecha_fin=datetime.strptime(str(n["end_date"]),"%Y-%m-%d")
	
			delta=fecha_fin-fecha_inicio
			reservation["days"]=0
			reservation["days"]=delta.days+1
			reservation["end_time"]=str(n["end_time"])
			reservation["end_date"]=str(n["end_date"])


		campaign_details['name']=reservation["name"]
		campaign_details['error']=""

		print("-------------------------------------")
		print("   Campaign Daily Performance:")
		print("-------------------------------------")
		url_campaign_performance='https://api.broadsign.com:10889/rest/campaign_performance/v6/by_reservable_id?domain_id=17244398';
		
		campaign_daily_performance={}
		campaign_display_unit_performance={}
		
		url_campaign_performance=url_campaign_performance+"&reservable_id=" +str(c_id);
		s=requests.get(url_campaign_performance,headers={'Accept': 'application/json','Authorization': auth});
		data=json.loads(s.text)

		campaign_days=[] #delete all days
		repetition_distribution = []  #reset distribution of repetitions

		#analyze data
		for n in data["campaign_performance"]:
			campaign_daily_performance["total_impressions"]=n["total_impressions"]
			campaign_daily_performance["played_on"]=str(n["played_on"])
			campaign_daily_performance["repetitions"]=n["total"]           
			campaign_daily_performance["reservation_id"]=n["reservable_id"]
			campaign_days.append(str(n["played_on"]));  #add days to array
			print(str(n["played_on"]) + " Repetitions: " + str(n["total"]) )

		campaign_details['daily_performance']=campaign_daily_performance

		#list display units for the campaign
		print("------------------------------------")
		print("      Display Unit in campaigns: ")
		print("-------------------------------------")

		url_display_unit_by_reservation= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_reservable?';
		url_display_unit_by_reservation=url_display_unit_by_reservation+"reservable_id=" +str(c_id)+"&active_filter=true";
		#print url_display_unit_performance
		s=requests.get(url_display_unit_by_reservation,headers={'Accept': 'application/json','Authorization': auth});
		data=json.loads(s.text)
		#print(json.dumps(data, indent=4))
		live_display_units=0
		live_display_units_id= []
		for du in data["display_unit"]:
			if du['host_screen_count']>0:
				live_display_units= live_display_units+1
				live_display_units_id.append(du['id'])


		print("Total display units in campaign with screens attached: ", live_display_units)
		campaign_details['display_units']=live_display_units

		print("")
		print("-------------------------------------")
		print("      Display Unit Performance: ")
		print("-------------------------------------")
		url_display_unit_performance="https://api.broadsign.com:10889/rest/display_unit_performance/v5/by_reservable_id?domain_id=17244398"
		url_display_unit_performance=url_display_unit_performance+"&reservable_id=" +str(c_id);
		s=requests.get(url_display_unit_performance,headers={'Accept': 'application/json','Authorization': auth});
		data=json.loads(s.text)
		#print(json.dumps(data, indent=4))

		print("Display units playing: "+ str(len(data["display_unit_performance"])))

		#checking if all display units are reporting plays 
		campaign_performance=[]

		if len(data["display_unit_performance"])< campaign_details['display_units']:
			print("not all du showing plays")
			campaign_details['error']= ""

		campaign_details['playing_display_units']=len(data["display_unit_performance"])


		for n in data["display_unit_performance"]:

			campaign_display_unit_performance={}

			campaign_display_unit_performance["repetitions"]=n["total"]
			campaign_display_unit_performance["display_unit_id"]=n["display_unit_id"]
			campaign_display_unit_performance['error']=""

			print("Plays for DU ",n["display_unit_id"], ": ", n["total"] )

			if n["total"]==0:
				#get displa unit that is no generating plays
				#get mall name from this display unit
				url_display_unit_info= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398';
				url_display_unit_info=url_display_unit_info+"&ids=" +str(n["display_unit_id"]);
				s=requests.get(url_display_unit_info,headers={'Accept': 'application/json','Authorization': auth});
				data_name=json.loads(s.text)

				for m in data_name["display_unit"]:
						
						if m["host_screen_count"] >0 :
							campaign_display_unit_performance['error']=str(m["name"].encode('utf-8', errors ='ignore'))
							print(" DU with no plays: ", campaign_display_unit_performance["display_unit_name"] )
							campaign_details['error']=campaign_details['error'] + ", "+ campaign_display_unit_performance["display_unit_name"]
						else:
							print("Du with no screens")
			else:
				try:
					live_display_units_id.remove(n["display_unit_id"])
				except: 
					print("display unit not in reservation")
					
			campaign_performance.append(campaign_display_unit_performance)

		#error with remaining display units
		
		print("Display units not showing plays: ", live_display_units_id)
		for du in live_display_units_id:
			url_display_unit_info= 'https://api.broadsign.com:10889/rest/display_unit/v12/by_id?domain_id=17244398';
			url_display_unit_info=url_display_unit_info+"&ids=" +str(du);
			s=requests.get(url_display_unit_info,headers={'Accept': 'application/json','Authorization': auth});
			data_name=json.loads(s.text)
			for m in data_name["display_unit"]:
				campaign_details['error']= campaign_details['error'] + 	m["name"] +" * "


		campaign_details['du_performance']=campaign_performance
		today_campaigns_details.append(campaign_details)

	today_campaigns_report['campaigns']=today_campaigns_details

	#check running campaigns
	print("Checking running campaigns")
	running_campaigns_report={}
	running_campaigns_details=[]

	df['start_date']=pd.to_datetime(df['start_date'], format="%Y-%m-%d")
	df['end_date']=pd.to_datetime(df['end_date'], format="%Y-%m-%d")

	fecha_actual = datetime.now()
	
	df_running = df[(df['start_date'] <= fecha_actual) & (df['end_date'] >= fecha_actual)]
	running_campaigns_id=df_running['campaign_id'].unique()

	new_schedules = 0

	for c_id in running_campaigns_id:

		reservation={}
		url_reservation_by_id= 'https://api.broadsign.com:10889/rest/reservation/v20/by_id?domain_id=17244398';
		url_reservation_by_id=url_reservation_by_id+"&ids=" +str(c_id);
		s=requests.get(url_reservation_by_id,headers={'Accept': 'application/json','Authorization': auth});
		data=json.loads(s.text)

		
		for n in data["reservation"]:

			#reservation["name"]=str(n["name"].encode('utf-8', errors ='ignore'))
			reservation["name"]=n["name"]
			#print(" Running campaign: ", reservation["name"])
			if "$" in reservation["name"]:
				#get all schedules and check each 
				url_schedules_by_id= 'https://api.broadsign.com:10889/rest/schedule/v8/by_reservable?domain_id=17244398';
				url_schedules_by_id=url_schedules_by_id+"&id=" +str(c_id);
				s=requests.get(url_schedules_by_id,headers={'Accept': 'application/json','Authorization': auth});
				data=json.loads(s.text)
				#print(json.dumps(data, indent=4))
				for s in data["schedule"]:
					running_campaigns_report={}
					if s['start_date']==date.today().strftime("%Y-%m-%d"):
						
						print("Schedule empieza hoy ",reservation["name"] )
						running_campaigns_report['campaign_new_schedule']=s["name"]
						running_campaigns_report['campaign_schedule_name']=reservation["name"]
	
						new_schedules= new_schedules +1
						
						print("Checking campaign Daily Performance...")
						url_campaign_performance='https://api.broadsign.com:10889/rest/campaign_performance/v6/by_reservable_id?domain_id=17244398';
						
						campaign_daily_performance={}
						campaign_display_unit_performance={}
						
						url_campaign_performance=url_campaign_performance+"&reservable_id=" +str(c_id);
						s=requests.get(url_campaign_performance,headers={'Accept': 'application/json','Authorization': auth});
						data=json.loads(s.text)
						#print(json.dumps(data, indent=4))
						running_campaigns_report['today_plays']= 0
						for p in data["campaign_performance"]:
							if str(p["played_on"]) == date.today().strftime("%Y-%m-%d"):
								running_campaigns_report['today_plays']=p["total"]
							

						running_campaigns_details.append(running_campaigns_report)
						running_campaigns_report={}

										
	today_campaigns_report['new_schedules']= new_schedules
	today_campaigns_report['schedules']=running_campaigns_details
		
	print(json.dumps(today_campaigns_report,indent=4))
	daily_report(today_campaigns_report)

def extract_current_campaigns():

	start_date= "2023-01-01"
	end_date= "2024-12-31"
	auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";

	url_reservation_by_display_unit= 'https://api.broadsign.com:10889/rest/reservation/v20/by_du_folder?domain_id=17244398&current_only=false&start_date='+start_date+'&end_date='+end_date;
	url_reservation_container=url_reservation_by_display_unit+"&container_ids=106135296";
	s=requests.get(url_reservation_container,headers={'Accept': 'application/json','Authorization': auth});
	data=json.loads(s.text)

	container_ids=[]
	container_name=[]
	reservation={}
	malls={}
	reservations=[]

	print("analyzing reservations")
	for n in data["reservation"]:
			
			insert=1
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
			reservation["name"]=n["name"]
			reservation["country"]="SPAIN"
			#reservation["last_updated"]=datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
			name=n["name"]
			if re.findall('\$(.*)\$',name):
					reservation["SAP_ID"]=re.findall('\$(.*)\$', name)[0]
			else:
					reservation["SAP_ID"]="not found"
			schedule_start_date=""
			schedule_end_date=""
			schedule_days=0

			reservations.append(reservation)
			reservation={}
	
	df_reservations= pd.DataFrame(reservations)
	print(df_reservations)

	print(list(df_reservations))
	return(df_reservations)
		
# -------------------------------------------------------------------------
print("Launching daily campaign analysis")

db_connection_str = 'mysql+pymysql://root:SonaeRootMysql2021!@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/audience'
db_connection = create_engine(db_connection_str)

#extract running campaigns
df_campaigns=extract_current_campaigns()

#launch today report
daily_campaign_analysis(db_connection, "SPAIN", df_campaigns)
