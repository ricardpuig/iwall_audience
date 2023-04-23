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



def lambda_occupancy(x):  
	campaigns=x['campaigns']
	saturations=x['saturations']
	campaign_list=campaigns.split("|")
	saturation_list=saturations.split("|")
	occupancy = 0
	
	for idx_sat, sat in enumerate(saturation_list):
			if "HIVESTACK" in campaign_list[idx_sat]: 
				continue
			if "REACH" in campaign_list[idx_sat]: 
				continue
			if "PIXEL" in campaign_list[idx_sat]: 
				continue
			if "TEST" in campaign_list[idx_sat]: 
				continue
			if "Unity-trigger" in campaign_list[idx_sat]: 
				continue

			if 1/float(sat)<0:
				occupancy = occupancy+(1/abs(float(sat)))
			else:
				occupancy = occupancy + abs(float(sat))		
	return(occupancy)

def lambda_anunciantes(x):  
	campaigns=x['campaigns']
	saturations=x['saturations']
	campaign_list=campaigns.split("|")
	saturation_list=saturations.split("|")
	anunciantes = 0
	
	for idx_sat, sat in enumerate(saturation_list):
			if "HIVESTACK" in campaign_list[idx_sat]: 
				continue
			if "REACH" in campaign_list[idx_sat]: 
				continue
			if "PIXEL" in campaign_list[idx_sat]: 
				continue
			if "TEST" in campaign_list[idx_sat]: 
				continue
			if "Unity-trigger" in campaign_list[idx_sat]: 
				continue
			anunciantes= anunciantes + 1

	return(anunciantes)


def lambda_programmatic(x):  
	campaigns=x['campaigns']
	saturations=x['saturations']
	campaign_list=campaigns.split("|")
	saturation_list=saturations.split("|")
	programmatic = 0
	
	for idx_sat, sat in enumerate(saturation_list):
			if "HIVESTACK" in campaign_list[idx_sat]: 
				if 1/float(sat)<0:
					programmatic = programmatic+(1/abs(float(sat)))
				else:
					programmatic = programmatic + abs(float(sat))		
			if "REACH" in campaign_list[idx_sat]: 
				if 1/float(sat)<0:
					programmatic = programmatic+(1/abs(float(sat)))
				else:
					programmatic = programmatic + abs(float(sat))		

			if "PIXEL" in campaign_list[idx_sat]: 
				continue
			if "TEST" in campaign_list[idx_sat]: 
				continue
			if "Unity-trigger" in campaign_list[idx_sat]: 
				continue

	return(programmatic)





def occupancy_report(db_connection, country):

	#load all SPAIN campaigns
	sql_select_reservations= "SELECT * from broadsign_reservations where country='%s'" % (country)
	df = pd.read_sql(sql_select_reservations, con=db_connection)
	print(df)

	df['schedule_start_date']=pd.to_datetime(df['schedule_start_date'], format="%Y-%m-%d")
	df['schedule_end_date']=pd.to_datetime(df['schedule_end_date'], format="%Y-%m-%d")

	#filter
	'''
	df=df[~df['name'].str.contains("FILLER ")]
	df=df[~df['name'].str.contains("LED - Gran Formato")]
	df=df[~df['name'].str.contains("_FILLER")]
	df=df[~df['name'].str.contains("CC ")]
	df=df[~df['name'].str.contains("AUTOPROMO ")]
	df=df[~df['name'].str.contains("Corporativa ")]
	df=df[~df['name'].str.contains("test ")]
	df=df[~df['name'].str.contains("TEST")]
	df=df[~df['name'].str.contains("ALCORES")]
	df=df[~df['name'].str.contains("ALBACENTER")]
	df=df[~df['name'].str.contains("Unity")]
	df=df[~df['name'].str.contains("AS TERMAS")]
	df=df[~df['name'].str.contains("EL PASEO")]
	df=df[~df['name'].str.contains("EL ROSAL")]
	df=df[~df['name'].str.contains("AS TERMAS")]
	df=df[~df['name'].str.contains("FINESTRELLES")]
	df=df[~df['name'].str.contains("ESPACIO LEON")]
	df=df[~df['name'].str.contains("GP2 Anual")]
	df=df[~df['name'].str.contains("GVV ANUAL")]
	df=df[~df['name'].str.contains("GRAN VIA 2 ANUAL")]
	df=df[~df['name'].str.contains("DIAGONAL corp")]
	df=df[~df['name'].str.contains("CAM AIRE SIN VIRUS")]
	df=df[~df['name'].str.contains("ANUAL")]
	df=df[~df['name'].str.contains("anual")]

	df=df[~df['name'].str.contains("Programmatic")]
	df=df[~df['name'].str.contains("PROGRAMMATIC")]
	df=df[~df['name'].str.contains("CORTINILLA")]
	df=df[~df['mall'].str.contains("CIRCUITO CENTROS COMERCIALES SPAIN")]
	df=df[~df['mall'].str.contains("CIRCUITO CENTROS COMERCIALES PERU")]
	df=df[~df['mall'].str.contains("CIRCUITO CENTROS COMERCIALES COLOMBIA")]
	df=df[~df['mall'].str.contains("IWALL COVID VUELTA")]
	'''


	df=df[pd.notnull(df['schedule_start_date'])]
	df=df[pd.notnull(df['schedule_end_date'])]

	df['exploded date'] = df.apply(lambda s: pd.date_range(s['schedule_start_date'], s['schedule_end_date'], freq='D').tolist(), 1)
	df = df.explode('exploded date')
	df['str_saturation'] = df['saturation'].astype(str)

	print(df)
	


	df_daily_occupancy=df.groupby(['mall', 'exploded date']).agg(
                        num_campaigns=('name', 'nunique'),
                        avg_saturation = ('saturation', 'mean'),
                        campaigns=('name', lambda x: '|'.join(x)),
                        saturations = ('str_saturation', lambda x: '|'.join(x))
                        )

	df_daily_occupancy['promo'] = np.where(df_daily_occupancy['campaigns'].str.contains("CC "), 1, 0)
	df_daily_occupancy['iwall_pixel'] = np.where(df_daily_occupancy['campaigns'].str.contains("PIXEL"), 1, 0)
	df_daily_occupancy['hivestack'] = np.where(df_daily_occupancy['campaigns'].str.contains("HIVESTACK"), 1, 0)
	df_daily_occupancy['reach'] = np.where(df_daily_occupancy['campaigns'].str.contains("REACH"), 1, 0)
	df_daily_occupancy['programmatic'] = np.where(df_daily_occupancy['campaigns'].str.contains(""), 1, 0)
	


	

	df_daily_occupancy.reset_index(inplace=True)
	print(df_daily_occupancy.dtypes)
	df_daily_occupancy['exploded date']=df_daily_occupancy['exploded date'].dt.strftime("%Y-%m-%d")

	#calculate occupancy based on saturation and number of campaigns
	df_daily_occupancy['occupancy']=df_daily_occupancy.apply(lambda_occupancy,axis=1)
	df_daily_occupancy['num_campaigns']=df_daily_occupancy.apply(lambda_anunciantes,axis=1)
	df_daily_occupancy['programmatic']=df_daily_occupancy.apply(lambda_programmatic,axis=1)


	print(df_daily_occupancy.dtypes)
	df_daily_occupancy.to_sql('occupancy', con=db_connection, if_exists='replace')


db_connection_str = 'mysql+pymysql://root:SonaeRootMysql2021!@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/audience'
db_connection = create_engine(db_connection_str)

occupancy_report(db_connection, "SPAIN")
