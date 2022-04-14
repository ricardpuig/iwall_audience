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
from pandasgui import show
import numpy as np



def occupancy_report(db_connection, country):

	#load all SPAIN campaigns
	sql_select_reservations= "SELECT * from reservations where country='%s'" % (country)
	df = pd.read_sql(sql_select_reservations, con=db_connection)
	print(df)

	df['schedule_start_date']=pd.to_datetime(df['schedule_start_date'], format="%Y-%m-%d")
	df['schedule_end_date']=pd.to_datetime(df['schedule_end_date'], format="%Y-%m-%d")

	#filter
	df=df[~df['name'].str.contains("FILLER ")]
	df=df[~df['name'].str.contains("LED - Gran Formato")]
	df=df[~df['name'].str.contains("_FILLER")]
	df=df[~df['name'].str.contains("CC ")]
	df=df[~df['name'].str.contains("AUTOPROMO ")]
	df=df[~df['name'].str.contains("Corporativa ")]
	df=df[~df['name'].str.contains("test ")]
	df=df[~df['name'].str.contains("TEST")]



	df=df[~df['name'].str.contains("Programmatic")]
	df=df[~df['name'].str.contains("CORTINILLA")]
	df=df[~df['mall'].str.contains("CIRCUITO CENTROS COMERCIALES SPAIN")]
	df=df[~df['mall'].str.contains("CIRCUITO CENTROS COMERCIALES PERU")]
	df=df[~df['mall'].str.contains("CIRCUITO CENTROS COMERCIALES COLOMBIA")]
	df=df[~df['mall'].str.contains("IWALL COVID VUELTA")]

	df=df[pd.notnull(df['schedule_start_date'])]
	df=df[pd.notnull(df['schedule_end_date'])]

	df['exploded date'] = df.apply(lambda s: pd.date_range(s['schedule_start_date'], s['schedule_end_date'], freq='D').tolist(), 1)
	df = df.explode('exploded date')
	df['str_saturation'] = df['saturation'].astype(str)

	df_daily_occupancy=df.groupby(['mall', 'exploded date']).agg(
                        num_campaigns=('name', 'nunique'),
                        avg_saturation = ('saturation', 'mean'),
                        campaigns=('name', lambda x: ','.join(x)),
                        saturations = ('str_saturation', lambda x: ':'.join(x))
                        )

	df_daily_occupancy['promo'] = np.where(df_daily_occupancy['campaigns'].str.contains("CC "), 1, 0)

	df_daily_occupancy.reset_index(inplace=True)
	print(df_daily_occupancy.dtypes)
	df_daily_occupancy['exploded date']=df_daily_occupancy['exploded date'].dt.strftime("%Y-%m-%d")
	df_daily_occupancy['occupancy']=df_daily_occupancy['num_campaigns']/8


	print(df_daily_occupancy.dtypes)
	df_daily_occupancy.to_sql('occupancy', con=db_connection, if_exists='replace')


	show(df_daily_occupancy)




db_connection_str = 'mysql+pymysql://root:SonaeRootMysql2021!@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/audience'
db_connection = create_engine(db_connection_str)



occupancy_report(db_connection, "SPAIN")
