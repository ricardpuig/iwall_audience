import requests
import json
import re
import mysql.connector
import pandas as pd
import numpy as np
import logging as log
from datetime import datetime, timedelta
from datetime import date
from sqlalchemy import create_engine
from datetime import datetime, timedelta

import numpy as np
import pandas as pd




host = "ec2-52-18-248-109.eu-west-1.compute.amazonaws.com"
user = "root"
passwd = "sonaeRootMysql2017"
db = "audience"
port = 3306

#connect to mysql audience database
"mysql+pymysql://sylvain:passwd@127.0.0.1/db?host=localhost?port=3306"
    # construct an engine connection string
engine_string = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user = user,
        password = passwd,
        host = host,
        port = port,
        db = db,
)
# create sqlalchemy engine
engine = create_engine(engine_string)
  

db = "seeux"
host = "seeux-test-database.chlgafah2kmy.eu-west-1.rds.amazonaws.com"
user = "postgres"
passwd = "1w4LLinshop"
port = 5432

#conenct to postgre sensors database
engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
        user = user,
        password = passwd,
        host = host,
        port = port,
        db = db,
    )
    # create sqlalchemy engine
engine2 = create_engine(engine_string)


#load mall data from audience DB 
query = "SELECT * from malls"
df_malls = pd.read_sql_query(query, engine)
print(df_malls)

malls=df_malls['id'].to_list()
print(df_malls)
print(malls)

#for each mall compute audience model 
#loop thorugh malls


engine.execute("DELETE FROM audience_segments WHERE datetime LIKE \'2021%%\'")
engine.execute("DELETE FROM audience_impressions WHERE date LIKE \'2021%%\'")

for m in malls: 

    df_mall=df_malls.loc[df_malls['id'] == m]
    malls_info=df_mall.to_dict('records')
    mall=malls_info[0]

    #get default model 
    query = "SELECT * from mall_default_models  WHERE mall_type = {type}".format(type=mall['mall_model_type'])
    df_model = pd.read_sql_query(query, engine)
    

    #get data updated data from cameras
    model_info=df_model.to_dict('records')
    model=model_info[0]

    #build default audience data for the current year

    #dates create 
    begin_date = '2020-01-01'
    end_date= '2021-12-31'

    df_audience_impressions=pd.DataFrame({'mall_id' : mall['id'], 'date':pd.date_range(start=begin_date, end=end_date)})
    df_audience_segments=pd.DataFrame({'mall_id' : mall['id'], 'date':pd.date_range(start=begin_date, end=end_date)})
    

    #get the overall multiplier from screen types
    num_screens=mall['screens']
    default_screen_impressions=mall['default_screen_day_impressions']
    type_deviation=mall['screen_type_deviation']


    overall_daily=(mall['screens_type1']*default_screen_impressions*type_deviation) + \
        (mall['screens_type2']*default_screen_impressions) + \
        (mall['screens_type3']*default_screen_impressions*(1+type_deviation))



    #do hourly population
    hourly=model['hourly'].split(':')
    hourly=list(np.float_(hourly))


    weekday=model['weekday'].split(':')
    weekday=list(np.float_(weekday))


    weekly=model['weekly'].split(':')
    weekly=list(np.float_(weekly))

    df_audience_impressions['total_impressions']=overall_daily

    #do weekday adjustements
    df_audience_impressions['date']=pd.to_datetime(df_audience_impressions['date'], format="%Y-%m-%d")
    df_audience_impressions=df_audience_impressions.set_index('date')
    df_audience_impressions['weekday']=df_audience_impressions.index.weekday
    df_audience_impressions['week']=df_audience_impressions.index.week


    df_audience_impressions['weekday_multiplier']=1.0
    df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 0, weekday[0], df_audience_impressions['weekday_multiplier'])
    df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 1, weekday[1], df_audience_impressions['weekday_multiplier'])
    df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 2, weekday[2], df_audience_impressions['weekday_multiplier'])
    df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 3, weekday[3], df_audience_impressions['weekday_multiplier'])
    df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 4, weekday[4], df_audience_impressions['weekday_multiplier'])
    df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 5, weekday[5], df_audience_impressions['weekday_multiplier'])
    df_audience_impressions['weekday_multiplier']=np.where(df_audience_impressions['weekday'] == 6, weekday[6], df_audience_impressions['weekday_multiplier'])

    df_audience_impressions['weekly_multiplier']=1.0

    i = 1
    while i < 53:
     df_audience_impressions['weekly_multiplier']=np.where(df_audience_impressions['week'] == i, weekly[i-1], df_audience_impressions['weekly_multiplier'])
     i += 1

    df_audience_impressions['total_impressions']=df_audience_impressions['total_impressions']*df_audience_impressions['weekday_multiplier']
    df_audience_impressions['total_impressions']=df_audience_impressions['total_impressions']*df_audience_impressions['weekly_multiplier']


    df_audience_impressions['impressions_00']=df_audience_impressions['total_impressions']*hourly[0]
    df_audience_impressions['impressions_01']=df_audience_impressions['total_impressions']*hourly[1]
    df_audience_impressions['impressions_02']=df_audience_impressions['total_impressions']*hourly[2]
    df_audience_impressions['impressions_03']=df_audience_impressions['total_impressions']*hourly[3]
    df_audience_impressions['impressions_04']=df_audience_impressions['total_impressions']*hourly[4]
    df_audience_impressions['impressions_05']=df_audience_impressions['total_impressions']*hourly[5]
    df_audience_impressions['impressions_06']=df_audience_impressions['total_impressions']*hourly[6]
    df_audience_impressions['impressions_07']=df_audience_impressions['total_impressions']*hourly[7]
    df_audience_impressions['impressions_08']=df_audience_impressions['total_impressions']*hourly[8]
    df_audience_impressions['impressions_09']=df_audience_impressions['total_impressions']*hourly[9]
    df_audience_impressions['impressions_10']=df_audience_impressions['total_impressions']*hourly[10]
    df_audience_impressions['impressions_11']=df_audience_impressions['total_impressions']*hourly[11]
    df_audience_impressions['impressions_12']=df_audience_impressions['total_impressions']*hourly[12]
    df_audience_impressions['impressions_13']=df_audience_impressions['total_impressions']*hourly[13]
    df_audience_impressions['impressions_14']=df_audience_impressions['total_impressions']*hourly[14]
    df_audience_impressions['impressions_15']=df_audience_impressions['total_impressions']*hourly[15]
    df_audience_impressions['impressions_16']=df_audience_impressions['total_impressions']*hourly[16]
    df_audience_impressions['impressions_17']=df_audience_impressions['total_impressions']*hourly[17]
    df_audience_impressions['impressions_18']=df_audience_impressions['total_impressions']*hourly[18]
    df_audience_impressions['impressions_19']=df_audience_impressions['total_impressions']*hourly[19]
    df_audience_impressions['impressions_20']=df_audience_impressions['total_impressions']*hourly[20]
    df_audience_impressions['impressions_21']=df_audience_impressions['total_impressions']*hourly[21]
    df_audience_impressions['impressions_22']=df_audience_impressions['total_impressions']*hourly[22]
    df_audience_impressions['impressions_23']=df_audience_impressions['total_impressions']*hourly[23]

    #print(df_audience_impressions)


    columns = ['weekday', 'week', 'weekday_multiplier', 'weekly_multiplier']
    df_audience_impressions.drop(columns, inplace=True, axis=1)

    #print(df_audience_impressions)

    df_audience_impressions=df_audience_impressions.reset_index()
    df_audience_impressions['date']=df_audience_impressions['date'].dt.strftime('%Y-%m-%d')


    #update data
    df_audience_impressions.to_sql('audience_impressions', engine, if_exists='append', index=False)



    #get audience data from cameras to adjust 




    #SEGMENTS DATA

    df_audience_segments_male=df_audience_segments.copy()
    df_audience_segments_female=df_audience_segments.copy()
    df_audience_segments_kid=df_audience_segments.copy()
    df_audience_segments_young=df_audience_segments.copy()
    df_audience_segments_adult=df_audience_segments.copy()
    df_audience_segments_senior=df_audience_segments.copy()

    #male
    df_audience_segments_male['target_id']=35

    df_audience_segments_male['concentration_00']=mall['default_dem_male']
    df_audience_segments_male['concentration_01']=mall['default_dem_male']
    df_audience_segments_male['concentration_02']=mall['default_dem_male']
    df_audience_segments_male['concentration_03']=mall['default_dem_male']
    df_audience_segments_male['concentration_04']=mall['default_dem_male']
    df_audience_segments_male['concentration_05']=mall['default_dem_male']
    df_audience_segments_male['concentration_06']=mall['default_dem_male']
    df_audience_segments_male['concentration_07']=mall['default_dem_male']
    df_audience_segments_male['concentration_08']=mall['default_dem_male']
    df_audience_segments_male['concentration_09']=mall['default_dem_male']
    df_audience_segments_male['concentration_10']=mall['default_dem_male']
    df_audience_segments_male['concentration_11']=mall['default_dem_male']
    df_audience_segments_male['concentration_12']=mall['default_dem_male']
    df_audience_segments_male['concentration_13']=mall['default_dem_male']
    df_audience_segments_male['concentration_14']=mall['default_dem_male']
    df_audience_segments_male['concentration_15']=mall['default_dem_male']
    df_audience_segments_male['concentration_16']=mall['default_dem_male']
    df_audience_segments_male['concentration_17']=mall['default_dem_male']
    df_audience_segments_male['concentration_18']=mall['default_dem_male']
    df_audience_segments_male['concentration_19']=mall['default_dem_male']
    df_audience_segments_male['concentration_20']=mall['default_dem_male']
    df_audience_segments_male['concentration_21']=mall['default_dem_male']
    df_audience_segments_male['concentration_22']=mall['default_dem_male']
    df_audience_segments_male['concentration_23']=mall['default_dem_male']



    df_audience_segments_female['target_id']=36

    df_audience_segments_female['concentration_00']=mall['default_dem_female']
    df_audience_segments_female['concentration_01']=mall['default_dem_female']
    df_audience_segments_female['concentration_02']=mall['default_dem_female']
    df_audience_segments_female['concentration_03']=mall['default_dem_female']
    df_audience_segments_female['concentration_04']=mall['default_dem_female']
    df_audience_segments_female['concentration_05']=mall['default_dem_female']
    df_audience_segments_female['concentration_06']=mall['default_dem_female']
    df_audience_segments_female['concentration_07']=mall['default_dem_female']
    df_audience_segments_female['concentration_08']=mall['default_dem_female']
    df_audience_segments_female['concentration_09']=mall['default_dem_female']
    df_audience_segments_female['concentration_10']=mall['default_dem_female']
    df_audience_segments_female['concentration_11']=mall['default_dem_female']
    df_audience_segments_female['concentration_12']=mall['default_dem_female']
    df_audience_segments_female['concentration_13']=mall['default_dem_female']
    df_audience_segments_female['concentration_14']=mall['default_dem_female']
    df_audience_segments_female['concentration_15']=mall['default_dem_female']
    df_audience_segments_female['concentration_16']=mall['default_dem_female']
    df_audience_segments_female['concentration_17']=mall['default_dem_female']
    df_audience_segments_female['concentration_18']=mall['default_dem_female']
    df_audience_segments_female['concentration_19']=mall['default_dem_female']
    df_audience_segments_female['concentration_20']=mall['default_dem_female']
    df_audience_segments_female['concentration_21']=mall['default_dem_female']
    df_audience_segments_female['concentration_22']=mall['default_dem_female']
    df_audience_segments_female['concentration_23']=mall['default_dem_female']


    df_audience_segments_kid['target_id']=24

    df_audience_segments_kid['concentration_00']=mall['default_age_kid']
    df_audience_segments_kid['concentration_01']=mall['default_age_kid']
    df_audience_segments_kid['concentration_02']=mall['default_age_kid']
    df_audience_segments_kid['concentration_03']=mall['default_age_kid']
    df_audience_segments_kid['concentration_04']=mall['default_age_kid']
    df_audience_segments_kid['concentration_05']=mall['default_age_kid']
    df_audience_segments_kid['concentration_06']=mall['default_age_kid']
    df_audience_segments_kid['concentration_07']=mall['default_age_kid']
    df_audience_segments_kid['concentration_08']=mall['default_age_kid']
    df_audience_segments_kid['concentration_09']=mall['default_age_kid']
    df_audience_segments_kid['concentration_10']=mall['default_age_kid']
    df_audience_segments_kid['concentration_11']=mall['default_age_kid']
    df_audience_segments_kid['concentration_12']=mall['default_age_kid']
    df_audience_segments_kid['concentration_13']=mall['default_age_kid']
    df_audience_segments_kid['concentration_14']=mall['default_age_kid']
    df_audience_segments_kid['concentration_15']=mall['default_age_kid']
    df_audience_segments_kid['concentration_16']=mall['default_age_kid']
    df_audience_segments_kid['concentration_17']=mall['default_age_kid']
    df_audience_segments_kid['concentration_18']=mall['default_age_kid']
    df_audience_segments_kid['concentration_19']=mall['default_age_kid']
    df_audience_segments_kid['concentration_20']=mall['default_age_kid']
    df_audience_segments_kid['concentration_21']=mall['default_age_kid']
    df_audience_segments_kid['concentration_22']=mall['default_age_kid']
    df_audience_segments_kid['concentration_23']=mall['default_age_kid']


    df_audience_segments_young['target_id']=25

    df_audience_segments_young['concentration_00']=mall['default_age_young']
    df_audience_segments_young['concentration_01']=mall['default_age_young']
    df_audience_segments_young['concentration_02']=mall['default_age_young']
    df_audience_segments_young['concentration_03']=mall['default_age_young']
    df_audience_segments_young['concentration_04']=mall['default_age_young']
    df_audience_segments_young['concentration_05']=mall['default_age_young']
    df_audience_segments_young['concentration_06']=mall['default_age_young']
    df_audience_segments_young['concentration_07']=mall['default_age_young']
    df_audience_segments_young['concentration_08']=mall['default_age_young']
    df_audience_segments_young['concentration_09']=mall['default_age_young']
    df_audience_segments_young['concentration_10']=mall['default_age_young']
    df_audience_segments_young['concentration_11']=mall['default_age_young']
    df_audience_segments_young['concentration_12']=mall['default_age_young']
    df_audience_segments_young['concentration_13']=mall['default_age_young']
    df_audience_segments_young['concentration_14']=mall['default_age_young']
    df_audience_segments_young['concentration_15']=mall['default_age_young']
    df_audience_segments_young['concentration_16']=mall['default_age_young']
    df_audience_segments_young['concentration_17']=mall['default_age_young']
    df_audience_segments_young['concentration_18']=mall['default_age_young']
    df_audience_segments_young['concentration_19']=mall['default_age_young']
    df_audience_segments_young['concentration_20']=mall['default_age_young']
    df_audience_segments_young['concentration_21']=mall['default_age_young']
    df_audience_segments_young['concentration_22']=mall['default_age_young']
    df_audience_segments_young['concentration_23']=mall['default_age_young']


    df_audience_segments_adult['target_id']=26

    df_audience_segments_adult['concentration_00']=mall['default_age_adult']
    df_audience_segments_adult['concentration_01']=mall['default_age_adult']
    df_audience_segments_adult['concentration_02']=mall['default_age_adult']
    df_audience_segments_adult['concentration_03']=mall['default_age_adult']
    df_audience_segments_adult['concentration_04']=mall['default_age_adult']
    df_audience_segments_adult['concentration_05']=mall['default_age_adult']
    df_audience_segments_adult['concentration_06']=mall['default_age_adult']
    df_audience_segments_adult['concentration_07']=mall['default_age_adult']
    df_audience_segments_adult['concentration_08']=mall['default_age_adult']
    df_audience_segments_adult['concentration_09']=mall['default_age_adult']
    df_audience_segments_adult['concentration_10']=mall['default_age_adult']
    df_audience_segments_adult['concentration_11']=mall['default_age_adult']
    df_audience_segments_adult['concentration_12']=mall['default_age_adult']
    df_audience_segments_adult['concentration_13']=mall['default_age_adult']
    df_audience_segments_adult['concentration_14']=mall['default_age_adult']
    df_audience_segments_adult['concentration_15']=mall['default_age_adult']
    df_audience_segments_adult['concentration_16']=mall['default_age_adult']
    df_audience_segments_adult['concentration_17']=mall['default_age_adult']
    df_audience_segments_adult['concentration_18']=mall['default_age_adult']
    df_audience_segments_adult['concentration_19']=mall['default_age_adult']
    df_audience_segments_adult['concentration_20']=mall['default_age_adult']
    df_audience_segments_adult['concentration_21']=mall['default_age_adult']
    df_audience_segments_adult['concentration_22']=mall['default_age_adult']
    df_audience_segments_adult['concentration_23']=mall['default_age_adult']

    df_audience_segments_senior['target_id']=27

    df_audience_segments_senior['concentration_00']=mall['default_age_senior']
    df_audience_segments_senior['concentration_01']=mall['default_age_senior']
    df_audience_segments_senior['concentration_02']=mall['default_age_senior']
    df_audience_segments_senior['concentration_03']=mall['default_age_senior']
    df_audience_segments_senior['concentration_04']=mall['default_age_senior']
    df_audience_segments_senior['concentration_05']=mall['default_age_senior']
    df_audience_segments_senior['concentration_06']=mall['default_age_senior']
    df_audience_segments_senior['concentration_07']=mall['default_age_senior']
    df_audience_segments_senior['concentration_08']=mall['default_age_senior']
    df_audience_segments_senior['concentration_09']=mall['default_age_senior']
    df_audience_segments_senior['concentration_10']=mall['default_age_senior']
    df_audience_segments_senior['concentration_11']=mall['default_age_senior']
    df_audience_segments_senior['concentration_12']=mall['default_age_senior']
    df_audience_segments_senior['concentration_13']=mall['default_age_senior']
    df_audience_segments_senior['concentration_14']=mall['default_age_senior']
    df_audience_segments_senior['concentration_15']=mall['default_age_senior']
    df_audience_segments_senior['concentration_16']=mall['default_age_senior']
    df_audience_segments_senior['concentration_17']=mall['default_age_senior']
    df_audience_segments_senior['concentration_18']=mall['default_age_senior']
    df_audience_segments_senior['concentration_19']=mall['default_age_senior']
    df_audience_segments_senior['concentration_20']=mall['default_age_senior']
    df_audience_segments_senior['concentration_21']=mall['default_age_senior']
    df_audience_segments_senior['concentration_22']=mall['default_age_senior']
    df_audience_segments_senior['concentration_23']=mall['default_age_senior']

    
    df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_female,ignore_index = True)
    df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_kid,ignore_index = True)
    df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_young,ignore_index = True)
    df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_adult,ignore_index = True)
    df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_senior,ignore_index = True)
    df_audience_segments_male=df_audience_segments_male.append(df_audience_segments_male,ignore_index = True) 

    #save to DB / update
    df_audience_segments=df_audience_segments_male.copy()
    print(df_audience_segments)

    df_audience_segments['datetime']=df_audience_segments['date']
    df_audience_segments['datetime']=df_audience_segments['datetime'].dt.strftime('%Y-%m-%d')

    columns = ['date']
    df_audience_segments.drop(columns, inplace=True, axis=1)



    df_audience_segments.to_sql('audience_segments', engine, if_exists='append', index=False)







