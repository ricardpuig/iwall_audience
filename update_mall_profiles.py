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

from fbprophet import Prophet
from fbprophet.plot import plot_plotly
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.offline as py
import sys

csv_file = sys.argv[1]

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
  
df_mall_profile=pd.read_csv(csv_file, delimiter=';', decimal = ',')
df_mall_profile['id']=df_mall_profile['id'].astype(int)
print(df_mall_profile.dtypes)



print(df_mall_profile)

query = "SELECT * from malls"
df_malls = pd.read_sql_query(query, engine)

df_mall_profile = df_mall_profile.rename(columns = {'anual_affluence': 'affluence', 'daily_impressions': 'default_screen_day_impressions', 
     'male': 'default_dem_male', 'female': 'default_dem_female', 
     'kid': 'default_age_kid', 'young': 'default_age_young', 'adult': 'default_age_adult', 'senior': 'default_age_senior', 
     'A': 'default_nse_A','B': 'default_nse_B','C': 'default_nse_C','D': 'default_nse_D' }, inplace = False)

df_mall_profile['affluence']=df_mall_profile['affluence'].fillna(0)
    
print(df_mall_profile)

for i in range(len(df_mall_profile)) : 
    #print(df_mall_profile.loc[i, "affluence"], df_mall_profile.loc[i, "default_dem_male"]) 
    query = "UPDATE malls SET name=\'{name}\',  affluence={aff},default_screen_day_impressions={imp},  default_dem_male={mal}, default_dem_female={fem}, default_age_kid={kid},  default_age_young={you},  default_age_adult={adu}, default_age_senior={sen}, default_nse_A={a}, default_nse_B={b}, default_nse_C={c} ,default_nse_D={d} WHERE id={id}".format(
            name=df_mall_profile.loc[i, "site"],
            aff=df_mall_profile.loc[i, "affluence"],
            imp=df_mall_profile.loc[i, "default_screen_day_impressions"],
            mal=df_mall_profile.loc[i, "default_dem_male"],
            fem=df_mall_profile.loc[i, "default_dem_female"],
            kid=df_mall_profile.loc[i, "default_age_kid"],
            you=df_mall_profile.loc[i, "default_age_young"],
            adu=df_mall_profile.loc[i, "default_age_adult"],
            sen=df_mall_profile.loc[i, "default_age_senior"],
            a=df_mall_profile.loc[i, "default_nse_A"],
            b=df_mall_profile.loc[i, "default_nse_B"],
            c=df_mall_profile.loc[i, "default_nse_C"],
            d=df_mall_profile.loc[i, "default_nse_D"],
            id=df_mall_profile.loc[i, "id"]
        )
    engine.execute(query)




#df_malls=df_malls.drop(['affluence', 'default_screen_day_impressions', 'default_dem_male', 'default_dem_female', 'default_age_kid', 'default_age_young', 'default_age_adult', 
# 'default_age_senior', 'default_nse_A', 'default_nse_B', 'default_nse_C','default_nse_D' ], axis=1)

#df_malls= pd.merge(df_malls,df_mall_profile, how='left', left_on= "id", right_on="id")




#print(df_malls)
#print(df_mall_profile)







