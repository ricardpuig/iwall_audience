import requests
import json
import re
import mysql.connector
import pandas as pd
import numpy as np
import csv
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



query = "SELECT * from mall_default_models  WHERE mall_type =2"
df_model = pd.read_sql_query(query, engine)


#get data updated data from cameras
model_info=df_model.to_dict('records')
model=model_info[0]
    
#do hourly population
hourly=model['hourly'].split(':')
hourly=list(np.float_(hourly))


weekday=model['weekday'].split(':')
weekday=list(np.float_(weekday))


weekly=model['weekly'].split(':')
weekly=list(np.float_(weekly))

np.savetxt("hourly.csv", hourly, delimiter=",", fmt='%s')
np.savetxt("weekday.csv", weekday, delimiter=",", fmt='%s')
np.savetxt("weekly.csv", weekly, delimiter=",", fmt='%s')

