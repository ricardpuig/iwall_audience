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
from trycourier import Courier 


def mall_update_report(message_to_send):

    email_to_send="rpuig@iwallinshop.com"
    client = Courier(auth_token="pk_prod_6S1S6BVGGXMEB5Q8TSVHDN59NY8D")
    auth = "Bearer e03b2732ac76e3a954e4be0c280a04a3";
    report_template= "63S3SQJ5EA4YAYH10N88VCJJE6QT"

    resp = client.send_message(
				message={
					"to": {
						"email": email_to_send,
					},
					"template": report_template,
						"data": {
							"message_type" : "Mall Inventory Update: ",
							"message": "https://datastudio.google.com/reporting/7e090667-0f67-4af8-bde8-3481df63bdb8"
						},
				})
    print(resp['requestId'])
    None


engine = create_engine("mysql+pymysql://{user}:{pw}@ec2-52-18-248-109.eu-west-1.compute.amazonaws.com/{db}"
                       .format(user="root",
                               pw="SonaeRootMysql2021!",
                               db="audience"))

#database connector
mydb = mysql.connector.connect(
  host="ec2-52-18-248-109.eu-west-1.compute.amazonaws.com",
  user="root",
  passwd="SonaeRootMysql2021!",
  database="audience"
)
mycursor = mydb.cursor()

if sys.argv[1]:
	country= sys.argv[1]
else:
	print("Country Missing, exiting....")
	exit(1)

print("Updating and checking mall information")

sql_select = "SELECT * from malls"
print(sql_select)
mycursor.execute(sql_select)
records= mycursor.fetchall()

for row in records:  
    print(row)


