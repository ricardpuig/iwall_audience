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
from trycourier import Courier 
from dateutil.relativedelta import relativedelta
from sklearn import preprocessing
from statistics import mean
import math


dus=[ 49705831, 49705914, 49759805, 49769955, 49769969, 49770980, 58445628, 175189058, 181248745, 197796991,
    197797937, 259381034, 288556610, 358867577, 386841366, 402980064, 412355877, 470981010, 553166147, 601721096,
    601721149, 601721201, 601997503, 635785013, 666884792, 666884852, 687507069, 687527856, 687530056, 134163995,
    146377276, 624591013, 709711513, 709721520, 713606572, 844345232, 845766505, 849216393, 849221600, 849221691,
    849221748, 849221806, 856687287, 860738934, 885976173, 827861523, 915742505, 915742505, 918581869, 918582308,
    919031249, 922038332, 927807019, 937913662, 955844873, 956837557, 958153114, 976696480, 976696762, 976697691,
    976697753, 976698579, 976699643, 976699711, 976699958, 976701105, 49519741, 127314259, 288338317, 905936750,
    916967617, 920136571, 925722041, 954752876, 963626869, 963626881, 963626915, 963626925, 963626936, 965122510,
    971993490, 975653967, 979032934, 979078519, 979078551, 979078566, 979078582, 980994005, 981717808, 981717813,
    983805146, 985111772, 985111781, 985111795, 985111817, 985111831, 985114500, 985114516, 985114526, 985116570,
    985116579, 985116714, 985118161, 985118175, 985120439, 985120469, 985121047, 985122309, 985122362, 985122473,
    985122510, 985124456, 985125164, 985125363, 985127361, 985127373, 985127398, 991806664, 993214455]

impressions_per_hour_du= {}
impressions_per_hour= []



#SQL connect
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



# players coger el mall id y player screens  con el display unit id

for du in dus: 
    sql= "SELECT mall_id, player_screens, container_name FROM players WHERE display_unit_id=%s" % (du)
    mycursor.execute(sql)
    records_players= mycursor.fetchall()
    for row in records_players:
        impressions_per_hour_du={}
        print("Mall ID: ", row[0], " Mall name", row[2], "screens: " , row[1])
        sql= "SELECT default_screen_day_impressions FROM malls WHERE id=%s" % (row[0]) 
        mycursor.execute(sql)
        records_malls= mycursor.fetchall()
        impressions_per_hour_du['display_unit_id']= du
        impressions_per_hour_du['mall_id']= row[0]

        for mall in records_malls:
            impressions= mall[0]
            print("Impressions: ", impressions)
            impressions_hour= impressions/12
            impressions_all_screens= impressions_hour*row[1]
            impressions_per_hour_du['impressions']=int(impressions_all_screens)
            impressions_per_hour.append(impressions_per_hour_du)

print("Impressions per hour: ", impressions_per_hour)


# Asegúrate de que impressions_per_hour es un DataFrame de pandas
# Si no es así, conviértelo en uno
if not isinstance(impressions_per_hour, pd.DataFrame):
    impressions_per_hour = pd.DataFrame(impressions_per_hour)

# Exportar a CSV
impressions_per_hour.to_csv('impressions_per_hour.csv', index=False)
      



# malls coger el default screen day impressions y sacar el valor por hora 



