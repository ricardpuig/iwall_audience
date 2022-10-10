import json
import csv
import re
import os
import sys, getopt, time
import ast
from subprocess import call
from collections import Counter
from operator import itemgetter
import mysql.connector
import pandas as pd
import argparse
import shutil
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

def main(argv):

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

    # Initialize parser
    parser = argparse.ArgumentParser()
    # Adding optional argument
    parser.add_argument("-f", "--folder", help = "Folder to process")
    parser.add_argument("-s", "--sensor", help = "sensor id")
    args= parser.parse_args()

    if args.folder:
        inputfolder=args.folder
    else:
        print("Input folder not specified")
        sys.exit()

    if args.sensor:
        sensor_id=args.sensor
    else:
        print("Sensor not specified")
        sys.exit()


    print('------------------------------')
    print('Pedcount daily data aggregator')
    print('------------------------------')
    print('Input folder is ', inputfolder)
    print('Sensor is ', sensor_id) 

    isExist = os.path.exists("./processed")
    if not isExist:
        os.makedirs("./processed")

    parsed_detections=[]
    
    for file in os.listdir(inputfolder):
       if os.path.isfile(os.path.join(inputfolder, file)):
            #print("Processing ", file)
            filepath= inputfolder+'/'+file
            with open(filepath) as f:
                raw=f.readline()
                detection=json.loads(raw)
                #print(json.dumps(detection, indent=4))
                parsed_detections.append({
                    "gender": detection['data'][0]['gender'],
                    "age" : detection['data'][0]['age'],
                    "time_in_camera": detection['data'][0]['time_inCamera'],
                    "detection_time" : detection['tstamp'],
                    "day":datetime.fromtimestamp(int(detection['tstamp'])).strftime('%Y-%m-%d'),
                    "hour":datetime.fromtimestamp(int(detection['tstamp'])).strftime('%H'),
                    })
            shutil.move(filepath, "./processed/"+ file)

    print("Consolidated data: ")
    print("-------------------")
    print("num total detections: ", len(parsed_detections))
    print("____________________")
    print("  By hour: ")
    print("0: ", len([i for i in parsed_detections if i['hour'] == '00']))
    print("1: ", len([i for i in parsed_detections if i['hour'] == '01']))
    print("2: ", len([i for i in parsed_detections if i['hour'] == '02']))
    print("3: ", len([i for i in parsed_detections if i['hour'] == '03']))
    print("4: ", len([i for i in parsed_detections if i['hour'] == '04']))
    print("5: ", len([i for i in parsed_detections if i['hour'] == '05']))
    print("6: ", len([i for i in parsed_detections if i['hour'] == '06']))
    print("7: ", len([i for i in parsed_detections if i['hour'] == '07']))
    print("8: ", len([i for i in parsed_detections if i['hour'] == '08']))
    print("9: ", len([i for i in parsed_detections if i['hour'] == '09']))
    print("10: ", len([i for i in parsed_detections if i['hour'] == '10']))
    print("11: ", len([i for i in parsed_detections if i['hour'] == '11']))
    print("12: ", len([i for i in parsed_detections if i['hour'] == '12']))
    print("13: ", len([i for i in parsed_detections if i['hour'] == '13']))
    print("14: ", len([i for i in parsed_detections if i['hour'] == '14']))
    print("15: ", len([i for i in parsed_detections if i['hour'] == '15']))
    print("16: ", len([i for i in parsed_detections if i['hour'] == '16']))
    print("17: ", len([i for i in parsed_detections if i['hour'] == '17']))
    print("18: ", len([i for i in parsed_detections if i['hour'] == '18']))
    print("19: ", len([i for i in parsed_detections if i['hour'] == '19']))
    print("20: ", len([i for i in parsed_detections if i['hour'] == '20']))
    print("21: ", len([i for i in parsed_detections if i['hour'] == '21']))
    print("22: ", len([i for i in parsed_detections if i['hour'] == '22']))
    print("23: ", len([i for i in parsed_detections if i['hour'] == '23']))

    print("____________________")
    print("Age - Unknown: ", len([i for i in parsed_detections if i['age'] == 'Unknown']))
    print("Age - Kid ", len([i for i in parsed_detections if i['age'] == 'Kid']))
    print("Age - Young ", len([i for i in parsed_detections if i['age'] == 'Young']))
    print("Age - Adult: ", len([i for i in parsed_detections if i['age'] == 'Adult']))
    print("Age - Senior: ", len([i for i in parsed_detections if i['age'] == 'Senior']))

    print("____________________")
    print("gender - male: ", len([i for i in parsed_detections if i['gender'] == 'male']))
    print("gender - female: ", len([i for i in parsed_detections if i['gender'] == 'female']))
    print("gender - unknown: ", len([i for i in parsed_detections if i['gender'] == 'unknown']))

    #get data by day
    df_detections=pd.DataFrame(parsed_detections)
    #print(df_detections)

    if len(parsed_detections)==0:
        sys.exit()

    df_grouped_by_date = df_detections.groupby('day')
    # iterate over each group
    for group_name, df_group in df_grouped_by_date:
        print(df_group)
        print(group_name)
        df_hours=df_group.groupby(['hour']).size().reset_index(name='count') 
        print(df_hours.set_index('hour').to_dict('dict'))
        hours_count_dict=df_hours.set_index('hour').to_dict('dict')
        hour_count=hours_count_dict['count']
        print(hour_count)

        entries=df_group.shape[0]

        mean_duration=df_group['time_in_camera'].mean()
        print("average time in cam: ", mean_duration)

        gender= df_group['gender'].value_counts(normalize=True).to_dict()
        print(gender)

        age= df_group['age'].value_counts(normalize=True).to_dict()
        print(age)

        
        #write to SQL 
        #update campaign analysis
        sql= "INSERT INTO pedcount_data (\
                pedcount_id, \
                date, \
                concentration_gender_male, \
                concentration_gender_female, \
                concentration_gender_unknown, \
                concentration_age_unknown, \
                concentration_age_teenager, \
                concentration_age_young,  \
                concentration_age_adult, \
                concentration_age_senior, \
                avg_view_time, \
                views_00, \
                views_01, \
                views_02, \
                views_03, \
                views_04, \
                views_05, \
                views_06, \
                views_07, \
                views_08, \
                views_09, \
                views_10, \
                views_11, \
                views_12, \
                views_13, \
                views_14, \
                views_15, \
                views_16, \
                views_17, \
                views_18, \
                views_19, \
                views_20, \
                views_21, \
                views_22, \
                views_23, \
                total_views) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
        val= (
            sensor_id,
            str(group_name), 
            gender['male'],
            gender['female'],
            gender['unknown'],
            age ['Unknown'] if 'Unknown' in age else 0.0,
            age['Teenager']if 'Teenager' in age else 0.0,
            age['Young']if 'Young' in age else 0.0,
            age['Adult']if 'Adult' in age else 0.0,
            age['Senior']if 'Senior' in age else 0.0,
            float(mean_duration),
            hour_count['00'] if '00' in hour_count else 0 ,
            hour_count['01'] if '01' in hour_count else 0 ,
            hour_count['02'] if '02' in hour_count else 0 ,
            hour_count['03'] if '03' in hour_count else 0 ,
            hour_count['04'] if '04' in hour_count else 0 ,
            hour_count['05'] if '05' in hour_count else 0 ,
            hour_count['06'] if '06' in hour_count else 0 ,
            hour_count['07'] if '07' in hour_count else 0 ,
            hour_count['08'] if '08' in hour_count else 0 ,
            hour_count['09'] if '09' in hour_count else 0 ,
            hour_count['10'] if '10' in hour_count else 0 ,
            hour_count['11'] if '11' in hour_count else 0 ,
            hour_count['12'] if '12' in hour_count else 0 ,
            hour_count['13'] if '13' in hour_count else 0 ,
            hour_count['14'] if '14' in hour_count else 0 ,
            hour_count['15'] if '15' in hour_count else 0 ,
            hour_count['16'] if '16' in hour_count else 0 ,
            hour_count['17'] if '17' in hour_count else 0 ,
            hour_count['18'] if '18' in hour_count else 0 ,
            hour_count['19'] if '19' in hour_count else 0 ,
            hour_count['20'] if '20' in hour_count else 0 ,
            hour_count['21'] if '21' in hour_count else 0 ,
            hour_count['22'] if '22' in hour_count else 0 ,
            hour_count['23'] if '23' in hour_count else 0 ,
            entries)
       
        mycursor.execute(sql,val)
        mydb.commit()

if __name__ == "__main__":
   main(sys.argv[1:])