import requests
import csv
import json
import re
import mysql.connector
from datetime import datetime
from datetime import date

print ("Order / campaign planning to Audience database \n ")

#URLS endpoints----------

container_ids=[]
container_name=[]
reservation={}
malls={}

campaign_daily_performance={};
campaign_daily_audience={};
campaign_display_unit_performance={};
campaign_display_unit_audience={};
campaign_mall_performance={};
campaign_mall_audience={};

#database connector
mydb = mysql.connector.connect(
  host="ec2-52-18-248-109.eu-west-1.compute.amazonaws.com",
  #host="54.38.184.204",
  user="root",
  #user="iwall",
  #database="netmon",
  #passwd= "iwalldigitalsignage",
  passwd="sonaeRootMysql2017",
  database="audience"
)

mycursor = mydb.cursor()

state= 0

with open('Audience-order-nissan-oct.csv',"rU") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    line_count = 0
    for row in csv_reader:

        if line_count == 0:
                #reservation_id
                reservation_id = int(row[1])
                print("Processing campaign: ", reservation_id)

               

                query= "DELETE FROM campaign_order WHERE reservation_id={id}".format(id=reservation_id)
                mycursor.execute(query)
                mydb.commit()

                query= "DELETE FROM campaign_order_items WHERE reservation_id={id}".format(id=reservation_id)
                mycursor.execute(query)
                mydb.commit()

                print("Reservation : ", str(reservation_id))
                sql= "INSERT INTO campaign_order (reservation_id,name) VALUES (%s,%s)"
                val= (row[1],"new")
                mycursor.execute(sql,val)
                mydb.commit()
        if str(row[0]) == "NOMBRE":
                print("Campaign_name : ", str(row[1]))
                sql= "UPDATE campaign_order SET name=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()

        if str(row[0]) == "FECHA INICIO":
                print("Start date :" , str(row[1]))
                sql= "UPDATE campaign_order SET start_date=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                start_date= row[1]
                mycursor.execute(sql,val)
                mydb.commit()
        if str(row[0]) == "FECHA FIN":
                print ("End date" , str(row[1]))
                sql= "UPDATE campaign_order SET end_date=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
                end_date=row[1]
 
        if str(row[0]) == "ANUNCIANTE":
                print ("Anunciante" , str(row[1]))
                sql= "UPDATE campaign_order SET anunciante=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if str(row[0]) == "NEGOCIADO CON":
                print ("Negociado" , str(row[1]))
                sql= "UPDATE campaign_order SET negociado=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if str(row[0]) == "IMPORTE TOTAL":
                print ("Importe" , str(row[1]))
                sql= "UPDATE campaign_order SET importe_total=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if str(row[0]) == "OBSERVACIONES":
                print ("Observaciones" , str(row[1]))
                sql= "UPDATE campaign_order SET observaciones=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()

        if str(row[0]) == "CARAS CONTRATADAS":
                print ("Caras" , str(row[1]))
                sql= "UPDATE campaign_order SET total_screens=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()

        if str(row[0]) == "CENTRO COMERCIAL":
                if state == 0:
                        state=1 
                        print("mall screens")          
                        continue       
        if state==1:
                print ("MALL" , str(row[0]))
                print ("Numero pantallas: " , str(row[1]))
                if row[1] > "0":
                    sql= "INSERT INTO campaign_order_items (reservation_id, mall_id, screen_count, start_date, end_date) VALUES (%s,%s,%s,%s,%s)"
                    val= (reservation_id, str(row[2]),str(row[1]) , start_date, end_date )
                    mycursor.execute(sql,val)
                    mydb.commit()
        
        line_count += 1

mycursor.close()

if(mydb.is_connected()):
    mycursor.close()
    print("MySQL connection is closed")
