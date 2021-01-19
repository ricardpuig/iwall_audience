import requests
import csv
import json
import re
import mysql.connector
from datetime import datetime
from datetime import date

print ("Order to Audience database \n ")

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


with open('order.csv',"rU") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
                #reservation_id
                 reservation_id = row[1]
                 mycursor.execute("DELETE FROM campaign_order WHERE reservation_id LIKE "+str(reservation_id))
                 mycursor.execute("DELETE FROM campaign_order_items WHERE reservation_id LIKE "+str(reservation_id))
                 print "Reservation : " + str(reservation_id)
                 sql= "INSERT INTO campaign_order (reservation_id,name) VALUES (%s,%s)"
                 val= (row[1],"new")
                 mycursor.execute(sql,val)
                 mydb.commit()
        if line_count == 1:
                print " Campaign_name : " + str(row[1])
                sql= "UPDATE campaign_order SET name=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()

        if line_count == 2:
                print "Start date :" + str(row[1])
                sql= "UPDATE campaign_order SET start_date=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if line_count == 3:
                print "End date" + str(row[1])
                sql= "UPDATE campaign_order SET end_date=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if line_count == 4:
                print "Agencia" + str(row[1])
                sql= "UPDATE campaign_order SET agency=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if line_count == 5:
                print "Anunciante" + str(row[1])
                sql= "UPDATE campaign_order SET anunciante=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if line_count == 6:
                print "Negociado" + str(row[1])
                sql= "UPDATE campaign_order SET negociado=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if line_count == 7:
                print "Importe" + str(row[1])
                sql= "UPDATE campaign_order SET importe_total=%s WHERE reservation_id=%s"
                val= (row[1], reservation_id)
                mycursor.execute(sql,val)
                mydb.commit()
        if line_count >=9:
                print "MALL" + str(row[0])
                print "numero pantallas: " + str(row[1])
                if row[1] > "0":
                    sql= "INSERT INTO campaign_order_items (reservation_id, mall_name, start_date, end_date, screen_count,broadsign_container_id) VALUES (%s,%s,%s,%s,%s,%s)"
                    val= (reservation_id, str(row[0]), str(row[3]), str(row[4]), row[1],row[2] )
                    mycursor.execute(sql,val)
                    mydb.commit()
        #if line_count == 5:
        #if line_count == 6:
        #if line_count == 7:
        #if line_count == 8:
        #if line_count == 9:
        #if line_count > 9:
        line_count += 1

mycursor.close()

if(mydb.is_connected()):
    mycursor.close()
    print("MySQL connection is closed")
