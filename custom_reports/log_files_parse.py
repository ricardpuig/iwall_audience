from datetime import datetime, timedelta
from datetime import date
import sys
import os,glob
import pandas as pd



#folder
folder_path = '/Users/ricardpuig/logs_colombia'

techno=[]
dg=[]

count_techno=0
count_dg=0

for filename in glob.glob(os.path.join(folder_path, '*')):
  with open(filename, 'r') as f:
    text = f.read()
    print (filename)
    data=text.split()

    count_techno=0
    count_dg=0
    for i in data: 
      if i =='542279735':
        count_techno=count_techno+1
      if i =='543538268':
        count_dg=count_dg+1

    techno.append({'date':filename.split('.')[2], 'number_occurrences': count_techno/2})
    dg.append({'date':filename.split('.')[2], 'number_occurrences': count_dg/2})

df_techno = pd.DataFrame(techno) 
df_dg = pd.DataFrame(dg) 

print(df_techno)



# saving the dataframe 
df_techno.to_csv('techno.csv')     
# saving the dataframe 
df_dg.to_csv('dg.csv') 

print(techno)
print(dg)

