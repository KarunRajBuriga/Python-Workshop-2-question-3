#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
import json

data = requests.get("https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2017-01-01&endtime=2017-12-31&alertlevel=yellow")
data = json.loads(data.text)
a = data['features']
df = pd.DataFrame(a)
b = df.properties

time_list = []
place_list = []
type_list = []
magnitude_list = []

for i in range(len(a)):
    time_list.append(b[i]['time'])
    place_list.append(b[i]['place'])
    type_list.append(b[i]['type'])
    magnitude_list.append(b[i]['cdi'])
    i+=1


df1 = pd.DataFrame(time_list,columns = ['Time'])
df1['Place'] = place_list
df1['Type'] = type_list
df1['Magnitude'] = magnitude_list
print(df1)



# In[ ]:


import pyodbc 
server = 'OTUSDPSQL'
database = 'B12022_Target_KBuriga'
username = 'B12022_KBuriga'
password = ''
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


# In[ ]:


cursor.execute('create table apiData(Time bigint , Place varchar(50) , Type varchar(25) , Magnitude numeric(3,1))')


# In[ ]:


for index,row in df1.iterrows():
    cursor.execute("INSERT INTO dbo.apiData values(?,?,?,?)",row.Time , row.Place, row.Type , row.Magnitude)


# In[ ]:


cnxn.commit()


# In[ ]:


cursor.close()


# In[ ]:


cursor.execute ('select top 1 * from dbo.apiData order by Magnitude desc')


# In[ ]:


cnxn.commit()


# In[ ]:


cursor.close()

