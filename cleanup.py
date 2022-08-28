import mysql.connector
import pandas as pd
import json
import sys
import requests
import random

def read_csv(path):
  return pd.read_csv(path)

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="qwerty"
)

mycursor = mydb.cursor()
mycursor.execute("show databases")
databases=[]
for i in mycursor:
  databases.append(i[0])
if 'dataengineer' not in databases:
  mycursor.execute('create database dataengineer')
mycursor.execute('use dataengineer')
print('databases created')
print('-------------------\n\n')


file_loc=r"C:\Users\Lenovo\Downloads\Senior_Data_Engineer_Assignment_ (2)\covid-19-state-level-data.csv"
data=read_csv(file_loc)
print("data Loaded from csv to python")
print('-------------------\n\n')
max_length=max(map(len,(data['state'])))
mycursor.execute('show tables')
tables=[]
for i in mycursor:
  tables.append(i[0])
if 'stage_covid' not in tables:
    mycursor.execute(f'create table stage_covid( s_number int primary key,date date,\
    state varchar({max_length}),\
    cfr float,\
    fips int,\
    cases int,\
    deaths int)')
print("table created in mysql server")
print('-------------------\n\n')
name_column=data.columns
for index,row in data.iterrows():
  string_ins=str(row[name_column[0]])+',\''+row[name_column[1]]+'\','+ '\''+row[name_column[2]]+'\','+str((1-(row[-1]/row[-2]))*100)
  for i in name_column[3:]:
    string_ins+=','+str(row[i])
  mycursor.execute(f'insert into stage_covid values({string_ins})')
mycursor.execute('commit')
print('----------------------\n\n')
mycursor.execute('commit')
if 'stage_state_covid' not in tables:
  mycursor.execute('create table stage_state_covid as (select date_format(date,\'%m-%y\') monyear,state,(((sum(cases)-sum(deaths))/sum(cases))*100) cfr, sum(deaths) deaths, sum(cases) cases from stage_covid group by date_format(date,\'%m-%y\'),state)')
  mycursor.execute('commit')

url=r'https://hooks.slack.com/services/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'
hex_number = random.randint(1118481, 16777215)
hex_number = str(hex(hex_number))
mycursor.execute('select monyear,state,cases  from stage_state_covid order by cases desc limit 3')
message='Total No. of cases month wise :- \n'
for i in mycursor:
  message+='Month/Year :- '+ str(i[0]) +'    |    ' + 'State :-  '+ i[1]+'   |      Total Cases :- '+str(i[2])+'\n'
slack_data={
        "attachments": [
            {
                "color": hex_number,
                "fields": [
                    {
                        "title": "Covid Data Summary",
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
byte_length = str(sys.getsizeof(slack_data))
headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
response = requests.post(url, data=json.dumps(slack_data), headers=headers)
if response.status_code != 200:
        raise Exception(response.status_code, response.text)
