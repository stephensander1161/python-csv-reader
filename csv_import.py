import os
import numpy as np
import pandas as pd
import psycopg2
from decouple import config


#find csv files in my current working directory
#for file in os.listdir(os.getcwd())

print(os.getcwd())

#isolate only the csv files

#make a new directory

#move the csv files into the new directory


df = pd.read_csv("test.csv")
df.head()

#clean table names
#lower case letters
#remove all white spaces
#replace -, /, \\, $ with _

file = "test\\?$"

clean_tbl_name = file.lower().replace(" ","_").replace("?","") \
  .replace("-","_").replace(r"/","_").replace("\\","_").replace("%","") \
  .replace(")","").replace(r"(","_").replace("$","")

#clean header names
#lower case letters
#remove all white spaces
#replace -, /, \\, $ with _

df.columns = [x.lower().replace(" ","_").replace("?","") \
  .replace("-","_").replace(r"/","_").replace("\\","_").replace("%","") \
  .replace(")","").replace(r"(","_").replace("$","") for x in df.columns]


replacements = {
  'object': 'varchar',
  'float64': 'float',
  'int64': 'int',
  'datetime64': 'timestamp',
  'timedelta64[ns]': 'varchar'
}



col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))
print(col_str)


conn_string = config('CONN_STRING')
#open a db connection


conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print('opened DB Successfully')

#drop tables with the same name
cursor.execute("drop table if exists bitcoin;")

#create table
cursor.execute("create table bitcoin \
                (date varchar, txvolume_usd int, adjustedtxvolume int, txcount int, marketcap int, \
                price_usd int, exchangevolume int, generatedcoins float, fees float, activeaddresses int, \
                averagedifficulty float, paymentcount int, mediantxvalue int, medianfee int, blocksize int, \
                blockcount int)")



#insert values to table

#save df to csv
df.to_csv('bitcoin.csv', header=df.columns, index=False, encoding='utf-8')

#open the csv file, save it as an object, and upload to the db
my_file = open('bitcoin.csv')
print('file opened in memory')


#upload to db
SQL_STATEMENT = """
COPY bitcoin FROM STDIN WITH
  CSV
  HEADER
  DELIMITER AS ','
"""

cursor.copy_expert(sql=SQL_STATEMENT, file = my_file)
print(' file copied to db')


#make public
cursor.execute("grant select on table bitcoin to public")
conn.commit()

cursor.close()
print('table bitcoin imported to db completed')
