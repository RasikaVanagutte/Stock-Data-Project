# Stock-Data-Project
Stock Data Project_For MySQL databases &amp; tables
import time
import os
import pandas as pd
import mysql.connector
import multiprocessing

os.chdir('C://ProgramData/MySQL/MySQL Server 8.0/Uploads')
connection = mysql.connector.connect(host='localhost',
                                 database='dev',
                                 user='rasika',
                                 password='Rasikajolly29!')
cursor = connection.cursor()

sql_query = 'ALTER TABLE daily_ticker_price DROP FOREIGN KEY daily_ticker_price_ibfk_1'
cursor.execute(sql_query)
connection.commit()

sql_query = 'ALTER TABLE daily_ticker_price DROP PRIMARY KEY'
cursor.execute(sql_query)
connection.commit()

sql_query = 'ALTER TABLE daily_ticker_price DROP INDEX stock_id'
cursor.execute(sql_query)
connection.commit()

sql_query = 'load data infile ' + '\'c:/programdata/mysql/mysql server 8.0/uploads/dtp_data.csv\' ' + 'into table daily_ticker_price  fields terminated by ' + '\',\'' + ' ignore 1 lines'
start_time = time.time()
print("here we go.......")
cursor.execute(sql_query)
connection.commit()

print("time taken for upload of csv data is ", time.time() - start_time, " seconds in total")

sql_query = 'ALTER TABLE daily_ticker_price ADD PRIMARY KEY (date_id,stock_id)'
cursor.execute(sql_query)
connection.commit()

sql_query = 'ALTER TABLE daily_ticker_price ADD FOREIGN KEY (stock_id) REFERENCES dir.ticker_list(stock_id)'
cursor.execute(sql_query)
connection.commit()

connection.close()
