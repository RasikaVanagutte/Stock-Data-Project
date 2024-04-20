import time
import sqlalchemy as db
import pandas as pd
import os
import glob
import multiprocessing
import numpy as np
import sqlalchemy.orm as sao

def write_process_(df_):
	engine = db.create_engine('mysql+mysqlconnector://akanksha:Akankshajolly29!@localhost:3306/dev', echo=False)
	df_.to_sql('daily_ticker_price', engine, index = False, if_exists = 'append', chunksize = 10000)
#---------------------------------------------------------------------------------------------------------------------------------------------
def process_(file):
    df = pd.read_csv(file)
    return df
#---------------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Connect to MySQL Server using SQLAlchemy
    engine = db.create_engine("mysql+mysqlconnector://akanksha:Akankshajolly29!@localhost:3306/dir", echo=False)
    connection = engine.connect()
#---------------------------------------------------------------------------------------------------------------------------------------------
    # Point to the directory of inputs; stock market data CSV files
    os.chdir('D:\\individual_stocks_5yr')
    # Start timing the execution time of entire script
    start_time = time.time()
#---------------------------------------------------------------------------------------------------------------------------------------------
    # Pull the CSV files' data into a dataframe
    pool = multiprocessing.Pool()
    with pool as p:
        df_list = p.map(process_,glob.glob('*.csv'))
        matching_df = pd.concat(df_list, ignore_index=True)
#---------------------------------------------------------------------------------------------------------------------------------------------
    # Create the dataframe to pass into the MySQL table; create the composite primary key index columns
    dtp_df = pd.DataFrame(columns= ['Date_ID','Stock_ID']) 
    dtp_df = pd.concat([dtp_df, matching_df], axis=1)
    # Remove NaN values from primary key columns
    dtp_df['Date_ID']  = dtp_df['Date_ID'].fillna(0) 
    dtp_df['Stock_ID'] = dtp_df['Stock_ID'].fillna(0)
#----------------------------------------------------------------------------------------------------------------------------------------------
    # Pull the Calendar tables and the Stock Name directories from MySQL and store into respective dataframes
    year_df = pd.read_sql_table('year_',engine)
    month_df = pd.read_sql_table('month_',engine)
    day_df = pd.read_sql_table('day_',engine)
    ticker_df = pd.read_sql_table('ticker_list',engine)
#----------------------------------------------------------------------------------------------------------------------------------------------
    #Change the datatypes of the columns to match the Daily Ticker Price column data types respectively
#    matching_df['date'] = pd.to_datetime(matching_df['date'])
    dtp_df['date'] = pd.to_datetime(dtp_df['date'])
    dtp_df['Stock_ID'] = dtp_df['Stock_ID'].astype('int64')
    dtp_df['volume'] = dtp_df['volume'].astype('float')
    dtp_df['Name'] = pd.Series(dtp_df['Name'].astype('string'))
    ticker_df['Symbol'] = pd.Series(ticker_df['Symbol'].astype('string'))
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Temporary line of code for convenience w.r.t. sample input data
    dtp_df = dtp_df.rename(columns={'Name': 'Symbol'})
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Generate the primary key values - part 1 - Ticker_List to Stock_ID
    dtp_df['Stock_ID'] = dtp_df['Symbol'].map(ticker_df.set_index('Symbol')['Stock_ID'])
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Generate the primary key values - part 2 - datetime to Date_ID
    dtp_df['Date_ID'] = pd.DatetimeIndex(dtp_df['date']).year.map(year_df.set_index('YEAR_col')['YID'])  
    dtp_df['Date_ID'] = dtp_df['Date_ID'].map(str) + pd.DatetimeIndex(dtp_df['date']).month.map(month_df.set_index('month_col')['mid']).map(str)
    dtp_df['Date_ID'] = pd.Series(dtp_df['Date_ID']).map(str) + pd.DatetimeIndex(dtp_df['date']).day.map(day_df.set_index('day_col')['did']).map(str)
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Drop columns not required for Daily_Ticker_Price
    dtp_df = dtp_df.drop(columns=['date', 'Symbol'])
    # Temporary line of code for convenience w.r.t. sample input data 
    dtp_df['Date_ID'] = dtp_df['Date_ID'].astype('int64')
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Drop primary and foreign keys on table to make inserts faster on daily_ticker_price table
    connection.execute('ALTER TABLE dev.daily_ticker_price DROP FOREIGN KEY daily_ticker_price_ibfk_1;')
    connection.execute('commit;')
    connection.execute('ALTER TABLE dev.daily_ticker_price DROP PRIMARY KEY;')
    connection.execute('commit;')
    connection.execute('ALTER TABLE dev.daily_ticker_price DROP INDEX stock_id;')
    connection.execute('commit;')
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Procedure using Generators/List comprehension to upload the final data frame into MySQL table - Daily_Ticker_Price    
    size = 10000
    list_of_dfs = (dtp_df.iloc[i:i+size,:] for i in range(0, len(dtp_df),size))
    print("Here we go....................")
    pool = multiprocessing.Pool()
    with pool as p:
        p.map(write_process_, list_of_dfs)
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Re-add the primary-key and foreign-key constraints back on daily_ticker_price table
    connection.execute('ALTER TABLE dev.daily_ticker_price ADD PRIMARY KEY (date_id,stock_id);')
    connection.execute('commit;')
    connection.execute('ALTER TABLE dev.daily_ticker_price ADD FOREIGN KEY (stock_id) REFERENCES dir.ticker_list(stock_id);')
    connection.execute('commit;')
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Print the script execution time 
    print("My program's took", time.time() - start_time, " seconds to run")
    #~~15 seconds. Tried even with list sizes 10000, 50000, 100000 -- only made a 1-2 second difference. Meh