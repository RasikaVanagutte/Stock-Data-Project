import time
import sqlalchemy as db
import pandas as pd
import os
import glob
import multiprocessing
import numpy as np
import sqlalchemy.orm as sao

def process_(file):
    df = pd.read_csv(file)
    return df
#---------------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Connect to MySQL Server using SQLAlchemy
    engine = db.create_engine("mysql+mysqlconnector://rasika:Rasikajolly29!@localhost:3306/dir", echo=False)
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
    dtp_df['Date_ID'] = dtp_df['Date_ID'].fillna(0)
    dtp_df['Stock_ID'] = dtp_df['Stock_ID'].fillna(0)
#----------------------------------------------------------------------------------------------------------------------------------------------
    # Pull the Calendar tables and the Stock Name directories from MySQL and store into respective dataframes
    year_df = pd.read_sql_table('year_',engine)
    month_df = pd.read_sql_table('month_',engine)
    day_df = pd.read_sql_table('day_',engine)
    ticker_df = pd.read_sql_table('ticker_list',engine)
#----------------------------------------------------------------------------------------------------------------------------------------------
    #Change the datatypes of the columns to match the Daily Ticker Price column data types respectively
    matching_df['date'] = pd.to_datetime(matching_df['date'])
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
    dtp_df = dtp_df.fillna(0)
#-----------------------------------------------------------------------------------------------------------------------------------------------
    print("Here we go.......................")
    dtp_df.to_csv(r'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\DTP_data.csv', index = False)
    print("Time taken by the program ", time.time() - start_time , " seconds")
