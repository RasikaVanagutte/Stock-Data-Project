import time
import sqlalchemy as db
import pandas as pd
import os
import glob
import multiprocessing
import numpy as np
import sqlalchemy.orm as sao

def write_process_(df_):
	engine = db.create_engine('mysql+mysqlconnector://akanksha:Akankshajolly29!@localhost:3306/dir', echo=False)
	df_.to_sql('ticker_list', engine, index = False, if_exists = 'append', chunksize = 10000)
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

    ticker_df = pd.DataFrame(data=matching_df.Name.unique(), columns = ['symbol'])

#---------------------------------------------------------------------------------------------------------------------------------------------
    # Procedure using Generators/List comprehension to upload the final data frame into MySQL table -Ticker_List
    size = 10000
    list_of_dfs = (ticker_df.iloc[i:i+size,:] for i in range(0, len(ticker_df),size))
    print("Here we go....................")
    pool = multiprocessing.Pool()
    with pool as p:
        p.map(write_process_, list_of_dfs)
#-----------------------------------------------------------------------------------------------------------------------------------------------
    # Print the script execution time 
    print("My program's took", time.time() - start_time, " seconds to run")