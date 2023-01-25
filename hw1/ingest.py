import pandas as pd 
from sqlalchemy import create_engine
from time import time
import argparse
import os

def insert_csv_data(csv_path, engine, table_name, datetime_columns=False):
    
    if datetime_columns:
        df= pd.read_csv(csv_path, parse_dates=['lpep_pickup_datetime',\
            'lpep_dropoff_datetime'], nrows=2)
        df_iterator= pd.read_csv(csv_path, parse_dates=['lpep_pickup_datetime',\
            'lpep_dropoff_datetime'], iterator=True, chunksize=1e5)
    else:
        df= pd.read_csv(csv_path, nrows=2)
        df_iterator= pd.read_csv(csv_path, iterator=True, chunksize=1e5)

    print(pd.io.sql.get_schema(df, name= table_name, con=engine))
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    try:
        while True:
            start_time= time()
            df_= next(df_iterator)
            df_.to_sql(name=table_name, con=engine, if_exists='append')
            end_time= time()
            print(f'Chunck inserted ..... it took {end_time-start_time} seconds')

    except StopIteration:
        print(f'Finished Data Ingestion for {table_name}')
        
def main(params):
    user= params.user
    password= params.password
    port= params.port
    host= params.host
    database= params.database
    green_taxi_path= params.green_taxi_path
    zones_path= params.zones_path


    print(f'user: {user} \
            password: {password} \
            port: {port} \
            host: {host} \
            database: {database} \
            green_taxi_path: {green_taxi_path} \
            zones_path: {zones_path}')

    engine= create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    insert_csv_data(green_taxi_path, engine, 'green_taxi', datetime_columns=True)
    insert_csv_data(zones_path, engine, 'zones')

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data into postgresql')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--database', help='database name')
    parser.add_argument('--green_taxi_path', help='csv path for table 1, trips details')
    parser.add_argument('--zones_path', help='csv path for table 2, zones')
    params = parser.parse_args()
    main(params)
