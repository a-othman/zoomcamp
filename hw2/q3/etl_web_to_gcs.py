# loading green taxi dataset Jan 2020
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd 
import os


@task(cache_result_in_memory=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch_data(url: str)-> pd.DataFrame:
    """fetches data from a url"""
    df= pd.read_csv(url)
    return df

@task()
def clean_data(df: pd.DataFrame)-> pd.DataFrame:
    """Cleans the data frame ensuring each column of the appropriate type."""
    print('BFORE: \n', df.dtypes)
    df['lpep_pickup_datetime']= pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime']= pd.to_datetime(df['lpep_dropoff_datetime'])
    print('AFTER: \n', df.dtypes)
    print(f'Number of data rows are: {len(df)}')
    return df

@task()
def store_localy(df: pd.DataFrame, color: str, dataset_file: str)-> str:
    """Store dataframe data in parquet format locally"""
    os.makedirs(f'./02_gcp/data/{color}', exist_ok=True)
    # df= df.head(100)
    df.to_parquet(f'./02_gcp/data/{color}/{dataset_file}.parquet')
    return f'./02_gcp/data/{color}/{dataset_file}.parquet'

@task()
def store_gcs(path: str)-> None:
    """Store data on google cloud storage"""
    gcs_bucket = GcsBucket.load("zoom-gcs")
    with open(path, "rb") as f:
        gcs_bucket.upload_from_file_object(f, path, timeout=3000)

@flow()
def extract_load_1file(color:str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df=fetch_data(url)
    # cleaned_df= clean_data(df)
    path= store_localy(df, color, dataset_file)
    store_gcs(path)
    return len(df)

@flow(log_prints=True)
def parent_flow(color: str, year: int, months: list)-> None:
    total_processed_rows=0
    for month in months:
        total_processed_rows+=extract_load_1file(color, year, month)
    print(f'Total Number of rows processed is: {total_processed_rows}')
        

if __name__=="__main__":
    color='yellow'
    year=2019
    months=[2,3]
    parent_flow(color, year, months)