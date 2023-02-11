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
def parent_flow():
    color='green'
    year=2020
    month=1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df=fetch_data(url)
    cleaned_df= clean_data(df)
    path= store_localy(cleaned_df, color, dataset_file)
    store_gcs(path)

if __name__=="__main__":
    parent_flow()