from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from datetime import timedelta
import pandas as pd 


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def read_dataset(url: str)-> pd.DataFrame:
    """Reads dataframe from google cloud storage bucket"""
    df= pd.read_parquet(url, storage_options=\
            {'token':'/Users/ahmedothman/Desktop/dataengineering_zoomcamp/zoomcamp-375102-936173b40c63.json'})
    return df


@task()
def load_gbq(df: pd.DataFrame)-> None:
    """Loads data into google big query"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    df.to_gbq(destination_table='yellow_taxi.rides', project_id="zoomcamp-375102",
        if_exists='append', progress_bar=True,
        credentials=gcp_credentials_block.get_credentials_from_service_account())

@flow(log_prints=True)
def parent_flow(datasets_urls: list)-> None:
    for url in datasets_urls:
        df= read_dataset(url)
        load_gbq(df)
