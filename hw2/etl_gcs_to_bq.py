from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    # gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path= f"02_gcp/data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f".")
    return gcs_path

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f'Rows: {len(df)}')
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    df.to_gbq(
        destination_table="dezoomcamp.yellow_taxi_rides",
        project_id="zoomcamp-375102",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color, year, months):
    """Main ETL flow to load data into Big Query"""
    color = color
    year = year
    total_rows_count=0
    for month in months: 
        path = extract_from_gcs(color, year, month)
        df = transform(f'../data/{path}')
        write_bq(df)
        total_rows_count+=len(df)
    print(f'Total number of rows: {total_rows_count}')


if __name__ == "__main__":
    color='yellow'
    year=2019
    months= [2,3]
    etl_gcs_to_bq(color, year, months)
