import os
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta


# Write a function to fetch taxi trip data from the NYC Open Data API
@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    retries=3, retry_delay_seconds=30,
    log_prints=True, timeout_seconds=60*15,
)
def fetch_dataset(service: str, year: int, month: int) -> tuple[pd.DataFrame, str]:
    """Returns a DataFrame of taxi trip data for a given year and month"""
    # Construct dataset url & dataset filename
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year:04d}-{month:02d}.parquet'
    filename = os.path.basename(url)

    # if file already exists, skip download
    if os.path.isfile(filename):
        print(f'INFO | Reading {service} taxi trip data from {year:04d}-{month:02d}')
        df = pd.read_parquet(filename)
        print('INFO | Done!')
        return df, filename
    
    # Fetch data from url
    print(f'INFO | Fetching {service} taxi trip data from {year:04d}-{month:02d}')
    df = pd.read_parquet(url)
    print('INFO | Done!')

    return df, filename

# Write a function to write a DataFrame to the local file system in parquet format
@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    log_prints=True,
)
def write_to_local(df: pd.DataFrame, filename: str) -> str:
    """Writes a DataFrame to the local file system in parquet format"""
    # Append .gz suffix to filename ot represent compressed file
    filename = f'{filename}.gz'
    
    # Store file in local file system
    print(f'INFO | Storing pandas.DataFrame to local file:\t{filename}')
    df.to_parquet(filename, index=False, compression='gzip')
    print('INFO | Done!')
    return filename

# Write a function to upload a DataFrame to Google Cloud Storage
@task(
    log_prints=True,
    timeout_seconds=60*15
)
def upload_to_gcs(filename: str) -> str:
    """Uploads a DataFrame to Google Cloud Storage"""
    # Get GCS Bucket block
    gcs_bucket = GcsBucket.load('gcp-gcs-bucket')

    # Upload file to GCS Bucket using block object
    print(f'INFO | Uploading file:\t{filename}')
    gcs_bucket_path = gcs_bucket.upload_from_path(filename, to_path=filename)
    print('INFO | Done!')
    return gcs_bucket_path

# Write a function to remove a file from the local file system
@task(
    log_prints=True
)
def remove_from_local(filename: str) -> None:
    """Removes a file from the local file system"""
    print(f'Deleting file:\t{filename}')
    os.remove(filename)
    
# Write a flow function to fetch and upload taxi trip data for a given year and month
@flow
def elt_web_to_gcs(service: str, year: int, month: int, cleanup: bool = False):
    """Fetches and uploads taxi trip data for a given year and month"""
    # Fetch the data
    df, filename = fetch_dataset(service, year, month)
    
    # Write the data
    filename = write_to_local(df, filename)

    # Upload the data
    upload_to_gcs(filename)

    # Clean up the local file system
    remove_from_local(filename)
        

if __name__ == '__main__':
    print()
    elt_web_to_gcs.serve(
        name='ingest-taxi-trips-deployment',
        cron='0 5 1 * *',
        tags=['dev','ingestion'],
        description="Given a taxi service, year, and month; download and ingest month's trip data into our Data Lake on Google Cloud Storage.",
        version="tutorial/deployments",
    )
    print()