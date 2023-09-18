import os
import pandas as pd
import datetime as dt
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_aws.s3 import S3Bucket
from datetime import timedelta



# Write a function to fetch taxi trip data from the NYC Open Data API
@task(
    retries=3, retry_delay_seconds=30,
    log_prints=True, timeout_seconds=60*15,
)
def fetch_dataset(service: str, year: int, month: int) -> tuple[pd.DataFrame, str]:
    """Returns a DataFrame of taxi trip data for a given year and month"""
    # Construct dataset url & dataset filename
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year:04d}-{month:02d}.parquet'
    filename = os.path.basename(url)

    # if file already exists, skip download
    if os.path.isfile(filename+'.gz'):
        date_fmt = dt.datetime(year,month,1).strftime('%B, %Y')
        print(f'INFO | Reading {service} taxi trip data from {date_fmt}')
        df = pd.read_parquet(filename, engine='pyarrow')
        print('INFO | Done!')
        return df, filename
    
    # Fetch data from url
    date_fmt = dt.datetime(year,month,1).strftime('%B %Y')
    print(f'INFO | Fetching {service} taxi trip data from {date_fmt}')
    df = pd.read_parquet(url, engine='pyarrow')
    print('INFO | Done!')

    return df, filename

# Write a function to write a DataFrame to the local file system in parquet format
@task(
    log_prints=True, timeout_seconds=60*15,
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

# Write a function to upload a DataFrame to AWS S3
@task(
    log_prints=True, timeout_seconds=60*15
)
def upload_to_s3(filename: str) -> str:
    """Uploads a DataFrame to AWS S3"""
    # Get AWS credentials block
    s3_bucket = S3Bucket.load("s3-data-lake")
    
    # Upload file to AWS S3
    print(f'INFO | Uploading {filename} to AWS S3 bucket')
    s3_bucket_path = s3_bucket.put_directory(filename, 'trip-data')
    print('INFO | Done!')
    return s3_bucket_path

# Write a function to remove a file from the local file system
@task(
    log_prints=True, timeout_seconds=60*15
)
def remove_from_local(filename: str) -> None:
    """Removes a file from the local file system"""
    print(f'INFO | Deleting file:\t{filename}')
    os.remove(filename)
    print('INFO | Done!')


# Write a prefect flow to ingest NYC taxi trip data from the NYC Open Data API to AWS S3
@flow
def elt_web_to_s3(service: str, year: int, month: int):
    """Ingests NYC taxi trip data from the NYC Open Data API to AWS S3"""

    # Fetch dataset from NYC Open Data API
    df, filename = fetch_dataset(service, year, month)

    # Write dataset to local file system
    local_filename = write_to_local(df, filename)

    # Upload dataset to AWS S3
    s3_bucket_path = upload_to_s3(local_filename)

    # Remove dataset from local file system
    remove_from_local(local_filename)

    return s3_bucket_path


# Run the flow
if __name__ == '__main__':
    print()
    print('Running flow...')
    elt_web_to_s3.serve(
        name='ingest-nyc-taxi-data-to-s3',
        parameters=dict(
            service='green',
            year=2020,
            month=3
        )
    )
    print()