import os
import sys
import threading
import logging
import boto3
import wget
from botocore.exceptions import ClientError
from argparse import ArgumentParser

import pandas as pd
import datetime as dt


class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

def parse_args():
    """Parse command line arguments"""
    parser = ArgumentParser()
    parser.add_argument('--service', type=str, default='yellow', help='Taxi service type')
    parser.add_argument('--year', type=int, default=2019, help='Year of taxi trip data')
    parser.add_argument('--month', type=int, default=1, help='Month of taxi trip data')
    return parser.parse_args()

def download_dataset(service: str, year: int, month: int) -> str:
    """Returns a DataFrame of taxi trip data for a given year and month"""
    # Construct dataset url & dataset filename
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year:04d}-{month:02d}.parquet'
    filename = os.path.basename(url)

    # Download data from url
    date_fmt = dt.datetime(year,month,1).strftime('%B %Y')
    print(f'INFO | Downloading {service} taxi trip data from {date_fmt}')
    wget.download(url, filename)
    print('INFO | Done!')

    return filename

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

def write_to_local(df: pd.DataFrame, filename: str) -> str:
    """Writes a DataFrame to the local file system in parquet format"""
    # Append .gz suffix to filename ot represent compressed file
    filename = f'{filename}.gz'
    
    # Store file in local file system
    print(f'INFO | Storing pandas.DataFrame to local file:\t{filename}')
    df.to_parquet(filename, index=False, compression='gzip')
    print('INFO | Done!')
    return filename

def upload_file(file_name: str, bucket: str, object_name: str = None):
    """Upload a file to an S3 bucket

    Args:
        file_name: File to upload
        bucket: Bucket to upload to
        object_name: S3 object name. If not specified then file_name is used
    
    Return:
        True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    object_name = object_name or os.path.basename(file_name)
    
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(
            file_name, bucket, object_name,
            Callback=ProgressPercentage(file_name)
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def main():
    """Main function"""
    # Parse command line arguments
    args = parse_args()

    # Fetch data from url
    df, filename = fetch_dataset(args.service, args.year, args.month)

    # Write the data to local file system
    filename = write_to_local(df, filename)

    # Upload the data to S3
    upload_file(filename, 'nyc-taxi-trip', 'trip-data/'+filename)

    # Clean up the local file system
    os.remove(filename)
    

if __name__ == '__main__':
    print()
    main()
    print()