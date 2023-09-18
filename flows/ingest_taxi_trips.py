import os
import sys
import wget
import boto3
import shutil
import logging
import threading
import pandas as pd
import datetime as dt
from argparse import ArgumentParser
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pathlib import Path

# Configure and create logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Read environment variables
load_dotenv(Path(__file__).parent.parent / '.env')

# Create progress bar
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

# Get command line arguments
def get_args():
    """Parse command line arguments"""
    parser = ArgumentParser()
    parser.add_argument('--service', type=str, default='yellow', help='Taxi service type')
    parser.add_argument('--year', type=int, default=2023, help='Year of taxi trip data')
    parser.add_argument('--month', type=int, default=2, help='Month of taxi trip data')
    parser.add_argument('--start-year', type=int, default=None, dest='start_year', help='Start year of taxi trip data')
    parser.add_argument('--end-year', type=int, default=None, dest='end_year', help='Start month of taxi trip data')
    return parser.parse_args()

# Fetch dataset from url
def fetch_dataset(service: str, year: int, month: int) -> tuple[pd.DataFrame, str]:
    """Returns a DataFrame of taxi trip data for a given year and month"""
    # Construct dataset url & dataset filename
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year:04d}-{month:02d}.parquet'
    filename = os.path.basename(url)

    # Download data from url
    date_fmt = dt.datetime(year,month,1).strftime('%b, %Y')
    logger.info(f'Downloading {service} taxi trip data from {date_fmt}')
    print()
    wget.download(url, filename)
    print('\n')
    logger.info('Done!')

    return filename

# Compres dataset to gzip
def compress_dataset(filename: str) -> str:
    """Compress dataset to gzip"""
    # Construct compressed filename
    file_name = filename+'.gz'

    # Read dataset to DataFrame
    df = pd.read_parquet(filename)

    # Write DataFrame to gzip
    logger.info(f'Writing DataFrame to {file_name}')
    df.to_parquet(file_name, compression='gzip', index=False,)
    logger.info('Done!')

    return file_name

# Upload dataset to S3
def upload_to_s3(src_filename: str, dst_filename: str = None, bucket_name: str = None,) -> None:
    """Upload file to S3 bucket"""
    # Construct destination filename
    dst_filename = dst_filename or os.path.basename(src_filename)

    # Construct bucket name
    bucket_name = bucket_name or os.getenv('AWS_S3_BUCKET')

    # Create S3 client
    s3 = boto3.client('s3')

    # Upload file to S3 bucket
    logger.info(f'Uploading {src_filename} to s3://{bucket_name}/{dst_filename}')
    try:
        print()
        s3.upload_file(src_filename, bucket_name, dst_filename,
                       Callback=ProgressPercentage(src_filename))
        print('\n')
    except ClientError as e:
        logger.error(e)
        return False
    logger.info('Done!')

    return True

# Remove a local file from the file system
def remove_local_file(filename: str) -> None:
    """Remove a local file from the file system"""
    logger.info(f'Removing local file:\t{filename}')
    shutil.rmtree(filename, ignore_errors=True)
    logger.info('Done!')

# Ingest taxi trips for a given year and month
def ingest_taxi_trips(service: str, year: int, month: int) -> None:
    """Run pipeline to fetch, compress, and upload a taxi trip file to S3"""
    try:
        # Fetch dataset from url
        filename = fetch_dataset(service, year, month)

        # Compress dataset to gzip
        filepath = compress_dataset(filename)

        # Delete uncompressed dataset
        remove_local_file(filename)

        # Upload dataset to S3
        upload_to_s3(filepath, 'trip-data/'+filepath)

        # Delete compressed dataset
        remove_local_file(filepath)

    except Exception as e:
        logger.error(e)
        raise e
    
    finally:
        filename = f'{service}_tripdata_{year:04d}-{month:02d}.parquet'
        remove_local_file(filename)
        remove_local_file(filename+'.gz')
        logger.info('Flow complete!')

# Ingest taxi trips over a range of years and months
def ingest_several_taxi_trips(service: str, start_year: int, end_year: int) -> None:
    """Run pipeline to fetch, compress, and upload multiple taxi trip files to S3"""
    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            try:
                # Fetch dataset from url
                filename = fetch_dataset(service, year, month)
            
                # Compress dataset to gzip
                filepath = compress_dataset(filename)

                # Delete uncompressed dataset
                remove_local_file(filename)

                # Upload dataset to S3
                upload_to_s3(filepath, 'trip-data/'+filepath)

                # Delete compressed dataset
                remove_local_file(filepath)
            
            except Exception as e:
                logger.error(e)
                raise e
            
            finally:
                filename = f'{service}_tripdata_{year:04d}-{month:02d}.parquet'
                remove_local_file(filename)
                remove_local_file(filename+'.gz')
                logger.info('Flow complete!')
        
# Main function
def main():
    """Main function"""
    # Parse command line arguments
    args = get_args()

    if (args.start_year is None) and (args.end_year is None):
        # Run pipeline for a single month
        ingest_taxi_trips(args.service, args.year, args.month)
    else:
        # Run pipeline for a range of months
        ingest_several_taxi_trips(args.service, args.start_year, args.end_year)

    
    
    
if __name__ == '__main__':
    print()
    main()
    print()