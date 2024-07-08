import os
import sys
import pandas as pd
import requests
import boto3
from botocore.exceptions import NoCredentialsError

def download_file(url, local_filename):
    print(f"Downloading file from {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"File downloaded to {local_filename}")
    return local_filename

def upload_to_s3(local_filename, bucket, s3_filename):
    s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'))
    try:
        print(f"Uploading {local_filename} to s3://{bucket}/{s3_filename}")
        s3.upload_file(local_filename, bucket, s3_filename)
        print(f"File uploaded to s3://{bucket}/{s3_filename}")
    except NoCredentialsError:
        print("Credentials not available")

def check_file_in_s3(bucket, s3_filename):
    s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'))
    try:
        s3.head_object(Bucket=bucket, Key=s3_filename)
        print(f"File s3://{bucket}/{s3_filename} exists in S3")
        return True
    except:
        print(f"File s3://{bucket}/{s3_filename} does not exist in S3")
        return False

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

def read_data(filename, categorical):
    options = {}
    if 'S3_ENDPOINT_URL' in os.environ:
        options['storage_options'] = {'client_kwargs': {'endpoint_url': os.getenv('S3_ENDPOINT_URL')}}

    print(f"Reading data from {filename}")
    df = pd.read_parquet(filename, **options)
    # Add your data processing logic here
    return df

def main(year, month):
    input_url = get_input_path(year, month)
    output_file = get_output_path(year, month)

    # Download the file from the internet if input_url is an HTTP URL
    local_input_file = f'yellow_tripdata_{year:04d}-{month:02d}.parquet'
    if input_url.startswith('http'):
        download_file(input_url, local_input_file)
        
        # Upload the file to S3
        s3_bucket = 'nyc-duration'
        s3_input_file = f'in/{year:04d}-{month:02d}.parquet'
        upload_to_s3(local_input_file, s3_bucket, s3_input_file)

        # Ensure the file is available in S3
        if not check_file_in_s3(s3_bucket, s3_input_file):
            print(f"File not found in S3: s3://{s3_bucket}/{s3_input_file}")
            sys.exit(1)

        # Use the S3 input file path for further processing
        input_file = f's3://{s3_bucket}/{s3_input_file}'
    else:
        # Use the provided S3 path directly
        input_file = input_url

    categorical = ['PULocationID', 'DOLocationID']
    df = read_data(input_file, categorical)

    # Additional processing code here

if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)
