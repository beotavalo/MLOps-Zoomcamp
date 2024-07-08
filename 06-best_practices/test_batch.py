import os
import sys
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError

def check_file_in_s3(bucket, s3_filename):
    s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'),
                      aws_access_key_id='test',
                      aws_secret_access_key='test')
    try:
        s3.head_object(Bucket=bucket, Key=s3_filename)
        print(f"File s3://{bucket}/{s3_filename} exists in S3")
        return True
    except:
        print(f"File s3://{bucket}/{s3_filename} does not exist in S3")
        return False

def get_input_path(year, month):
    default_input_pattern = 's3://nyc-duration/in/{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

def read_data(filename, categorical):
    options = {
        'storage_options': {
            'client_kwargs': {
                'endpoint_url': os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'),
                'aws_access_key_id': 'test',
                'aws_secret_access_key': 'test'
            }
        }
    }

    print(f"Reading data from {filename}")
    df = pd.read_parquet(filename, **options)
    # Add your data processing logic here
    return df

def save_predictions_to_s3(df, bucket, s3_filename):
    local_output_file = '/tmp/predictions.parquet'
    df.to_parquet(local_output_file)
    s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'),
                      aws_access_key_id='test',
                      aws_secret_access_key='test')
    try:
        print(f"Uploading predictions to s3://{bucket}/{s3_filename}")
        s3.upload_file(local_output_file, bucket, s3_filename)
        print(f"Predictions uploaded to s3://{bucket}/{s3_filename}")
    except NoCredentialsError:
        print("Credentials not available")
    os.remove(local_output_file)

def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    # Ensure the file is available in S3
    s3_bucket = 'nyc-duration'
    s3_input_file = f'in/{year:04d}-{month:02d}.parquet'
    if not check_file_in_s3(s3_bucket, s3_input_file):
        print(f"File not found in S3: s3://{s3_bucket}/{s3_input_file}")
        sys.exit(1)

    categorical = ['PULocationID', 'DOLocationID']
    df = read_data(input_file, categorical)

    # Example prediction logic: Add a dummy column 'prediction' with a constant value
    df['prediction'] = 1.0

    # Save predictions back to S3
    save_predictions_to_s3(df, 'nyc-duration', f'taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet')

if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)
