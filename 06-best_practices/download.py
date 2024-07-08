import boto3
import requests
import os
from datetime import datetime, timedelta

# LocalStack endpoint configuration
localstack_endpoint_url = 'http://localhost:4566'  # Adjust port if necessary

# AWS credentials setup for LocalStack
aws_access_key_id = 'test'  # LocalStack default access key ID
aws_secret_access_key = 'test'  # LocalStack default secret access key
aws_region_name = 'us-east-1'

# S3 bucket and folder setup for LocalStack
bucket_name = 'nyc-duration'
folder_prefix = 'in/'

# Function to download and upload Parquet file to LocalStack S3
def download_and_upload_to_s3(url, s3_key):
    filename = url.split('/')[-1]
    local_file_path = f'/tmp/{filename}'  # Temporarily store file locally

    # Download file from URL
    print(f'Downloading {filename}...')
    with requests.get(url, stream=True) as r:
        if r.status_code != 200:
            raise ValueError(f"Failed to download {url}. HTTP status code {r.status_code}")
        
        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    # Upload file to LocalStack S3
    print(f'Uploading {filename} to LocalStack S3...')
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=localstack_endpoint_url,
                                 aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key,
                                 region_name=aws_region_name
                                 )
        s3_client.create_bucket(Bucket=bucket_name)  # Create bucket if not exists
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f'{filename} uploaded successfully to LocalStack S3.')

    except Exception as e:
        print(f"Failed to upload {filename} to LocalStack S3: {str(e)}")

    finally:
        # Clean up local file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

# Main function to iterate over months and years
def main():
    # Iterate over months and years from January 2024 to May 2023
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 5, 1)
    delta = timedelta(days=30)  # Assuming monthly intervals

    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        # Example URL format, replace with your actual URL pattern
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'

        # S3 key format
        s3_key = f'{folder_prefix}{year}-{month:02d}.parquet'

        # Download and upload to LocalStack S3
        try:
            download_and_upload_to_s3(url, s3_key)
        
        except Exception as e:
            print(f"Failed to process {year}-{month:02d}: {str(e)}")

        # Move to the previous month
        current_date += delta

if __name__ == '__main__':
    main()

