#Script for integration test
import os
import pandas as pd
from datetime import datetime
import boto3

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def prepare_data(df, categorical):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def create_test_data():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)
    
    categorical = ['PULocationID', 'DOLocationID']
    
    df_prepared = prepare_data(df, categorical)
    
    return df_prepared

def save_to_s3(df, bucket, key):
    options = {
        'client_kwargs': {
            'endpoint_url': 'http://localhost:4566',
            'aws_access_key_id': 'test',
            'aws_secret_access_key': 'test'
        }
    }
    
    df.to_parquet(
        key,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

def main():
    bucket = 'nyc-duration'
    key = 's3://nyc-duration/in/2023-01.parquet'
    
    df = create_test_data()
    
    save_to_s3(df, bucket, key)

if __name__ == "__main__":
    main()

