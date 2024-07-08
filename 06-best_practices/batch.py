#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
year = int(sys.argv[1])
month = int(sys.argv[2])

bucket = 'nyc-duration'
input_key = f'in/{year:04d}-{month:02d}.parquet'
output_file = f'output/yellow_tripdata_{year:04d}-{month:02d}.parquet'


def read_from_s3(bucket, key):
    options = {
        'client_kwargs': {
            'endpoint_url': 'http://localhost:4566',
            'aws_access_key_id': 'test',
            'aws_secret_access_key': 'test'
        }
    }
    
    s3_path = f"s3://{bucket}/{key}"
    print(f"Reading data from {s3_path}")
    df = pd.read_parquet(s3_path, storage_options=options)
    
    return df


    
# Read the data from S3
#df = read_from_s3(bucket, input_key)

#input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
#output_file = f'output/yellow_tripdata_{year:04d}-{month:02d}.parquet'


with open('model.bin', 'rb') as f_in:
    dv, lr = pickle.load(f_in)


categorical = ['PULocationID', 'DOLocationID']

def read_data(input_key):
    #df = pd.read_parquet(filename)
    df = read_from_s3(bucket, input_key)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


df = read_data(input_key)
df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = lr.predict(X_val)


print('predicted mean duration:', y_pred.mean())


df_result = pd.DataFrame()
df_result['ride_id'] = df['ride_id']
df_result['predicted_duration'] = y_pred


df_result.to_parquet(output_file, engine='pyarrow', index=False)
