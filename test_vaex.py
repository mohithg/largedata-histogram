import boto3
import pandas as pd
import numpy as np
from io import StringIO
from scipy.stats import entropy
from datetime import datetime

S3_BUCKET = 'dmm-microbench'

s3 = boto3.client('s3', aws_access_key_id="AKIASVDNFDSGZYUVLQED", aws_secret_access_key="y8XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXre")

def download_s3_file(file_name, destination_file_name):
    s3.download_file(Bucket=S3_BUCKET, Key=file_name, Filename=destination_file_name)

def get_content(file_name, expression):
    return s3.select_object_content(
        Bucket=S3_BUCKET,
        Key=file_name,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization={'CSV': {"FileHeaderInfo": "Use"}},
        OutputSerialization={'CSV': {}},
    )


def convert_data_to_df(data, record_header):
    for event in data['Payload']:
        if 'Records' in event:
            record_header.append(event['Records']['Payload'])
    csv_content = ''.join(r.decode('utf-8').replace("\r", "") for r in record_header)
    csv_pd = pd.read_csv(StringIO(csv_content))

    print('\n##################################')
    print(f"Length of dataframe: {len(csv_pd)}")
    print(f"Memory usage of dataframe: \n {csv_pd.info(memory_usage='deep')}")
    print('\n##################################')

    return pd.DataFrame(csv_pd)

def convert_file_to_hdf5(file_name):
    import vaex
    vaex.from_csv(file_name, convert=True, chunk_size=500_000)

i = 1

download_s3_file(f"yellow_tripdata_2019-0{i}.csv", f"yellow_tripdata_2019-0{i}.csv")
    
convert_file_to_hdf5(f"yellow_tripdata_2019-0{i}.csv")