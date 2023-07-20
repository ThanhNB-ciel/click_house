import pandas as pd
import boto3
from io import StringIO 
import csv
from minio import Minio
import datetime
from io import BytesIO


# data = pd.read_csv('/home/thanhnb/school_10_2019.csv',sep="|", quotechar='"') 



file_path = '/home/thanhnb/export_lop10_new.csv'


with open(file_path, "r", encoding="utf-8") as f:
    data = f.read()
    

client = boto3.client(
        "s3",
        endpoint_url='http://192.168.1.21:2345',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
    )
client.put_object(
            Bucket='thanhnb',
            Key='cdp/cdp_1/export_lop10_new.csv',
            Body=data,
        )

print("ok") 


