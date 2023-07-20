import boto3
from io import StringIO
import pandas as pd
# from botocore.client import Config

s3  = boto3.client(
    's3',
    endpoint_url ='http://192.168.1.21:2345/',   # connect to minio
    aws_access_key_id = 'minioadmin',
    aws_secret_access_key ='minioadmin',
    # config=Config(signature_version='s3v4'),
    # region_name='us-east-1'  # or your preferred region
)
# print("hello")
# bucket = s3.Bucket('thanhnb')

l_o = s3.list_objects_v2(Bucket="thanhnb", Prefix="cdp/cdp_1")
data =[]
keys =[]
for o in l_o["Contents"]:
    t = o["Key"].split("/")
    key = t[-1].split(".")[0]   
    keys.append(key)
    print(keys)
    # print(o["Key"])

    obj = s3.get_object(Bucket="thanhnb", Key=o["Key"])["Body"].read().decode("utf-8")  # lấy dữ liệu từ minio
    df = pd.read_csv(StringIO(obj))
    data.append(df)
    # print(data)
    
for dfs in range(len(data)):
    data[dfs].to_csv(f'{keys[dfs]}.csv')   # in dữ liệu ra file csv

for dfx in data:
    client.insert_dataframe("insert into thanhnb.test_minio values",dfs.astype(str))  # đẩy dữ liệu vào clickhouse

    
    

    