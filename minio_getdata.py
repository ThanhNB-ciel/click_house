# import boto3
# # from botocore.client import Config

# s3  = boto3.resource(
#     's3',
#     endpoint_url ='http://192.168.1.21:2345/',
#     aws_access_key_id = 'minioadmin',
#     aws_secret_access_key ='minioadmin',
#     # config=Config(signature_version='s3v4'),
#     # region_name='us-east-1'  # or your preferred region
# )
# print("hello")
# # bucket = s3.Bucket('thanhnb')
# for bucket in s3.buckets.all():
#     print(bucket.name)

# # obj = bucket.Object('thanhnb/cdp/cdp_1/test12.csv')
# # body = obj.get()['Body'].read()
# # print(body)

import boto3
from io import StringIO
import pandas as pd
# from botocore.client import Config

s3  = boto3.client(
    's3',
    endpoint_url ='http://192.168.1.21:2345/',
    aws_access_key_id = 'minioadmin',
    aws_secret_access_key ='minioadmin',
    # config=Config(signature_version='s3v4'),
    # region_name='us-east-1'  # or your preferred region
)
# print("hello")
# bucket = s3.Bucket('thanhnb')

l_o = s3.list_objects_v2(Bucket="tes1", Prefix="cdp/cdp_1")
for o in l_o["Contents"]:
    print(o["Key"])

    obj = s3.get_object(Bucket="tes1", Key=o["Key"])["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(obj))
    print(df)