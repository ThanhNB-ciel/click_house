import pandas as pd
import boto3
from io import StringIO 
# data = pd.read_csv('/home/thanhnb/test.csv')
with open("/home/thanhnb/test.csv", "r", encoding="utf-8") as f:
    data = f.read()
# print(data.head())
# csv_buffer = StringIO()

# data.to_csv(csv_buffer)

client = boto3.client(
        "s3",
        endpoint_url='http://192.168.1.21:2345',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
    )
client.put_object(
            Bucket='thanhnb',
            Key='cdp/cdp_1/test12.csv',
            Body=data,
        )

print("ok")