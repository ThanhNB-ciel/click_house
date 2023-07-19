import pandas as pd
import numpy as np
df = pd.read_csv('/home/thanhnb/ck.csv',index_col= 0)

# đẩy data import pandas as pd
from clickhouse_driver import Client
import csv
clickhouse_info = {
    "host": "103.119.132.171",
    "user": "default",
    "password": "",
    'port' : "8123"
}
client = Client(host=clickhouse_info['host'], port=clickhouse_info['port'], user=clickhouse_info['user'],
                   password=clickhouse_info['password'], settings={
    'use_numpy': True}
)

# client = Client(host ='localhost', port=9002, user="default", settings ={'use_numpy':True})
client.insert_dataframe("insert into thanhnb.chung_khoan values",df)