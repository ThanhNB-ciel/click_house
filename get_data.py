import pandas as pd
from clickhouse_driver import Client
from datetime import date, timedelta

clickhouse_info = {
    "host" : "103.119.132.171",
    "user" : "default",
    "password" : ""
}
client = Client(host = clickhouse_info['host'],user = clickhouse_info['user'], port=9002,
                password = clickhouse_info['password'],settings={'user_numpy': True})
data = client.query_dataframe(f"""
                              select * 
                            from thanhnb.diemthi_lop10
                            """)
print(data.head())