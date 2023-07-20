import pandas as pd
import numpy as np
from clickhouse_driver import Client


year = 2022
df = pd.read_csv(f"/home/thanhnb/school_10_{year} (1).csv", names=range(8))
df = df.drop(columns=[7])

for i in df[df[2].isnull() & df[1].isnull()].index.to_list():
    df.iloc[i - 1][6] = df.iloc[i - 1][6] + " " + df.iloc[i][0]

df = df.dropna(subset=[1])

for i in df[df[2].isnull()].index.to_list():
    df.iloc[i - 1][2] = df.iloc[i - 1][2] + " " + df.iloc[i][0]
    df.iloc[i - 1][3] = df.iloc[i][1]
    df.iloc[i - 1][4] = df.iloc[i][2]
    df.iloc[i - 1][5] = df.iloc[i][3]
    df.iloc[i - 1][6] = df.iloc[i][4]

df[7] = year

df = df.dropna(subset=[2])
_max_1 = 8

df = df[1:]
df["note"] = df[[i for i in range(6, _max_1)]].sum(axis=1)

print(df)

client = Client(host ='localhost', port=9002, user="default", settings ={'use_numpy':True})
client.insert_dataframe("insert into thanhnb.diemchuan_lop10 values",df)
# df.to_csv(
#     "test_thanhnb.csv",
#     index=False,
#     columns=["City", "STT", "schoolName", "NV1", "NV2", "NV3", "Note"],
# )


# client = Client(host ='localhost', port=9002, user="default", settings ={'use_numpy':True})
# client.insert_dataframe("insert into thanhnb.diemchuan_lop10 values",df_22)

