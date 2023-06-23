import pandas as pd
import numpy as np
from clickhouse_driver import Client

# _max =45
# dfs = []
# for year in range(2015, 2021):
#     df = pd.read_csv(f"/home/thanhnb/school_10_{year}.csv", names=[str(i) for i in range(_max)], header=None)
#     df = df.fillna("")
#     df[[str(i) for i in range(7, _max)]] = df[[str(i) for i in range(7, _max)]].astype(str)
#     df["note"] = df[[str(i) for i in range(7, _max)]].sum(axis=1)

#     df2 = df[[*[str(i) for i in range(6)], "note"]]
#     df2 = df2.iloc[1:]
#     df2.columns = ["city", "STT", "schoolName", "NV1", "NV2", "NV3", "Note"]
#     df2['year'] = year
#     dfs.append(df2)
#     data = pd.concat(dfs,ignore_index = False )
#     data['NV1'] = pd.to_numeric(data['NV1'], errors='coerce').fillna('')
#     columns_to_convert = ['NV1','NV2','NV3']
#     data[columns_to_convert] = data[columns_to_convert].replace("", np.NaN)
#     data[columns_to_convert] = data[columns_to_convert].astype(float)

# # print(data)   


# # client = Client(host ='localhost', port=9002, user="default", settings ={'use_numpy':True})
# # client.insert_dataframe("insert into thanhnb.diemchuan_lop10 values",data)
# _max_1 = 8
# df_22 = pd.read_csv('/home/thanhnb/school_10_2022.csv',names=[str(i) for i in range(_max_1)], header=None)
# for i in df_22[df_22[2].isnull() & df_22[1].isnull()].index.to_list():
#     df_22.iloc[i - 1][6] = df_22.iloc[i - 1][6] + " " + df.iloc[i][0] 
    
# df_22 = df_22.dropna(subset =[1])
# print(df_22)
# quit()
    
# df_22 = df_22.fillna("")
# df_22[[str(i) for i in range(7, _max_1)]] = df_22[[str(i) for i in range(7, _max_1)]].astype(str)
# df_22["note"] = df[[str(i) for i in range(7, _max_1)]].sum(axis=1)
# df_22 = df_22[[*[str(i) for i in range(6)],'note']]
# df_22 = df_22.iloc[1:]
# df_22.columns = ["city", "STT", "schoolName", "NV1", "NV2", "NV3", "Note"]

# # df_22['STT'] = pd.to_numeric(df_22['STT'], errors='coerce').fillna('')

# columns_to_convert_1 = ['NV1','NV2','NV3']
# df_22[columns_to_convert_1] = df_22[columns_to_convert_1].replace("", np.NaN)
# df_22[columns_to_convert_1] = df_22[columns_to_convert_1].astype(float)


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

