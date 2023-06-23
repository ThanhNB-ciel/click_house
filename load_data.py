import pandas as pd 
from clickhouse_driver import Client
client = Client(host ='localhost', port=9002, user="default", settings ={'use_numpy':True})

# data = pd.read_csv('school_mark_2017_final.csv')
# data.columns=['tinh_id','ten_tinh','loai_truong','ma_truong','ten_truong','stt','nganh_hoc','ma_nganh','khoi_thi','diem_chuan','ghi_chu']
# client.execute(
#     """create table thanhnb.diemthi_lop10 (
#         tinh_id Int, ten_tinh String , loai_truong Int , ma_truong String, ten_truong String, stt Int, nganh_hoc String, ma_nganh String, khoi_thi String, diem_chuan String , ghi_chu String
#         )
#         ENGINE = MergeTree
#         ORDER BY tinh_id
#     """
# )


# client.insert_dataframe("insert into thanhnb.diemthi_lop10 values",data)
# print(data.head())
for year in range(2015,2023):
    data = pd.read_csv('/home/thanhnb/school_10_{year}.csv',delimiter=';')
print(data)