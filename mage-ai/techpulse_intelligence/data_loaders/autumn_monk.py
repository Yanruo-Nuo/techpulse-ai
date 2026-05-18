import oss2
import pandas as pd
from io import BytesIO
import os

access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

auth = oss2.Auth(access_key_id, access_key_secret)
bucket = oss2.Bucket(auth, "https://oss-cn-hongkong.aliyuncs.com", "techpulse-data-lake-hk-unique")

# 读取其中一个文件
obj = bucket.get_object("raw_data/hn/2026-04-02/batch_1775125514.parquet")
buffer = BytesIO(obj.read())

df = pd.read_parquet(buffer)
print("=== 实际字段 ===")
print(df.dtypes)
print("\n=== 前两行数据 ===")
print(df.head(2))