import oss2
import pandas as pd
from io import BytesIO
import os
import sys

# 如果你需要在 Mage 里加载环境变量（可选）
# from mage_ai.settings import get_settings
# settings = get_settings()

# 阿里云凭证
access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

# 初始化OSS
auth = oss2.Auth(access_key_id, access_key_secret)
bucket = oss2.Bucket(
    auth,
    "https://oss-cn-hongkong.aliyuncs.com",
    "techpulse-data-lake-hk-unique"
)

# 读取文件
obj = bucket.get_object("raw_data/hn/2026-04-02/batch_1775125514.parquet")
buffer = BytesIO(obj.read())

df = pd.read_parquet(buffer)

# 输出结果
print("=== 实际字段 ===")
print(df.dtypes)
print("\n=== 前两行数据 ===")
print(df.head(2))