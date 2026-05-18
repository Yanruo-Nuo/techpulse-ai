import os
import oss2
import pandas as pd
from io import BytesIO

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def read_parquet_from_oss(data, *args, **kwargs):
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    bucket = oss2.Bucket(
        oss2.Auth(access_key_id, access_key_secret),
        "https://oss-cn-hongkong.aliyuncs.com",
        "techpulse-data-lake-hk-unique"
    )

    files = data["files"]
    ds = data["ds"]

    df_list = []

    for file in files:
        print(f"📥 读取: {file}")
        obj = bucket.get_object(file)
        df = pd.read_parquet(BytesIO(obj.read()))
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)

    print(f"📊 总数据量: {len(df)}")

    return {
        "df": df,
        "ds": ds
    }