import os
import oss2
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def list_oss_files(*args, **kwargs):
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    bucket = oss2.Bucket(
        oss2.Auth(access_key_id, access_key_secret),
        "https://oss-cn-hongkong.aliyuncs.com",
        "techpulse-data-lake-hk-unique"
    )

    ds = datetime.utcnow().strftime("%Y%m%d")
    prefix = f"processed_data/hn/ds={ds}/"

    files = [
        obj.key for obj in oss2.ObjectIterator(bucket, prefix=prefix)
        if obj.key.endswith(".parquet")
    ]

    print(f"✅ 找到 {len(files)} 个文件")

    return {
        "files": files,
        "ds": ds
    }