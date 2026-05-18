import oss2
import pandas as pd
from io import BytesIO
from datetime import datetime
import os

# 流式输出到阿里云 OSS
@streaming_sink
def export_data(**kwargs):
    # 1. 从 Kafka 获取数据（固定写法，不会报错）
    data = kwargs.get("data", [])
    
    # 2. 从系统环境变量读取 AK/SK（这才是正确方式！）
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
    
    # 3. 转 DataFrame
    df = pd.DataFrame(data)
    
    # 4. 上传 OSS
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, "oss-cn-hongkong.aliyuncs.com", "techpulse-data-lake-hk-unique")
    
    now = datetime.now()
    path = f"raw_data/hn/{now.strftime('%Y-%m-%d')}/batch_{int(now.timestamp())}.parquet"
    
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow")
    bucket.put_object(path, buffer.getvalue())