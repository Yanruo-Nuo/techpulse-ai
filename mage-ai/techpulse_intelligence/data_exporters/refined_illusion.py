import oss2
import pandas as pd
from io import BytesIO
from datetime import datetime

@data_exporter
def export_data(payload, *args, **kwargs):
    df = pd.DataFrame(payload)
    
    # 使用注入的环境变量
    auth = oss2.Auth(kwargs['ALIBABA_CLOUD_ACCESS_KEY_ID'], kwargs['ALIBABA_CLOUD_ACCESS_KEY_SECRET'])
    bucket = oss2.Bucket(auth, 'oss-cn-hongkong.aliyuncs.com', 'techpulse-data-lake-hk-unique')
    
    # 路径分区：raw/2023-10-27/data_ts.parquet
    now = datetime.now()
    path = f"raw_data/hn/{now.strftime('%Y-%m-%d')}/batch_{int(now.timestamp())}.parquet"
    
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow')
    bucket.put_object(path, buffer.getvalue())