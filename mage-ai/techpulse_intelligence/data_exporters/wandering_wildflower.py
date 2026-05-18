import oss2
import pandas as pd
from io import BytesIO
from datetime import datetime

@streaming_sink
def export_data(data, *args, **kwargs):  # 这里必须用 data，不是 payload
    df = pd.DataFrame(data)  # data 就是从 Kafka 来的消息
    
    # 阿里云 OSS 认证
    auth = oss2.Auth(
        kwargs['ALIBABA_CLOUD_ACCESS_KEY_ID'],
        kwargs['ALIBABA_CLOUD_ACCESS_KEY_SECRET']
    )
    bucket = oss2.Bucket(auth, 'oss-cn-hongkong.aliyuncs.com', 'techpulse-data-lake-hk-unique')
    
    # 生成 OSS 路径
    now = datetime.now()
    path = f"raw_data/hn/{now.strftime('%Y-%m-%d')}/batch_{int(now.timestamp())}.parquet"
    
    # 写入 parquet 并上传
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow')
    bucket.put_object(path, buffer.getvalue())