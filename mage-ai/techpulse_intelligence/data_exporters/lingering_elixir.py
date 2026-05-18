import oss2
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
from mage_ai.streaming.sinks.base_python import BasePythonSink
from typing import Callable, Dict, List

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink


@streaming_sink
class CustomSink(BasePythonSink):
    def init_client(self):
        """
        初始化阿里云 OSS 客户端
        """
        access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
        access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

        if not access_key_id or not access_key_secret:
            raise ValueError("环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET 未设置")

        auth = oss2.Auth(access_key_id, access_key_secret)
        self.bucket = oss2.Bucket(
            auth,
            "https://oss-cn-hongkong.aliyuncs.com",
            "techpulse-data-lake-hk-unique"
        )
        print("✅ OSS 连接初始化成功")

    def batch_write(self, messages: List[Dict]):
        """
        批量写入 OSS（Parquet 格式）
        """
        if not messages:
            print("ℹ️ 空数据，跳过")
            return

        print(f"📥 本次处理数据条数: {len(messages)}")

        # 转 DataFrame
        df = pd.DataFrame(messages)

        # 生成存储路径
        now = datetime.now()
        path = (
            f"raw_data/hn/{now.strftime('%Y-%m-%d')}/"
            f"batch_{int(now.timestamp())}.parquet"
        )

        # 写入 Parquet 到内存
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        # 上传 OSS
        self.bucket.put_object(path, buffer.getvalue())
        print(f"✅ 上传成功: {path}")