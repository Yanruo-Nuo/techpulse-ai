import os
import time
import oss2
import pandas as pd
from io import BytesIO
from datetime import datetime
from typing import Dict, List
from mage_ai.streaming.sinks.base_python import BasePythonSink

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

@streaming_sink
class UnifiedSink(BasePythonSink):
    def init_client(self):
        # OSS 初始化
        access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
        access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
        endpoint = "https://oss-cn-hongkong.aliyuncs.com"
        bucket_name = "techpulse-data-lake-hk-unique"
        auth = oss2.Auth(access_key_id, access_key_secret)
        self.bucket = oss2.Bucket(auth, endpoint, bucket_name)
        print("✅ OSS 初始化成功")

    def batch_write(self, messages: List[Dict]):
        if not messages:
            print("⏳ 当前无消息，跳过")
            return

        # 数据格式化
        df = pd.DataFrame(messages)
        now = datetime.utcnow()
        ds = now.strftime("%Y%m%d")
        timestamp = int(now.timestamp() * 1000)

        table_columns = [
            "id", "source", "title", "url", "author", "score", "type",
            "ingest_time", "fetch_status", "html_oss_path",
            "content_oss_path", "content_excerpt",
            "ai_summary", "ai_insight", "tech_category"
        ]

        for col in table_columns:
            if col not in df.columns:
                df[col] = ""
        df = df[table_columns]

        # ===================== 写入 OSS =====================
        try:
            # 1. 写入 Parquet
            parquet_path = f"processed_data/hn/ds={ds}/batch_{timestamp}.parquet"
            buffer = BytesIO()
            df.to_parquet(buffer, engine="pyarrow", index=False)
            self.bucket.put_object(parquet_path, buffer.getvalue())
            print(f"✅ Parquet 写入成功：{parquet_path}，记录数：{len(df)}")

            # 2. 写入 CSV 备份
            csv_path = f"processed_data/hn/ds={ds}/batch_{timestamp}.csv"
            buffer = BytesIO()
            df.to_csv(buffer, index=False)
            self.bucket.put_object(csv_path, buffer.getvalue())
            print(f"✅ CSV 备份写入成功：{csv_path}")

            # 3. 写入元数据
            import json
            metadata = {
                "batch_id": timestamp,
                "ds": ds,
                "record_count": len(df),
                "parquet_path": parquet_path,
                "csv_path": csv_path,
                "columns": list(df.columns),
                "timestamp": now.isoformat()
            }
            meta_path = f"processed_data/hn/ds={ds}/batch_{timestamp}_metadata.json"
            self.bucket.put_object(meta_path, json.dumps(metadata, indent=2, ensure_ascii=False))
            print(f"✅ 元数据写入成功：{meta_path}")

        except Exception as e:
            # 失败写入死信队列
            dlq_path = f"dlq/hn/failed_{timestamp}.parquet"
            try:
                buffer = BytesIO()
                df.to_parquet(buffer, engine="pyarrow", index=False)
                self.bucket.put_object(dlq_path, buffer.getvalue())
                print(f"✅ 失败数据已保存至DLQ：{dlq_path}")
            except Exception as dlq_error:
                print(f"❌ DLQ 写入也失败了：{str(dlq_error)}")
            
            print(f"❌ OSS 写入失败")
            print(f"错误详情：{str(e)}")
            raise Exception(f"写入失败，已保存DLQ：{str(e)}")

    @property
    def batch_size(self) -> int:
        return 10

    @property
    def batch_flush_interval(self) -> int:
        return 5