"""Standalone: Load OSS parquet → Write to MaxCompute → Run dbt"""

import os, sys
import time
import oss2
import pandas as pd
import subprocess
from io import BytesIO
from datetime import datetime
from odps import ODPS, options

ACCESS_KEY_ID = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
ACCESS_KEY_SECRET = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
OSS_ENDPOINT = "https://oss-cn-hongkong.aliyuncs.com"
OSS_BUCKET = "techpulse-data-lake-hk-unique"
ODPS_PROJECT = "techpulse_dw"
ODPS_ENDPOINT = "http://service.cn-hongkong.maxcompute.aliyun.com/api"


def run():
    _start_time = time.time()
    auth = oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)

    ds = datetime.utcnow().strftime("%Y%m%d")
    prefix = f"processed_data/hn/ds={ds}/"
    files = [obj.key for obj in oss2.ObjectIterator(bucket, prefix=prefix) if obj.key.endswith(".parquet")]
    print(f"Found {len(files)} parquet files in {prefix}")

    if not files:
        print("No new data to process")
        return

    # Load all parquet files
    df_list = []
    for f in files:
        print(f"Reading: {f}")
        obj = bucket.get_object(f)
        df_list.append(pd.read_parquet(BytesIO(obj.read())))
    df = pd.concat(df_list, ignore_index=True)
    print(f"Total records: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Write to MaxCompute
    odps = ODPS(ACCESS_KEY_ID, ACCESS_KEY_SECRET, project=ODPS_PROJECT, endpoint=ODPS_ENDPOINT)
    table = odps.get_table("hn_raw")
    partition = f"ds={ds}"

    # Type coercion
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype("int64")
    df["id"] = df["id"].astype(str)
    df["title"] = df["title"].astype(str)
    df["url"] = df["url"].astype(str)
    df["author"] = df["author"].astype(str)
    df["type"] = df["type"].astype(str)
    for col in [
        "source", "ingest_time", "fetch_status", "html_oss_path",
        "content_oss_path", "content_excerpt",
        "ai_summary", "ai_insight", "tech_category"
    ]:
        if col in df.columns:
            df[col] = df[col].astype(str)
        else:
            df[col] = ""

    print(f"Writing to hn_raw partition: {partition}")
    options.tunnel.use_instance_tunnel = True
    with table.open_writer(partition=partition, create_partition=True, arrow=True) as writer:
        writer.write(df)
    print("MaxCompute write done!")

    # Run dbt
    dbt_path = "/home/src/techpulse_dbt"
    _dbt_duration = 0
    result = None
    if os.path.exists(dbt_path):
        print("Running dbt...")
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = dbt_path
        _dbt_start = time.time()
        result = subprocess.run(
            ["dbt-mc", "run"],
            cwd=dbt_path,
            env=env,
            capture_output=True,
            text=True
        )
        _dbt_duration = time.time() - _dbt_start
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise Exception("dbt failed")
        print("dbt done!")

        # dbt test — 数据质量校验
        print("Running dbt tests...")
        test_result = subprocess.run(
            ["dbt-mc", "test"],
            cwd=dbt_path,
            env=env,
            capture_output=True,
            text=True,
        )
        print(test_result.stdout)
        if test_result.returncode != 0:
            print(test_result.stderr)
            # tests 失败不阻断 pipeline（记录即可，避免假阳性阻塞生产）
            print(f"⚠️ dbt tests 失败，共 {test_result.stdout.count('FAILED')} 项未通过")
        else:
            print("✅ dbt tests 全部通过！")
    else:
        print(f"dbt path not found: {dbt_path}")

    print("Pipeline complete!")
    return {
        "sync_success": True,
        "sync_duration": time.time() - _start_time,
        "sync_rows": len(df),
        "dbt_success": result.returncode == 0 if result is not None else False,
        "dbt_duration": _dbt_duration,
    }


if __name__ == "__main__":
    run()
