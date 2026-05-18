import os
import pandas as pd
from odps import ODPS, options

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def write_to_maxcompute(data, *args, **kwargs):
    df = data["df"]
    ds = data["ds"]

    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    odps = ODPS(
        access_key_id,
        access_key_secret,
        project="techpulse_dw",
        endpoint="http://service.cn-hongkong.maxcompute.aliyun.com/api"
    )

    table = odps.get_table("hn_raw")
    partition = f"ds={ds}"

    print(f"🚀 写入分区: {partition}")

    # ✅ 开启 Arrow
    options.tunnel.use_instance_tunnel = True

    # =============================
    # ⭐ 类型标准化（必须在写入前完成）
    # =============================
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype("int64")

    df["id"] = df["id"].astype(str)
    df["title"] = df["title"].astype(str)
    df["url"] = df["url"].astype(str)
    df["author"] = df["author"].astype(str)
    df["type"] = df["type"].astype(str)

    # 可选字段
    for col in [
        "source", "ingest_time", "fetch_status", "html_oss_path",
        "content_oss_path", "content_excerpt",
        "ai_summary", "ai_insight", "tech_category"
    ]:
        df[col] = df[col].astype(str)

    # =============================
    # ⭐ 写入（只执行一次！）
    # =============================
    with table.open_writer(
        partition=partition,
        create_partition=True,
        arrow=True
    ) as writer:
        writer.write(df)

    print("✅ 写入成功")

    return data