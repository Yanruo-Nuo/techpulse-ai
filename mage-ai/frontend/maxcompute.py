"""MaxCompute 数据加载（从 app.py 抽取）"""

import os
import time
import streamlit as st
import pandas as pd
from odps import ODPS
from metrics_collector import mc_query_scanned_bytes, mc_query_duration


def get_odps():
    return ODPS(
        os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID'),
        os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET'),
        project=os.getenv('MAXCOMPUTE_PROJECT'),
        endpoint=os.getenv('MAXCOMPUTE_ENDPOINT')
    )


CACHE_VERSION = "v2"  # 缓存版本号，数据管道变更时递增以强制刷新


@st.cache_data(ttl=600, show_spinner="📊 加载趋势数据...")
def load_trend_data():
    """从 dbt mart 层加载趋势数据"""
    try:
        o = get_odps()
        sql = """
            SELECT ds, tech_category, daily_cnt
            FROM mart_trend_analysis
            WHERE ds IS NOT NULL
            ORDER BY ds ASC
        """
        _query_start = time.time()
        with o.execute_sql(sql, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
            df = reader.to_pandas()
            df['ds'] = pd.to_datetime(df['ds'], errors='coerce')
            df = df.dropna(subset=['ds'])
        _query_dur = time.time() - _query_start
        mc_query_duration.observe(_query_dur)
        mc_query_scanned_bytes.inc(len(df) * 512)  # rough estimate
        return df
    except Exception as e:
        st.error(f"❌ 趋势数据加载失败：{str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner="📰 加载新闻数据...")
def load_news_data(_version=CACHE_VERSION):
    """从 dbt fact_article 加载去重后的新闻数据，若不可用降级到 hn_raw"""
    try:
        o = get_odps()
        # 优先查询 fact_article（dbt marts 层）
        sql = """
            SELECT
                article_id AS id,
                title,
                url,
                ai_summary,
                ai_insight,
                tech_category,
                source,
                ds,
                score,
                article_created_at AS ingest_time
            FROM fact_article
            WHERE ds IS NOT NULL
            ORDER BY article_created_at DESC
        """
        _query_start = time.time()
        with o.execute_sql(sql, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
            result = reader.to_pandas()
        _query_dur = time.time() - _query_start
        mc_query_duration.observe(_query_dur)
        mc_query_scanned_bytes.inc(len(result) * 512)
        return result
    except Exception:
        # 降级: fact_article 不存在时查 hn_raw
        try:
            o2 = get_odps()
            sql_fallback = """
                SELECT id, title, url, score, ai_summary, ai_insight,
                       tech_category, COALESCE(source, 'hackernews') AS source,
                       ingest_time, ds
                FROM (
                    SELECT id, title, url, score, ai_summary, ai_insight,
                           tech_category, source, ingest_time, ds,
                           ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingest_time DESC) AS rn
                    FROM hn_raw
                    WHERE ds IS NOT NULL
                ) t
                WHERE rn = 1
                ORDER BY ingest_time DESC
            """
            _query_start = time.time()
            with o2.execute_sql(sql_fallback, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
                result = reader.to_pandas()
            _query_dur = time.time() - _query_start
            mc_query_duration.observe(_query_dur)
            mc_query_scanned_bytes.inc(len(result) * 512)
            return result
        except Exception as e2:
            st.error(f"❌ 新闻数据加载失败（主备均不可用）：{str(e2)}")
            return pd.DataFrame()
