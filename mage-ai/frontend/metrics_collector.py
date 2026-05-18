"""Frontend Prometheus metrics — data quality collector + MC query + page load

Starts an HTTP server on port 8003 for Prometheus scraping.
Can be imported by app.py or run standalone for testing.

Metrics are created safely (idempotent) to handle Streamlit script re-execution.
"""
import os
import time
import threading
import logging
from prometheus_client import start_http_server, Gauge, Counter, Histogram, REGISTRY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _gauge(name, doc, labelnames=()):
    """Create or retrieve a Gauge — safe for module re-imports."""
    if name in REGISTRY._names_to_collectors:
        return REGISTRY._names_to_collectors[name]
    return Gauge(name, doc, labelnames)


def _counter(name, doc, labelnames=()):
    """Create or retrieve a Counter — safe for module re-imports."""
    if name in REGISTRY._names_to_collectors:
        return REGISTRY._names_to_collectors[name]
    return Counter(name, doc, labelnames)


def _histogram(name, doc, labelnames=(), buckets=None):
    """Create or retrieve a Histogram — safe for module re-imports."""
    if name in REGISTRY._names_to_collectors:
        return REGISTRY._names_to_collectors[name]
    kwargs = {"buckets": buckets} if buckets else {}
    return Histogram(name, doc, labelnames, **kwargs)


# --- Data Quality Gauges ---
dq_ai_summary_missing_ratio = _gauge(
    'dq_ai_summary_missing_ratio', 'Ratio of articles missing AI summary'
)
dq_ai_category_missing_ratio = _gauge(
    'dq_ai_category_missing_ratio', 'Ratio of articles missing AI category'
)
dq_category_distribution = _gauge(
    'dq_category_distribution', 'Article count per category',
    ['category']
)
dq_others_category_ratio = _gauge(
    'dq_others_category_ratio', 'Ratio of articles in Others category'
)
dq_empty_content_ratio = _gauge(
    'dq_empty_content_ratio', 'Empty content ratio per source',
    ['source']
)
dq_duplicate_urls_24h = _gauge(
    'dq_duplicate_urls_24h', 'Duplicate URLs blocked in last 24h'
)

# --- MC Query Metrics ---
mc_query_scanned_bytes = _counter(
    'mc_query_scanned_bytes', 'MaxCompute bytes scanned'
)
mc_query_duration = _histogram(
    'mc_query_duration', 'MaxCompute query duration in seconds',
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60)
)

# --- Page Load Metrics ---
frontend_page_load_duration = _histogram(
    'frontend_page_load_duration', 'Page load duration in seconds',
    ['page'],
    buckets=(0.1, 0.5, 1, 2, 5, 10)
)

# --- Pipeline E2E Latency ---
pipeline_e2e_latency_seconds = _gauge(
    'pipeline_e2e_latency_seconds', 'End-to-end latency (publish->display)',
    ['source']
)
pipeline_stage_timestamp = _gauge(
    'pipeline_stage_timestamp', 'Pipeline stage unix timestamp',
    ['stage', 'source']
)

_server_started = False
_collector_started = False


def start_metrics_server():
    """Start the Prometheus HTTP server on port 8003.

    Idempotent — safe to call multiple times (Streamlit re-runs script).
    """
    global _server_started
    if _server_started:
        return
    _server_started = True
    try:
        start_http_server(8003)
        logger.info("Frontend metrics HTTP server started on port 8003")
    except OSError:
        logger.warning("Port 8003 already in use (metrics server already running)")


def init_background_collector():
    """Start background thread that periodically collects data quality metrics.

    Idempotent — safe to call multiple times.
    """
    global _collector_started
    if _collector_started:
        return
    _collector_started = True
    thread = threading.Thread(target=_collect_loop, daemon=True)
    thread.start()
    logger.info("Background metrics collector started")


def _collect_loop():
    """Periodically collect data quality metrics from MaxCompute."""
    time.sleep(30)  # Initial delay to let Streamlit initialize
    while True:
        try:
            _collect_once()
        except Exception as e:
            logger.error(f"Metrics collection error: {e}")
        time.sleep(300)  # Every 5 minutes


def _collect_once():
    """Single round of data quality metric collection from MaxCompute."""
    from odps import ODPS

    o = ODPS(
        os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID'),
        os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET'),
        project=os.getenv('MAXCOMPUTE_PROJECT'),
        endpoint=os.getenv('MAXCOMPUTE_ENDPOINT')
    )

    # --- AI field missing ratio ---
    try:
        sql = """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN (ai_summary IS NULL OR ai_summary = '') THEN 1 ELSE 0 END) AS summary_missing,
                SUM(CASE WHEN (tech_category IS NULL OR tech_category = '') THEN 1 ELSE 0 END) AS category_missing
            FROM hn_raw
            WHERE ds = (SELECT MAX(ds) FROM hn_raw WHERE ds IS NOT NULL)
        """
        with o.execute_sql(sql).open_reader() as reader:
            df = reader.to_pandas()
            if not df.empty:
                row = df.iloc[0]
                total = int(row['total']) if row['total'] else 1
                dq_ai_summary_missing_ratio.set(
                    float(row['summary_missing'] or 0) / total
                )
                dq_ai_category_missing_ratio.set(
                    float(row['category_missing'] or 0) / total
                )
    except Exception as e:
        logger.warning(f"Failed to collect missing ratio: {e}")

    # --- Category distribution ---
    try:
        sql = """
            SELECT tech_category, COUNT(*) AS cnt
            FROM hn_raw
            WHERE ds = (SELECT MAX(ds) FROM hn_raw WHERE ds IS NOT NULL)
            GROUP BY tech_category
        """
        with o.execute_sql(sql).open_reader() as reader:
            df = reader.to_pandas()
            total = int(df['cnt'].sum()) if not df.empty else 1
            for _, row in df.iterrows():
                dq_category_distribution.labels(category=row['tech_category']).set(int(row['cnt']))
            others_val = df.loc[df['tech_category'] == 'Others', 'cnt'].sum() if 'Others' in df['tech_category'].values else 0
            dq_others_category_ratio.set(float(others_val) / total)
    except Exception as e:
        logger.warning(f"Failed to collect category distribution: {e}")

    # --- Empty content ratio per source ---
    try:
        sql = """
            SELECT COALESCE(source, 'hackernews') AS src,
                   COUNT(*) AS total,
                   SUM(CASE WHEN (content_excerpt IS NULL OR content_excerpt = '') THEN 1 ELSE 0 END) AS empty_cnt
            FROM hn_raw
            WHERE ds = (SELECT MAX(ds) FROM hn_raw WHERE ds IS NOT NULL)
            GROUP BY source
        """
        with o.execute_sql(sql).open_reader() as reader:
            for _, row in reader.to_pandas().iterrows():
                t = int(row['total']) if row['total'] else 1
                dq_empty_content_ratio.labels(source=row['src']).set(
                    float(row['empty_cnt'] or 0) / t
                )
    except Exception as e:
        logger.warning(f"Failed to collect empty content ratio: {e}")

    # --- Duplicate URLs in latest day ---
    try:
        sql = """
            SELECT COUNT(*) AS dup_cnt
            FROM (
                SELECT url, COUNT(*) AS cnt
                FROM hn_raw
                WHERE ds = (SELECT MAX(ds) FROM hn_raw WHERE ds IS NOT NULL)
                GROUP BY url
                HAVING COUNT(*) > 1
            ) t
        """
        with o.execute_sql(sql).open_reader() as reader:
            df = reader.to_pandas()
            if not df.empty:
                dq_duplicate_urls_24h.set(int(df.iloc[0]['dup_cnt']))
    except Exception as e:
        logger.warning(f"Failed to collect duplicate URLs: {e}")

    # --- Pipeline E2E latency per source ---
    try:
        sql = """
            SELECT
                COALESCE(source, 'unknown') AS src,
                MAX(CAST(ingest_time AS BIGINT)) AS latest_ingest_ts
            FROM hn_raw
            WHERE ds = (SELECT MAX(ds) FROM hn_raw WHERE ds IS NOT NULL)
            GROUP BY source
        """
        _now = time.time()
        with o.execute_sql(sql).open_reader() as reader:
            for _, row in reader.to_pandas().iterrows():
                _ts = row['latest_ingest_ts']
                if _ts and _ts > 0:
                    _latency = _now - float(_ts)
                    pipeline_e2e_latency_seconds.labels(source=row['src']).set(
                        max(_latency, 0)
                    )
    except Exception as e:
        logger.warning(f"Failed to collect e2e latency: {e}")


# Auto-start on module import (handles Streamlit exec semantics)
start_metrics_server()
init_background_collector()


if __name__ == "__main__":
    # Standalone mode for testing
    start_metrics_server()
    _collect_loop()
