"""Consumer 健康指标 — 独立于管线执行，永远有值

启动一个单独的 HTTP 端口（默认 8005），注册：
  - consumer_alive: 1（服务健康）
  - consumer_last_success_timestamp: 上次成功处理批次的时间
  - consumer_pending_articles: 缓冲中待处理的消息数
  - consumer_pipeline_stage: 0/1/2/3（当前管线所处阶段）

与 metrics.py 的区别：
  - metrics.py: 由管线步骤调用，管线不跑就不更新
  - health_metrics.py: 独立线程，定时刷新，永远有值
"""

import threading
import time
import logging

from prometheus_client import start_http_server, Gauge

logger = logging.getLogger(__name__)

# ── 健康指标（永远有值）──
consumer_alive = Gauge("consumer_alive", "1 if consumer process is running", ["pipeline"])
consumer_last_success_timestamp = Gauge(
    "consumer_last_success_timestamp",
    "Unix timestamp of last successful batch write",
)
consumer_pending_articles = Gauge(
    "consumer_pending_articles",
    "Number of articles currently buffered",
)
consumer_pipeline_stage = Gauge(
    "consumer_pipeline_stage",
    "Current pipeline stage: 0=idle, 1=fetching, 2=ai, 3=writing",
)

# ── 共享状态（主线程写入，健康线程读取）──
_last_success: float = 0
_pending: int = 0
_stage: int = 0
_lock = threading.Lock()


def set_stage(stage: int):
    global _stage
    with _lock:
        _stage = stage


def set_pending(n: int):
    global _pending
    with _lock:
        _pending = n


def mark_success():
    global _last_success
    with _lock:
        _last_success = time.time()


def _refresh_loop(interval: int = 15):
    """每 interval 秒刷新一次健康指标"""
    while True:
        with _lock:
            consumer_alive.labels(pipeline="techpulse").set(1)
            consumer_last_success_timestamp.set(_last_success)
            consumer_pending_articles.set(_pending)
            consumer_pipeline_stage.set(_stage)
        time.sleep(interval)


def start_health_server(port: int = 8005):
    """启动健康指标 HTTP 服务器（独立线程）"""
    start_http_server(port)
    logger.info(f"Health metrics HTTP server started on port {port}")

    # 初始值
    consumer_alive.labels(pipeline="techpulse").set(1)
    consumer_pending_articles.set(0)
    consumer_pipeline_stage.set(0)

    t = threading.Thread(target=_refresh_loop, daemon=True, args=(15,))
    t.start()
    logger.info(f"Health metrics refresh loop started (interval=15s)")
