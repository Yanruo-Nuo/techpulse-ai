"""Standalone Kafka consumer pipeline: raw_tech_feeds → OSS → MaxCompute"""

import os, sys
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(__file__))
from transformers.quixotic_illusion import transform_fetch
from transformers.billowing_hill import transform_ai, transform_ai_v2
from data_exporters.insightful_resonance import UnifiedSink

from kafka import KafkaConsumer

from prometheus_client import start_http_server
from metrics import (
    oss_write_total, oss_write_bytes, oss_write_duration_seconds,
    kafka_consume_lag
)
from health_metrics import start_health_server, set_stage, set_pending, mark_success
from data_quality.dead_letter import DeadLetterQueue
from data_quality.validator import validate_batch, report_metrics

BATCH_SIZE = 10
POLL_TIMEOUT_MS = 30000


def run():
    # 主管线指标（随管线更新）
    start_http_server(8002)
    logger.info("Metrics HTTP server started on port 8002")

    # 健康指标（独立线程，永远有值）
    start_health_server(8005)
    logger.info("Health metrics HTTP server started on port 8005")

    consumer = KafkaConsumer(
        "raw_tech_feeds",
        bootstrap_servers="kafka:9092",
        group_id="techpulse-mage-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        max_poll_records=500,
        session_timeout_ms=180000,
        max_poll_interval_ms=360000,
        heartbeat_interval_ms=30000,
        consumer_timeout_ms=60000,
    )
    logger.info("Connected to kafka:9092, subscribed to raw_tech_feeds")

    sink = UnifiedSink()
    sink.init_client()
    logger.info("OSS sink initialized")

    retry_attempt = 0
    while True:
        try:
            _run_loop(consumer, sink)
            retry_attempt = 0
        except Exception as e:
            retry_attempt += 1
            backoff = min(30 * (2 ** retry_attempt), 300)
            logger.error(
                f"Consumer error: {e}, reconnecting in {backoff}s (attempt {retry_attempt})",
                exc_info=True,
            )
            time.sleep(backoff)


def _run_loop(consumer, sink):
    buffer = []
    dlq = DeadLetterQueue(max_retries=3)

    while True:
        msg_pack = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

        for tp, records in msg_pack.items():
            try:
                high = consumer.highwater(tp)
                pos = consumer.position(tp)
                if high is not None:
                    kafka_consume_lag.labels(partition=str(tp.partition)).set(high - pos)
            except Exception:
                pass

            for msg in records:
                # --- JSON 解析失败: 直接死信 ---
                try:
                    value = json.loads(msg.value.decode("utf-8"))
                    buffer.append(value)
                    logger.info(
                        f"Buffered [{len(buffer)}]: "
                        f"{value.get('source', '?')} / {value.get('title', '?')[:50]}"
                    )
                except json.JSONDecodeError as je:
                    logger.error(f"JSON parse error, dead-lettering: {je}")
                    dlq.write([{"title": "?"}], f"JSONDecodeError: {je}")
                    consumer.commit()
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error: {e}", exc_info=True)
                    continue

                # --- Pipeline batch 处理 ---
                if len(buffer) >= BATCH_SIZE:
                    batch = buffer
                    buffer = []
                    logger.info(f"Processing batch of {len(batch)} messages")

                    set_pending(len(batch))
                    try:
                        set_stage(1)  # fetching
                        fetch_result = transform_fetch(batch)
                        set_stage(2)  # ai
                        ai_result = transform_ai(fetch_result)

                        # v2: 多轮 AI 抽取（在原始 AI 处理后追加深度分析）
                        for article in ai_result:
                            article_v2 = transform_ai_v2(article)
                            if article_v2.get("ai_analysis_v2"):
                                article["ai_analysis_v2"] = article_v2["ai_analysis_v2"]
                                article["ai_chunks"] = article_v2.get("ai_chunks", [])

                        # AI 输出质量校验
                        dq_checks = validate_batch(ai_result)
                        report_metrics(dq_checks)

                        set_stage(3)  # writing
                        _oss_start = time.time()
                        sink.batch_write(ai_result)
                        _oss_dur = time.time() - _oss_start
                        oss_write_duration_seconds.labels(target='hn_raw').observe(_oss_dur)
                        oss_write_total.labels(target='hn_raw', status='success').inc()
                        mark_success()
                        set_stage(0)  # idle
                        logger.info(f"Batch done: {len(ai_result)} records written")

                        # ✅ 成功后才 commit
                        consumer.commit()
                        dlq.reset(batch)

                    except Exception as e:
                        logger.error(f"Pipeline error: {e}", exc_info=True)
                        oss_write_total.labels(target='hn_raw', status='failure').inc()

                        # 死信判断: 连续失败 3 次则跳过
                        if dlq.should_deadletter(batch):
                            dlq.write(batch, str(e))
                            consumer.commit()
                            logger.warning(
                                f"Dead-lettered {len(batch)} records after {dlq.max_retries} retries"
                            )

        # --- Flush 积压 ---
        if not msg_pack and buffer:
            logger.info(f"Flushing {len(buffer)} buffered messages")
            batch = buffer
            buffer = []

            set_pending(len(batch))
            try:
                set_stage(1)
                fetch_result = transform_fetch(batch)
                set_stage(2)
                ai_result = transform_ai(fetch_result)

                # v2: 多轮 AI 抽取（flush 批次）
                for article in ai_result:
                    article_v2 = transform_ai_v2(article)
                    if article_v2.get("ai_analysis_v2"):
                        article["ai_analysis_v2"] = article_v2["ai_analysis_v2"]
                        article["ai_chunks"] = article_v2.get("ai_chunks", [])

                dq_checks = validate_batch(ai_result)
                report_metrics(dq_checks)

                set_stage(3)
                _oss_start = time.time()
                sink.batch_write(ai_result)
                _oss_dur = time.time() - _oss_start
                oss_write_duration_seconds.labels(target='hn_raw').observe(_oss_dur)
                oss_write_total.labels(target='hn_raw', status='success').inc()
                mark_success()
                set_stage(0)
                logger.info(f"Flush done: {len(ai_result)} records written")

                consumer.commit()
                dlq.reset(batch)

            except Exception as e:
                logger.error(f"Flush error: {e}", exc_info=True)
                oss_write_total.labels(target='hn_raw', status='failure').inc()

                if dlq.should_deadletter(batch):
                    dlq.write(batch, str(e))
                    consumer.commit()


if __name__ == "__main__":
    run()
