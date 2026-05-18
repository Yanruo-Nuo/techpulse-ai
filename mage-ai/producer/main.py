"""多源爬虫调度器：自动发现 scrapers → 循环调度 → 验证 → 推送 Kafka"""

import json
import time
import logging
from confluent_kafka import Producer
from scrapers import discover_scrapers, validate_article
from dataclasses import asdict
from prometheus_client import start_http_server
from metrics import (
    crawler_articles_total, crawler_failures_total,
    crawler_last_success_timestamp, crawler_in_cooldown,
    crawler_produce_lag_seconds
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC = "raw_tech_feeds"

KAFKA_CONF = {
    "bootstrap.servers": "kafka:9092",
    "client.id": "techpulse-crawler-hk",
}


def delivery_report(err, msg):
    if err:
        logger.error(f"推送失败: {err}")
    else:
        logger.info(f"成功推送 {msg.key().decode('utf-8')}")


def run():
    p = None
    start_http_server(8001)
    logger.info("Metrics HTTP server started on port 8001")
    while p is None:
        try:
            p = Producer(KAFKA_CONF)
            logger.info("Kafka Producer 初始化成功")
        except Exception as e:
            logger.warning(f"等待 Kafka 就绪... {e}")
            time.sleep(5)

    scrapers = discover_scrapers()
    logger.info(f"发现 {len(scrapers)} 个 scraper: {[s.name for s in scrapers]}")

    failure_counts = {s.name: 0 for s in scrapers}
    last_run = {s.name: 0 for s in scrapers}

    while True:
        now = time.time()
        for scraper in scrapers:
            if not scraper.enabled:
                continue
            if now - last_run[scraper.name] < scraper.interval:
                continue

            try:
                articles = scraper.fetch()
                crawler_articles_total.labels(source=scraper.name, status='fetched').inc(len(articles))
                validated = [a for a in (validate_article(a) for a in articles) if a]
                for article in validated:
                    p.produce(
                        TOPIC,
                        key=article.source_id,
                        value=json.dumps(asdict(article)),
                        callback=delivery_report,
                    )
                p.flush()
                crawler_articles_total.labels(source=scraper.name, status='pushed').inc(len(validated))
                crawler_last_success_timestamp.labels(source=scraper.name).set(time.time())
                logger.info(f"[{scraper.name}] 推送 {len(validated)}/{len(articles)} 篇")
                failure_counts[scraper.name] = 0
                crawler_in_cooldown.labels(source=scraper.name).set(0)
                for article in articles:
                    if article.published_at:
                        try:
                            pub_ts = float(article.published_at)
                            lag = now - pub_ts
                            if 0 < lag < 86400 * 30:
                                crawler_produce_lag_seconds.labels(source=scraper.name).set(lag)
                        except (ValueError, TypeError):
                            pass
            except Exception as e:
                logger.error(f"[{scraper.name}] 错误: {e}")
                failure_counts[scraper.name] += 1
                crawler_failures_total.labels(source=scraper.name, error_type='scrape').inc()
                if failure_counts[scraper.name] >= 5:
                    logger.warning(f"[{scraper.name}] 连续失败 5 次，暂停 1 小时")
                    crawler_in_cooldown.labels(source=scraper.name).set(1)
                    last_run[scraper.name] = now + 3600
                    failure_counts[scraper.name] = 0
                    continue
                else:
                    crawler_in_cooldown.labels(source=scraper.name).set(0)

            last_run[scraper.name] = now

        time.sleep(30)


if __name__ == "__main__":
    run()
