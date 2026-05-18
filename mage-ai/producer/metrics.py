"""Crawler Prometheus metrics"""
from prometheus_client import Counter, Gauge, Histogram

crawler_articles_total = Counter(
    'crawler_articles_total', 'Articles fetched/pushed per round',
    ['source', 'status']
)
crawler_failures_total = Counter(
    'crawler_failures_total', 'Scrape failure count',
    ['source', 'error_type']
)
crawler_last_success_timestamp = Gauge(
    'crawler_last_success_timestamp', 'Last successful run unix timestamp',
    ['source']
)
crawler_in_cooldown = Gauge(
    'crawler_in_cooldown', 'Cooldown flag (1=in cooldown)',
    ['source']
)
crawler_http_status_codes = Counter(
    'crawler_http_status_codes', 'HTTP response code distribution',
    ['source', 'code']
)
crawler_scrape_duration_seconds = Histogram(
    'crawler_scrape_duration_seconds', 'Scrape duration per round in seconds',
    ['source'],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600)
)
crawler_produce_lag_seconds = Gauge(
    'crawler_produce_lag_seconds', 'Lag from article publish to produce',
    ['source']
)
