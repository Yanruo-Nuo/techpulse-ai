"""向量同步管线的 Prometheus 监控指标"""

try:
    from prometheus_client import Counter, Gauge, Histogram

    sync_total = Counter(
        'vector_sync_total',
        'Total sync operations',
        ['collection', 'status']  # status: success / empty / error
    )

    triple_sync_total = Counter(
        'triple_sync_total',
        'Triples synced per run',
        ['status']  # status: synced / skipped
    )

    sync_duration_seconds = Histogram(
        'sync_duration_seconds',
        'Sync duration in seconds',
        ['collection'],
        buckets=(1, 5, 15, 30, 60, 120, 300, 600)
    )

    articles_with_zero_triples = Counter(
        'articles_with_zero_triples_total',
        'Articles where graph_extractor returned 0 triples',
        ['source']
    )

    triple_sync_articles_scanned = Counter(
        'triple_sync_articles_scanned_total',
        'Total articles scanned during triple sync',
        ['source']
    )

except ImportError:
    # Fallback: 当 prometheus_client 未安装时用 no-op 替代品
    import logging
    logger = logging.getLogger(__name__)
    logger.warning("prometheus_client not installed, metrics disabled")

    class _NoopCounter:
        def inc(self, amount=1):
            pass
        def labels(self, **kwargs):
            return self

    class _NoopHistogram:
        def observe(self, amount):
            pass

    sync_total = _NoopCounter()
    triple_sync_total = _NoopCounter()
    sync_duration_seconds = _NoopHistogram()
    articles_with_zero_triples = _NoopCounter()
    triple_sync_articles_scanned = _NoopCounter()
