"""Orchestrator Prometheus metrics"""
from prometheus_client import Counter, Gauge, Histogram

# AI / Dashscope
ai_token_usage_total = Counter(
    'ai_token_usage_total', 'Dashscope token consumption',
    ['model', 'operation']
)
ai_token_cost_dollars = Counter(
    'ai_token_cost_dollars', 'Estimated AI cost in USD',
    ['model']
)
ai_processing_duration_seconds = Histogram(
    'ai_processing_duration_seconds', 'AI processing duration per record',
    ['operation'],
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60)
)
ai_rate_limit_hits_total = Counter(
    'ai_rate_limit_hits_total', 'Rate limit (429) trigger count',
    ['model']
)
ai_rate_limit_hits_total.labels(model='glm-5.1').inc(0)  # init at 0 so time series exists

# Init at 0 so time series exists from prometheus start
# 预注册所有 5 个 AI 操作，确保面板有完整分类
for op in ('classify', 'round1_extract', 'round2_analyze', 'round3_integrate', 'entities'):
    ai_token_usage_total.labels(model='glm-5.1', operation=op).inc(0)
ai_token_cost_dollars.labels(model='glm-5.1').inc(0)

# OSS
oss_write_total = Counter(
    'oss_write_total', 'OSS write count',
    ['target', 'status']
)
oss_write_bytes = Counter(
    'oss_write_bytes', 'OSS bytes written',
    ['target']
)
oss_write_duration_seconds = Histogram(
    'oss_write_duration_seconds', 'OSS write duration',
    ['target'],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30)
)

# Kafka
kafka_consume_lag = Gauge(
    'kafka_consume_lag', 'Kafka consumer lag per partition',
    ['partition']
)

# MaxCompute sync
mc_sync_duration_seconds = Gauge(
    'mc_sync_duration_seconds', 'OSS->MC sync duration in seconds'
)
mc_sync_rows = Gauge(
    'mc_sync_rows', 'Rows synced from OSS to MC'
)
mc_sync_success = Gauge(
    'mc_sync_success', 'MC sync success flag (1=success, 0=failure)'
)
dbt_run_duration_seconds = Gauge(
    'dbt_run_duration_seconds', 'dbt run duration in seconds'
)
dbt_run_success = Gauge(
    'dbt_run_success', 'dbt run success flag (1=success, 0=failure)'
)
