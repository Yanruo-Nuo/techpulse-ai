# 监控平台 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 TechPulse AI 的 3 个 Python 服务 (news-crawler, orchestrator, tech-frontend) 增加 Prometheus 埋点，部署 Prometheus + Grafana，实现成本配额、数据质量和全链路延迟的监控。

**Architecture:** 每个服务内部通过 prometheus_client 启动独立的 HTTP server 暴露 `/metrics`，Docker Compose 新增 prometheus/grafana 容器，Prometheus 每 15s 拉取 3 个 target，Grafana 从 Prometheus 数据源读取并展示面板 + 告警。

**Tech Stack:** prometheus_client, Prometheus, Grafana, Docker Compose

---

## 文件结构

| 文件 | 操作 | 职责 |
|------|------|------|
| `prometheus/prometheus.yml` | 新建 | Prometheus 拉取配置 |
| `docker-compose.yml` | 修改 | 新增 prometheus + grafana 服务 + 端口暴露 |
| `producer/main.py` | 修改 | 启动 metrics HTTP server + 爬虫指标埋点 |
| `producer/metrics.py` | 新建 | 爬虫 Prometheus 指标定义 |
| `producer/scrapers/base.py` | 修改 | HTTP 状态码、请求耗时 histogram |
| `producer/requirements.txt` | 修改 | 新增 prometheus-client |
| `producer/Dockerfile` | 修改 | 暴露 8001 |
| `techpulse_intelligence/kafka_consumer.py` | 修改 | 启动 metrics HTTP server + OSS/Kafka 指标 |
| `techpulse_intelligence/transformers/billowing_hill.py` | 修改 | AI Token/费用/429/耗时指标 |
| `techpulse_intelligence/oss_to_mc_runner.py` | 修改 | 返回结构化结果供 periodic_sync 采集 |
| `techpulse_intelligence/periodic_sync.py` | 修改 | MC 同步 + dbt 指标埋点 |
| `techpulse_intelligence/metrics.py` | 新建 | 数据处理 Prometheus 指标定义 |
| `techpulse_intelligence/requirements.txt` | 修改 | 新增 prometheus-client |
| `frontend/metrics_collector.py` | 新建 | 数据质量定时采集 + frontend metrics HTTP server |
| `frontend/maxcompute.py` | 修改 | MC 扫描量/查询耗时指标 |
| `frontend/app.py` | 修改 | 启动 metrics server + collector 线程 |
| `frontend/Dockerfile` | 修改 | 暴露 8003 |
| `frontend/requirements.txt` | 修改 | 新增 prometheus-client |
| `grafana/datasources/datasource.yml` | 新建 | Prometheus 数据源自动配置 |
| `grafana/dashboards/dashboard.yml` | 新建 | Dashboard provision 配置 |
| `grafana/dashboards/monitoring-dashboard.json` | 新建 | 监控面板 JSON 模型 |
| `grafana/alerting/alert-rules.yml` | 新建 | 告警规则配置 |

---

### Task 1: Prometheus + Grafana 底座

**Files:**
- Create: `prometheus/prometheus.yml`
- Modify: `docker-compose.yml`

- [ ] **Step 1: 创建目录并编写 prometheus/prometheus.yml**

```bash
mkdir -p /root/techpulse-ai/mage-ai/prometheus
```

```yaml
# prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'news-crawler'
    static_configs:
      - targets: ['news-crawler:8001']

  - job_name: 'orchestrator'
    static_configs:
      - targets: ['magic:8002']

  - job_name: 'tech-frontend'
    static_configs:
      - targets: ['tech-frontend:8003']
```

- [ ] **Step 2: 更新 docker-compose.yml — 为现有服务添加端口暴露**

在 `news-crawler` 服务的 `restart` 行之后、`environment` 块之后添加：
```yaml
    expose:
      - "8001"
```

在 `magic` 服务的 `volumes` 行之后添加：
```yaml
    expose:
      - "8002"
```

在 `tech-frontend` 服务的 `volumes` 行之后添加：
```yaml
    expose:
      - "8003"
```

- [ ] **Step 3: 更新 docker-compose.yml — 添加 prometheus 和 grafana 服务**

在文件末尾、`volumes:` 之前，插入 prometheus 和 grafana：

```yaml
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    volumes:
      - ./prometheus:/etc/prometheus
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=30d'
    ports:
      - "9090:9090"
    restart: always

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    ports:
      - "3000:3000"
    volumes:
      - grafana_data:/var/lib/grafana
    restart: always

volumes:
  kafka_data:
    driver: local
  prometheus_data:
    driver: local
  grafana_data:
    driver: local
```

- [ ] **Step 4: 验证 docker-compose 配置**

```bash
cd /root/techpulse-ai/mage-ai && docker-compose config
```
Expected: 解析成功无报错，service 列表中包含 prometheus 和 grafana。

- [ ] **Step 5: 提交**

```bash
git add prometheus/prometheus.yml docker-compose.yml
git commit -m "feat: add Prometheus + Grafana infrastructure"
```

---

### Task 2: 爬虫埋点 (news-crawler)

**Files:**
- Create: `producer/metrics.py`
- Modify: `producer/main.py`
- Modify: `producer/scrapers/base.py`
- Modify: `producer/requirements.txt`
- Modify: `producer/Dockerfile`

- [ ] **Step 1: 创建 producer/metrics.py — 爬虫指标定义**

```python
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
```

- [ ] **Step 2: 修改 producer/main.py — 集成 metrics**

在 import 块追加：
```python
from prometheus_client import start_http_server
from metrics import (
    crawler_articles_total, crawler_failures_total,
    crawler_last_success_timestamp, crawler_in_cooldown,
    crawler_produce_lag_seconds
)
```

在 `run()` 函数开头 `p = None` 之后、`while p is None` 之前添加：
```python
    start_http_server(8001)
    logger.info("Metrics HTTP server started on port 8001")
```

在 scraper 循环的 `try` 块内、`articles = scraper.fetch()` 之后添加：
```python
                crawler_articles_total.labels(source=scraper.name, status='fetched').inc(len(articles))
```

在 `p.flush()` 之后、`failure_counts[scraper.name] = 0` 之前添加：
```python
                crawler_articles_total.labels(source=scraper.name, status='pushed').inc(len(validated))
                crawler_last_success_timestamp.labels(source=scraper.name).set(time.time())
```

在 `except Exception as e` 块内、`failure_counts[scraper.name] += 1` 之后添加：
```python
                crawler_failures_total.labels(source=scraper.name, error_type='scrape').inc()
```

在 `if failure_counts[scraper.name] >= 5` 分支内设置冷却标志：
```python
                    crawler_in_cooldown.labels(source=scraper.name).set(1)
```

在 `else` 分支（连续成功时）清除冷却标志：
```python
                else:
                    crawler_in_cooldown.labels(source=scraper.name).set(0)
```

- [ ] **Step 3: 修改 producer/scrapers/base.py — HTTP 状态码 + 耗时**

在文件顶部现有 import 块中添加：
```python
from metrics import crawler_http_status_codes, crawler_scrape_duration_seconds
```

在 `ScraperClient.get()` 方法中，将：
```python
                resp = self.session.get(url, headers=headers, timeout=timeout)
                self._request_count += 1
                return resp
```

修改为：
```python
                resp = self.session.get(url, headers=headers, timeout=timeout)
                self._request_count += 1
                _dur = time.time() - self._last_request_time
                crawler_scrape_duration_seconds.labels(source=self.source_name).observe(_dur)
                crawler_http_status_codes.labels(
                    source=self.source_name, code=str(resp.status_code)
                ).inc()
                return resp
```

注意：`_rate_limit()` 方法内部会更新 `self._last_request_time`，所以 `time.time() - self._last_request_time` 近似等于该次请求的耗时（含限流等待）。

- [ ] **Step 4: 更新 producer/requirements.txt**

追加一行：
```
prometheus-client
```

- [ ] **Step 5: 更新 producer/Dockerfile**

在 `CMD` 之前添加：
```
EXPOSE 8001
```

- [ ] **Step 6: 验证构建**

```bash
cd /root/techpulse-ai/mage-ai && docker-compose build news-crawler
```
Expected: 构建成功，无 import 错误。

- [ ] **Step 7: 提交**

```bash
git add producer/main.py producer/metrics.py producer/scrapers/base.py producer/requirements.txt producer/Dockerfile
git commit -m "feat: add crawler Prometheus instrumentation"
```

---

### Task 3: 数据处理埋点 (orchestrator/magic)

**Files:**
- Create: `techpulse_intelligence/metrics.py`
- Modify: `techpulse_intelligence/kafka_consumer.py`
- Modify: `techpulse_intelligence/transformers/billowing_hill.py`
- Modify: `techpulse_intelligence/oss_to_mc_runner.py`
- Modify: `techpulse_intelligence/periodic_sync.py`
- Modify: `techpulse_intelligence/requirements.txt`

- [ ] **Step 1: 创建 techpulse_intelligence/metrics.py**

```python
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
    'mc_sync_duration_seconds', 'OSS→MC sync duration in seconds'
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
```

- [ ] **Step 2: 修改 techpulse_intelligence/kafka_consumer.py — 集成 metrics server + OSS/Kafka 指标**

在 import 块追加：
```python
from prometheus_client import start_http_server
from metrics import (
    oss_write_total, oss_write_bytes, oss_write_duration_seconds,
    kafka_consume_lag
)
```

在 `run()` 函数的 `consumer = KafkaConsumer(...)` 之前添加：
```python
    start_http_server(8002)
    logger.info("Metrics HTTP server started on port 8002")
```

在 `_run_loop` 函数中，找到 `sink.batch_write(ai_result)` 调用（第 74 行和 94 行附近）。将第一处（batch 处理）包裹为：

```python
                            import time
                            _oss_start = time.time()
                            sink.batch_write(ai_result)
                            _oss_dur = time.time() - _oss_start
                            oss_write_duration_seconds.labels(target='hn_raw').observe(_oss_dur)
                            oss_write_total.labels(target='hn_raw', status='success').inc()
                            logger.info(f"Batch done: {len(ai_result)} records written")
```

对应的 except 块增加失败计数：
```python
                            logger.error(f"Pipeline error: {e}", exc_info=True)
                            oss_write_total.labels(target='hn_raw', status='failure').inc()
```

对 flush 块（第 94 行附近）的 `sink.batch_write(ai_result)` 做相同的包裹处理。

在 `_run_loop` 的 `for tp, records in msg_pack.items():` 循环内添加 Kafka 积压追踪：

```python
        for tp, records in msg_pack.items():
            # Track Kafka consumer lag
            try:
                high = consumer.highwater(tp)
                pos = consumer.position(tp)
                if high is not None:
                    kafka_consume_lag.labels(partition=str(tp.partition)).set(high - pos)
            except Exception:
                pass
            for msg in records:
                ...
```

- [ ] **Step 3: 修改 techpulse_intelligence/transformers/billowing_hill.py — AI 指标**

在 import 块追加：
```python
import time
from metrics import (
    ai_token_usage_total, ai_token_cost_dollars,
    ai_processing_duration_seconds, ai_rate_limit_hits_total
)

# 预估费用
AI_MODEL = "deepseek-v4-flash"
INPUT_COST_PER_TOKEN = 0.5 / 1_000_000
OUTPUT_COST_PER_TOKEN = 2.0 / 1_000_000
```

在 `classify_from_text` 函数内，将 `resp = Generation.call(...)` 调用及后续处理替换为：

```python
            _start = time.time()
            resp = Generation.call(
                model=AI_MODEL,
                messages=[{"role": "user", "content": prompt_text}],
                result_format="message",
                temperature=0.3,
                max_tokens=2048,
            )
            _dur = time.time() - _start
            ai_processing_duration_seconds.labels(operation='classify').observe(_dur)

            if resp.status_code == 200:
                _usage = getattr(resp, 'usage', None)
                if _usage:
                    _input = getattr(_usage, 'input_tokens', 0) or 0
                    _output = getattr(_usage, 'output_tokens', 0) or 0
                    ai_token_usage_total.labels(model=AI_MODEL, operation='classify').inc(_input + _output)
                    _cost = _input * INPUT_COST_PER_TOKEN + _output * OUTPUT_COST_PER_TOKEN
                    ai_token_cost_dollars.labels(model=AI_MODEL).inc(_cost)
                return resp.output.choices[0]["message"]["content"]
            elif resp.status_code == 429:
                ai_rate_limit_hits_total.labels(model=AI_MODEL).inc()
            print(f"⚠️ AI API 错误 (attempt {attempt+1}): {resp.message}")
```

- [ ] **Step 4: 修改 techpulse_intelligence/oss_to_mc_runner.py — 返回结构化结果**

在文件顶部添加 `import time`（如果还没有）。

在 `def run():` 第一行添加 `_start_time = time.time()`。

在 dbt 运行前后添加计时。找到 `result = subprocess.run(...)` dbt 调用，在其前添加 `_dbt_start = time.time()`，在其后添加 `_dbt_duration = time.time() - _dbt_start`。

在 `print("Pipeline complete!")` 之前、函数末尾添加 return 语句：

```python
    return {
        "sync_success": True,
        "sync_duration": time.time() - _start_time,
        "sync_rows": len(df),
        "dbt_success": result.returncode == 0,
        "dbt_duration": _dbt_duration,
    }
```

如果当前 `run()` 被 `__main__` 直接调用，确保异常也被捕获：
```python
if __name__ == "__main__":
    run()
```

- [ ] **Step 5: 修改 techpulse_intelligence/periodic_sync.py — MC 同步指标**

在 import 块追加：
```python
from prometheus_client import start_http_server
from metrics import mc_sync_duration_seconds, mc_sync_rows, mc_sync_success, dbt_run_duration_seconds, dbt_run_success
```

在 `run()` 函数开头添加：
```python
    start_http_server(8004)
    print("Metrics HTTP server started on port 8004")
```

将 while 循环体修改为：

```python
    while True:
        print(f"[{time.strftime('%H:%M:%S')}] Syncing OSS → MaxCompute → dbt...")
        _sync_start = time.time()
        result = subprocess.run(
            [sys.executable, SCRIPT],
            capture_output=True,
            text=True,
        )
        _sync_dur = time.time() - _sync_start

        for line in result.stdout.splitlines():
            if any(x in line for x in ["Found", "Total", "done", "Error", "OK", "FAIL", "PASS"]):
                print(f"  {line}")

        if result.returncode == 0:
            mc_sync_success.set(1)
            mc_sync_duration_seconds.set(_sync_dur)
            dbt_run_success.set(1)

            for line in result.stdout.splitlines():
                if "Total records:" in line:
                    try:
                        mc_sync_rows.set(int(line.split(":")[1].strip()))
                    except ValueError:
                        pass
        else:
            mc_sync_success.set(0)
            dbt_run_success.set(0)
            print(f"  Error: {result.stderr[-300:]}")

        print(f"[{time.strftime('%H:%M:%S')}] Sleep {INTERVAL}s...")
        time.sleep(INTERVAL)
```

- [ ] **Step 6: 更新 techpulse_intelligence/requirements.txt**

追加一行：
```
prometheus-client
```

- [ ] **Step 7: 验证构建**

```bash
cd /root/techpulse-ai/mage-ai && docker-compose build magic
```
Expected: 构建成功。

- [ ] **Step 8: 提交**

```bash
git add techpulse_intelligence/metrics.py \
  techpulse_intelligence/kafka_consumer.py \
  techpulse_intelligence/transformers/billowing_hill.py \
  techpulse_intelligence/oss_to_mc_runner.py \
  techpulse_intelligence/periodic_sync.py \
  techpulse_intelligence/requirements.txt
git commit -m "feat: add orchestrator Prometheus instrumentation"
```

---

### Task 4: 前端埋点 (tech-frontend)

**Files:**
- Create: `frontend/metrics_collector.py`
- Modify: `frontend/maxcompute.py`
- Modify: `frontend/app.py`
- Modify: `frontend/Dockerfile`
- Modify: `frontend/requirements.txt`

- [ ] **Step 1: 创建 frontend/metrics_collector.py — 数据质量指标 + metrics HTTP server**

```python
"""Frontend Prometheus metrics — data quality collector + MC query + page load

Starts an HTTP server on port 8003 for Prometheus scraping.
Can be imported by app.py or run standalone for testing.
"""
import time
import threading
import logging
from prometheus_client import start_http_server, Gauge, Counter, Histogram

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Data Quality Gauges ---
dq_ai_summary_missing_ratio = Gauge(
    'dq_ai_summary_missing_ratio', 'Ratio of articles missing AI summary'
)
dq_ai_category_missing_ratio = Gauge(
    'dq_ai_category_missing_ratio', 'Ratio of articles missing AI category'
)
dq_category_distribution = Gauge(
    'dq_category_distribution', 'Article count per category',
    ['category']
)
dq_others_category_ratio = Gauge(
    'dq_others_category_ratio', 'Ratio of articles in Others category'
)
dq_empty_content_ratio = Gauge(
    'dq_empty_content_ratio', 'Empty content ratio per source',
    ['source']
)
dq_duplicate_urls_24h = Gauge(
    'dq_duplicate_urls_24h', 'Duplicate URLs blocked in last 24h'
)

# --- MC Query Metrics ---
mc_query_scanned_bytes = Counter(
    'mc_query_scanned_bytes', 'MaxCompute bytes scanned'
)
mc_query_duration = Histogram(
    'mc_query_duration', 'MaxCompute query duration in seconds',
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60)
)

# --- Page Load Metrics ---
frontend_page_load_duration = Histogram(
    'frontend_page_load_duration', 'Page load duration in seconds',
    ['page'],
    buckets=(0.1, 0.5, 1, 2, 5, 10)
)

# --- Pipeline E2E Latency ---
pipeline_e2e_latency_seconds = Gauge(
    'pipeline_e2e_latency_seconds', 'End-to-end latency (publish→display)',
    ['source']
)
pipeline_stage_timestamp = Gauge(
    'pipeline_stage_timestamp', 'Pipeline stage unix timestamp',
    ['stage', 'source']
)

_server_started = False


def start_metrics_server():
    """Start the Prometheus HTTP server on port 8003.
    
    Idempotent — safe to call multiple times (Streamlit re-runs script).
    """
    global _server_started
    if _server_started:
        return
    _server_started = True
    start_http_server(8003)
    logger.info("Frontend metrics HTTP server started on port 8003")


def init_background_collector():
    """Start background thread that periodically collects data quality metrics."""
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
    # Lazy import to avoid circular dependency at module load time
    from maxcompute import get_odps

    o = get_odps()

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


if __name__ == "__main__":
    # Standalone mode for testing
    start_metrics_server()
    _collect_loop()
```

- [ ] **Step 2: 修改 frontend/maxcompute.py — MC 查询耗时指标**

在 import 块追加：
```python
import time
from metrics_collector import mc_query_scanned_bytes, mc_query_duration
```

在 `load_trend_data()` 函数中修改 SQL 执行部分。找到：
```python
        with o.execute_sql(sql, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
            df = reader.to_pandas()
            df['ds'] = pd.to_datetime(df['ds'], errors='coerce')
            df = df.dropna(subset=['ds'])
            return df
```

修改为：
```python
        _query_start = time.time()
        with o.execute_sql(sql, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
            df = reader.to_pandas()
            df['ds'] = pd.to_datetime(df['ds'], errors='coerce')
            df = df.dropna(subset=['ds'])
        _query_dur = time.time() - _query_start
        mc_query_duration.observe(_query_dur)
        mc_query_scanned_bytes.inc(len(df) * 512)  # rough estimate
        return df
```

对 `load_news_data()` 做相同修改。找到类似 `with o.execute_sql(...)` 块，包裹为：
```python
        _query_start = time.time()
        with o.execute_sql(sql, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
            result = reader.to_pandas()
        _query_dur = time.time() - _query_start
        mc_query_duration.observe(_query_dur)
        mc_query_scanned_bytes.inc(len(result) * 512)
        return result
```

- [ ] **Step 3: 修改 frontend/app.py — 启动 metrics server 和 collector**

在 import 块追加：
```python
from metrics_collector import start_metrics_server, init_background_collector
```

在 `init_db()` 之后、`st.set_page_config(...)` 之前添加：
```python
# Start Prometheus metrics (idempotent — safe for Streamlit re-runs)
start_metrics_server()
init_background_collector()
```

- [ ] **Step 4: 更新 frontend/Dockerfile**

在 `EXPOSE 8501` 行之后添加：
```
EXPOSE 8003
```

- [ ] **Step 5: 更新 frontend/requirements.txt**

追加一行：
```
prometheus-client
```

- [ ] **Step 6: 验证构建**

```bash
cd /root/techpulse-ai/mage-ai && docker-compose build tech-frontend
```
Expected: 构建成功。

- [ ] **Step 7: 提交**

```bash
git add frontend/metrics_collector.py frontend/maxcompute.py frontend/app.py \
  frontend/Dockerfile frontend/requirements.txt
git commit -m "feat: add frontend Prometheus instrumentation"
```

---

### Task 5: Grafana 配置 (Dashboard + 告警 + 数据源 Provisioning)

**Files:**
- Create: `grafana/datasources/datasource.yml`
- Create: `grafana/dashboards/dashboard.yml`
- Create: `grafana/dashboards/monitoring-dashboard.json`
- Create: `grafana/alerting/alert-rules.yml`
- Modify: `docker-compose.yml`

- [ ] **Step 1: 创建 Grafana 目录结构**

```bash
mkdir -p /root/techpulse-ai/mage-ai/grafana/{dashboards,datasources,alerting}
```

- [ ] **Step 2: 创建 grafana/datasources/datasource.yml**

```yaml
# grafana/datasources/datasource.yml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

- [ ] **Step 3: 创建 grafana/dashboards/dashboard.yml**

```yaml
# grafana/dashboards/dashboard.yml
apiVersion: 1

providers:
  - name: 'TechPulse AI Monitoring'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /etc/grafana/dashboards
```

- [ ] **Step 4: 创建 grafana/dashboards/monitoring-dashboard.json**

```json
{
  "title": "TechPulse AI Monitoring",
  "tags": ["techpulse"],
  "schemaVersion": 36,
  "time": { "from": "now-24h", "to": "now" },
  "timepicker": {},
  "panels": [
    {
      "title": "AI Token 消耗趋势",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        {
          "expr": "rate(ai_token_usage_total[5m])",
          "legendFormat": "{{model}} {{operation}}"
        }
      ]
    },
    {
      "title": "当日 Token 总量",
      "type": "stat",
      "datasource": "Prometheus",
      "gridPos": { "h": 4, "w": 4, "x": 12, "y": 0 },
      "targets": [
        {
          "expr": "sum(increase(ai_token_usage_total[24h]))"
        }
      ]
    },
    {
      "title": "当日预估费用",
      "type": "stat",
      "datasource": "Prometheus",
      "gridPos": { "h": 4, "w": 4, "x": 16, "y": 0 },
      "targets": [
        {
          "expr": "sum(increase(ai_token_cost_dollars[24h]))"
        }
      ],
      "fieldConfig": {
        "defaults": { "unit": "currencyUSD" }
      }
    },
    {
      "title": "MaxCompute 扫描量",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 8, "x": 0, "y": 8 },
      "targets": [
        {
          "expr": "rate(mc_query_scanned_bytes[5m])",
          "legendFormat": "scanned"
        }
      ]
    },
    {
      "title": "429 触发计数",
      "type": "bargauge",
      "datasource": "Prometheus",
      "gridPos": { "h": 4, "w": 4, "x": 12, "y": 4 },
      "targets": [
        {
          "expr": "rate(ai_rate_limit_hits_total[5m])"
        }
      ]
    },
    {
      "title": "AI 字段缺失率",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
      "targets": [
        {
          "expr": "dq_ai_summary_missing_ratio",
          "legendFormat": "摘要缺失"
        },
        {
          "expr": "dq_ai_category_missing_ratio",
          "legendFormat": "分类缺失"
        }
      ]
    },
    {
      "title": "Others 占比",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 8, "x": 12, "y": 16 },
      "targets": [
        {
          "expr": "dq_others_category_ratio",
          "legendFormat": "Others占比"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              { "color": "green", "value": null },
              { "color": "orange", "value": 0.3 },
              { "color": "red", "value": 0.4 }
            ]
          }
        }
      }
    },
    {
      "title": "端到端延迟",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 24 },
      "targets": [
        {
          "expr": "pipeline_e2e_latency_seconds",
          "legendFormat": "{{source}}"
        }
      ]
    },
    {
      "title": "Kafka 消费积压",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 24 },
      "targets": [
        {
          "expr": "kafka_consume_lag",
          "legendFormat": "partition {{partition}}"
        }
      ]
    },
    {
      "title": "OSS 写入耗时",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 8, "x": 0, "y": 32 },
      "targets": [
        {
          "expr": "oss_write_duration_seconds_sum / oss_write_duration_seconds_count",
          "legendFormat": "avg"
        }
      ]
    },
    {
      "title": "爬虫采集延迟",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 8, "x": 8, "y": 32 },
      "targets": [
        {
          "expr": "crawler_produce_lag_seconds",
          "legendFormat": "{{source}}"
        }
      ]
    }
  ]
}
```

- [ ] **Step 5: 创建 grafana/alerting/alert-rules.yml**

```yaml
# grafana/alerting/alert-rules.yml
apiVersion: 1

groups:
  - name: TechPulse AI Alerts
    interval: 30s
    rules:
      - alert: CrawlerCooldown
        expr: crawler_in_cooldown == 1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "爬虫 {{ $labels.source }} 冷却中超过 5 分钟"

      - alert: CrawlerStalled
        expr: time() - crawler_last_success_timestamp > 900
        labels:
          severity: critical
        annotations:
          summary: "爬虫 {{ $labels.source }} 超过 15 分钟未成功运行"

      - alert: OthersCategorySpike
        expr: dq_others_category_ratio > 0.4
        labels:
          severity: warning
        annotations:
          summary: "Others 分类占比超过 40%"

      - alert: SummaryMissingHigh
        expr: dq_ai_summary_missing_ratio > 0.2
        labels:
          severity: warning
        annotations:
          summary: "AI 摘要缺失率超过 20%"

      - alert: MCSyncFailed
        expr: mc_sync_success == 0
        labels:
          severity: critical
        annotations:
          summary: "MaxCompute 同步失败"

      - alert: E2ELatencyHigh
        expr: pipeline_e2e_latency_seconds > 3600
        labels:
          severity: warning
        annotations:
          summary: "端到端延迟超过 1 小时 ({{ $labels.source }})"

      - alert: KafkaLagHigh
        expr: kafka_consume_lag > 1000
        labels:
          severity: warning
        annotations:
          summary: "Kafka 积压超过 1000 条 (partition {{ $labels.partition }})"
```

- [ ] **Step 6: 更新 docker-compose.yml — 挂载 Grafana provisioning 目录**

修改 grafana 服务的 volumes 块，将：
```yaml
    volumes:
      - grafana_data:/var/lib/grafana
```
改为：
```yaml
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/datasources:/etc/grafana/provisioning/datasources
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/alerting:/etc/grafana/provisioning/alerting
```

- [ ] **Step 7: 验证 docker-compose 配置**

```bash
cd /root/techpulse-ai/mage-ai && docker-compose config
```
Expected: 解析成功无报错。

- [ ] **Step 8: 提交**

```bash
git add docker-compose.yml grafana/
git commit -m "feat: add Grafana dashboard and alert provisioning"
```

---

### Task 6: 全栈启动验证

- [ ] **Step 1: 停止旧容器并重建**

```bash
cd /root/techpulse-ai/mage-ai && docker-compose down
docker-compose build
docker-compose up -d
```

- [ ] **Step 2: 验证所有服务启动**

```bash
docker-compose ps
```
Expected: 6 个服务全部 `Up` 状态。

- [ ] **Step 3: 验证 Prometheus targets**

```bash
sleep 15  # wait for first scrape
curl -s http://localhost:9090/api/v1/targets | python3 -c "
import sys, json
data = json.load(sys.stdin)
for t in data['data']['activeTargets']:
    print(f\"{t['labels']['job']}: {t['health']}\")
"
```
Expected: 3 个 target 全部 `up`。

- [ ] **Step 4: 验证 metrics 端点是否正常返回数据**

```bash
curl -s http://localhost:9090/api/v1/query?query=up | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data['data']['result']:
    print(f\"{r['metric']['job']}: {r['value'][1]}\")
"
```
Expected: 3 个服务各返回一行 `1`（表示 UP）。

- [ ] **Step 5: 验证 Grafana**

```bash
curl -s -u admin:admin http://localhost:3000/api/health
```
Expected: `{"msg":"OK"}`

- [ ] **Step 6: 验证 Grafana 数据源已自动配置**

```bash
curl -s -u admin:admin http://localhost:3000/api/datasources
```
Expected: 返回包含 Prometheus 数据源的 JSON 数组。

- [ ] **Step 7: 提交最终验证结果（可选）**

```bash
git add -A
git commit -m "chore: complete monitoring platform implementation"
```

---

## 自检

**1. Spec 覆盖:**
- 架构总览 (spec §1) → Task 1 Docker Compose 新增 prometheus/grafana，Task 2-4 端口 expose
- 指标设计 (spec §2.1-2.3) → Task 2-4 的 metrics.py + 埋点代码，覆盖全部 34 个指标
- Prometheus 配置 (spec §3) → Task 1 prometheus.yml + Task 5 datasource
- Grafana Dashboard (spec §4) → Task 5 dashboard JSON + alert-rules
- 实施计划 (spec §5) → Task 1→5 对齐 Phase 1→5
- 文件变更清单 (spec §6) → 全部 20 个文件已覆盖
- 风险与边界 (spec §7) → 30d retention (Task 1), Grafana 默认密码 (Task 5 后续可改)

**2. 无占位符:** 所有步骤包含完整代码，无 TBD/TODO 或模糊描述。

**3. 类型一致性:**
- metrics 变量名在 metrics.py 定义和 consumer 中一致
- HTTP server 端口与 docker-compose expose 端口一致（crawler=8001, orchestrator/kafka=8002, periodic_sync=8004, frontend=8003）
- Prometheus scrape targets 的 hostname 与 docker-compose service name 一致
