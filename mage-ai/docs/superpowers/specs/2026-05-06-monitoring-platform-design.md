# TechPulse AI 监控平台设计

> **Goal**: 为 TechPulse AI 建立生产级监控体系，覆盖成本配额管控、数据质量追踪和全链路性能延迟监控。
> **Architecture**: Prometheus + Grafana 方案，3 个 Python 服务各自埋点暴露 `/metrics`，Prometheus 拉取存储，Grafana 可视化 + 告警。
> **Tech Stack**: Python prometheus_client, Prometheus, Grafana, Docker Compose

---

## 1. 架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Docker Compose (6 services)                                            │
│                                                                         │
│  ┌──────────┐   ┌────────────────┐   ┌──────────────────┐              │
│  │  Kafka   │   │ news-crawler   │   │  Orchestrator    │              │
│  │  :9092   │   │ :8001/metrics  │   │  :8002/metrics   │              │
│  └──────────┘   └───────┬────────┘   └────────┬─────────┘              │
│                         │                      │                        │
│  ┌──────────┐    ┌──────┴───────┐    ┌────────┴─────────┐              │
│  │ Grafana  │◄───│  Prometheus  │◄───│  tech-frontend   │              │
│  │ :3000    │    │  :9090       │    │  :8003/metrics   │              │
│  └──────────┘    └──────────────┘    └──────────────────┘              │
│                                                                         │
│   ┌─────────────────────────────────────────────────────┐               │
│   │  prometheus.yml: scrape_configs → 3 targets         │               │
│   └─────────────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────────┘
```

### 新增 Docker 服务

| 服务 | 镜像 | 端口 | 用途 |
|------|------|------|------|
| prometheus | prom/prometheus:latest | 9090 | 时序数据采集与存储 |
| grafana | grafana/grafana:latest | 3000 | 可视化与告警 |

### 埋点端点

| 服务 | 端口 | HTTP 路径 |
|------|------|-----------|
| news-crawler | 8001 | /metrics |
| Orchestrator | 8002 | /metrics |
| tech-frontend | 8003 | /metrics |

---

## 2. 指标设计

### 2.1 爬虫服务 (news-crawler)

| 指标名 | 类型 | Labels | 说明 |
|--------|------|--------|------|
| `crawler_articles_total` | Counter | source, status | 每轮抓取/推送文章数 |
| `crawler_failures_total` | Counter | source, error_type | 失败次数 |
| `crawler_last_success_timestamp` | Gauge | source | 末次成功 Unix 时间戳 |
| `crawler_in_cooldown` | Gauge | source | 冷却中标志 (0/1) |
| `crawler_http_status_codes` | Counter | source, code | HTTP 响应码分布 |
| `crawler_scrape_duration_seconds` | Histogram | source | 每轮耗时分布 |
| `crawler_produce_lag_seconds` | Gauge | source | 文章发布到爬取推送的延迟 |

### 2.2 数据处理服务 (Orchestrator)

| 指标名 | 类型 | Labels | 说明 |
|--------|------|--------|------|
| `ai_token_usage_total` | Counter | model, operation | Dashscope Token 消耗 |
| `ai_token_cost_dollars` | Counter | model | 预估费用 |
| `ai_processing_duration_seconds` | Histogram | operation | AI 处理耗时 |
| `ai_rate_limit_hits_total` | Counter | — | 429 触发次数 |
| `oss_write_total` | Counter | target, status | OSS 写入统计 |
| `oss_write_bytes` | Counter | target | 写入字节 |
| `oss_write_duration_seconds` | Histogram | target | OSS 写入耗时 |
| `kafka_consume_lag` | Gauge | partition | Kafka 消费积压 |
| `mc_sync_duration_seconds` | Gauge | — | OSS→MC 同步耗时 |
| `mc_sync_rows` | Gauge | — | 同步行数 |
| `mc_sync_success` | Gauge | — | 同步成功标志 |
| `dbt_run_duration_seconds` | Gauge | — | dbt 耗时 |
| `dbt_run_success` | Gauge | — | dbt 成功标志 |

### 2.3 前端服务 (tech-frontend)

| 指标名 | 类型 | Labels | 说明 |
|--------|------|--------|------|
| `dq_ai_summary_missing_ratio` | Gauge | — | AI 摘要缺失率 % |
| `dq_ai_category_missing_ratio` | Gauge | — | AI 分类缺失率 % |
| `dq_category_distribution` | Gauge | category | 各分类文章占比 |
| `dq_others_category_ratio` | Gauge | — | Others 分类占比 |
| `dq_empty_content_ratio` | Gauge | source | 空正文比例 |
| `dq_duplicate_urls_24h` | Gauge | — | 24h 重复拦截数 |
| `mc_query_scanned_bytes` | Counter | — | MaxCompute 扫描量 |
| `mc_query_duration` | Histogram | — | 查询耗时 |
| `frontend_page_load_duration` | Histogram | page | 页面加载耗时 |
| `pipeline_e2e_latency_seconds` | Gauge | source | 端到端延迟（发布→前端展示） |
| `pipeline_stage_timestamp` | Gauge | stage, source | 各阶段时间戳（publish/produce/consume/ai/oss/mc） |

---

## 3. Prometheus 配置

```yaml
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

数据保留: `--storage.tsdb.retention.time=30d`。

---

## 4. Grafana Dashboard

### 面板组 1: 成本与配额

| 面板 | 图表类型 | PromQL |
|------|----------|--------|
| AI Token 消耗趋势 | 折线图 (24h) | `rate(ai_token_usage_total[5m])` |
| 当日 Token 总量 | Singlestat | `sum(increase(ai_token_usage_total[24h]))` |
| 当日预估费用 | Singlestat | `sum(increase(ai_token_cost_dollars[24h]))` |
| MaxCompute 扫描量 | 面积图 | `rate(mc_query_scanned_bytes[5m])` |
| 429 触发计数 | 柱状图 | `rate(ai_rate_limit_hits_total[5m])` |

### 面板组 2: 数据质量

| 面板 | 图表类型 | PromQL |
|------|----------|--------|
| AI 字段缺失率 | 双线图 | `dq_ai_summary_missing_ratio` + `dq_ai_category_missing_ratio` |
| 分类分布 | 饼图 | `dq_category_distribution` |
| Others 占比 | 折线图+阈值线 | `dq_others_category_ratio` (阈值 40%) |
| 各来源空正文率 | 柱状图 | `dq_empty_content_ratio` |
| 24h 重复 URL | Singlestat | `dq_duplicate_urls_24h` |
| 服务健康 | 状态表 | 各 `*_success` + `crawler_in_cooldown` |

### 面板组 3: 性能与全链路延迟

| 面板 | 图表类型 | PromQL |
|------|----------|--------|
| 端到端延迟趋势 | 折线图 | `pipeline_e2e_latency_seconds{source="hackernews"}` (按 source 分色) |
| 各阶段耗时瀑布 | 柱状图 | 通过 `pipeline_stage_timestamp` 计算各阶段 delta |
| Kafka 消费积压 | 面积图 | `kafka_consume_lag` |
| OSS 写入耗时 | 折线图 | `oss_write_duration_seconds` |
| 爬虫采集延迟 | 折线图 | `crawler_produce_lag_seconds` |

### 告警规则

| 规则名 | 条件 | 严重度 |
|--------|------|--------|
| AI Token 超限预警 | `sum(increase(ai_token_usage_total[24h])) > 预估预算80%` | Warning |
| 扫描量过大 | 单次 `mc_query_scanned_bytes > 1GB` | Warning |
| 爬虫冷却 | `crawler_in_cooldown == 1` > 5min | Critical |
| 爬虫停滞 | `time() - crawler_last_success_timestamp > 900` (15min) | Critical |
| Others 突增 | `dq_others_category_ratio > 0.4` | Warning |
| 摘要缺失过高 | `dq_ai_summary_missing_ratio > 0.2` | Warning |
| MC 同步失败 | `mc_sync_success == 0` | Critical |
| 端到端延迟过高 | `pipeline_e2e_latency_seconds > 3600` (1h) | Warning |
| Kafka 积压过多 | `kafka_consume_lag > 1000` | Warning |

---

## 5. 实施计划

### Phase 1: Prometheus + Grafana 底座
- 创建 `prometheus/prometheus.yml`
- 更新 `docker-compose.yml`，添加 prometheus + grafana 服务

### Phase 2: 爬虫埋点
- `producer/main.py`: prometheus_client HTTP server + 爬虫指标
- `producer/scrapers/base.py`: HTTP 状态码、请求耗时
- `producer/requirements.txt` + `producer/Dockerfile`: 新增 prometheus_client, 暴露 8001

### Phase 3: 数据处理埋点
- `kafka_consumer.py`: HTTP server + OSS 指标
- `billowing_hill.py`: AI Token、费用、429
- `oss_to_mc_runner.py` / `periodic_sync.py`: 同步指标
- `techpulse_intelligence/requirements.txt`: 新增 prometheus_client

### Phase 4: 前端埋点
- `maxcompute.py`: 扫描量和查询耗时
- 新建 `metrics_collector.py`: 数据质量定时采集（缺失率、分类分布、重复率）
- `frontend/Dockerfile` + `frontend/requirements.txt`: 暴露 8003 + prometheus_client

### Phase 5: Grafana 配置
- 导入 Dashboard JSON
- 配置告警规则

---

## 6. 文件变更清单

| 文件 | 操作 |
|------|------|
| `docker-compose.yml` | 修改 |
| `prometheus/prometheus.yml` | 新建 |
| `producer/main.py` | 修改 |
| `producer/scrapers/base.py` | 修改 |
| `producer/requirements.txt` | 修改 |
| `producer/Dockerfile` | 修改 |
| `techpulse_intelligence/kafka_consumer.py` | 修改 |
| `techpulse_intelligence/transformers/billowing_hill.py` | 修改 |
| `techpulse_intelligence/oss_to_mc_runner.py` | 修改 |
| `techpulse_intelligence/periodic_sync.py` | 修改 |
| `techpulse_intelligence/requirements.txt` | 修改 |
| `frontend/maxcompute.py` | 修改 |
| `frontend/metrics_collector.py` | 新建 |
| `frontend/Dockerfile` | 修改 |
| `frontend/requirements.txt` | 修改 |

---

## 7. 风险与边界

- **存储**: 30d 保留期约需 5-10GB（视指标基数）
- **安全**: Grafana 默认无认证，生产需加反向代理或基础认证
- **告警通知**: 首次不含通知端点，后续可接 Webhook（企业微信/钉钉/Slack）
- **费用估算**: Token 费用按 Dashscope 定价实时 Counter 估算，非精确账单
