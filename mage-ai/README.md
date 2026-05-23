# TechPulse AI — 智能技术新闻数据平台

> **一个包含数据采集、流处理、AI 增强、数仓建模、向量检索与监控告警的完整数据工程项目**

[![Kafka](https://img.shields.io/badge/Message%20Queue-Kafka-231F20?logo=apache-kafka)](https://kafka.apache.org/)
[![dbt](https://img.shields.io/badge/Data%20Modeling-dbt-FF694B?logo=dbt)](https://www.getdbt.com/)
[![DashScope](https://img.shields.io/badge/AI%20Enhancement-DashScope-1677FF)](https://dashscope.aliyun.com/)
[![Qdrant](https://img.shields.io/badge/Vector%20DB-Qdrant-8B5CFU?logo=qdrant)](https://qdrant.tech/)
[![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B?logo=streamlit)](https://streamlit.io/)
[![Prometheus](https://img.shields.io/badge/Monitoring-Prometheus-E6522C?logo=prometheus)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Dashboard-Grafana-F46800?logo=grafana)](https://grafana.com/)

---

## 📋 目录

- [项目概述](#项目概述)
- [架构总览](#架构总览)
- [技术栈](#技术栈)
- [数据流详解](#数据流详解)
- [数仓分层](#数仓分层)
- [指标体系](#指标体系)
- [数据质量](#数据质量)
- [快速开始](#快速开始)
- [服务说明](#服务说明)
- [监控面板](#监控面板)
- [后续规划](#后续规划)

---

## 项目概述

TechPulse AI 定位为一个**AI 增强的数据工程平台**。它从 6 个英文技术源（Hacker News、Reddit、GitHub Trending、Dev.to、Lobsters、RSS）实时采集技术新闻，经过 **Kafka 消息队列**流式处理，由 **DashScope AI（Qwen3.6-Plus）** 进行自动分类、摘要生成和深度洞察，最终存入**阿里云 OSS 数据湖 + MaxCompute 数据仓库**，并通过 **dbt** 进行分层建模后，在 **Streamlit** 前端提供时间线浏览、收藏管理和**RAG 语义检索**等能力。

**核心数据流：** 采集 → 消息队列 → AI 增强 → 数据湖 → 数仓建模 → 服务供数 → 展示

**项目亮点：**
- **Kafka 流处理** — 多源采集器解耦于管道加工，支持缓冲、批量消费、死信重试
- **AI 增强管线** — DashScope Qwen3.6-Plus 对每篇文章做结构化分类、摘要、洞察；输出经过 5 维度质量校验
- **向量语义检索** — Qdrant HNSW 近似检索替代 O(n) 暴力扫描，首屏从 80s 降至 <5ms
- **dbt 数仓建模** — 分层建模（ODS → DWD → DWS → ADS），支持增量更新、测试、文档化
- **全链路监控** — Prometheus + Grafana 覆盖采集、AI、存储各环节，7 条告警规则

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  采集层 (Data Collection)                                                    │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐   │
│  │  HN    │  │ Reddit │  │ GitHub │  │ Dev.to │  │Lobsters│  │  RSS   │   │
│  │  API   │  │  API   │  │Trending│  │  API   │  │  API   │  │  Feed  │   │
│  └────┬───┘  └────┬───┘  └────┬───┘  └────┬───┘  └────┬───┘  └────┬───┘   │
│       └──────────┼────────────┼────────────┼────────────┼──────────┘        │
│                  └────────────┴────────────┴────────────┘                    │
│                               │ Producer (port 8001)                        │
└───────────────────────────────┼─────────────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │  Kafka (KRaft)        │
                    │  topic: raw_tech_feeds │
                    └───────────┬───────────┘
                                │
┌───────────────────────────────┼─────────────────────────────────────────────┐
│  加工层 (Data Processing)     │      ╔══ 监控 ═══════════════════════════╗   │
│                     ┌─────────▼─────────┐  ║                              ║   │
│                     │ Kafka Consumer     │  ║  Prometheus scrape           ║   │
│                     │ batch=10 + DLQ    │  ║  ports: 8001/8002/8003       ║   │
│                     └─────────┬─────────┘  ║                              ║   │
│                               │             ╚══════════════════════════════╝   │
│                     ┌─────────▼─────────┐                                     │
│                     │ transform_fetch()  │                                     │
│                     │ (数据清洗/格式化)    │                                     │
│                     └─────────┬─────────┘                                     │
│                               │                                                │
│                     ┌─────────▼─────────┐  ┌──────────────────┐               │
│                     │ transform_ai()     │  │ 5-Dimension DQ   │               │
│                     │ (DashScope Qwen3.6-Plus)│──┤ Validator        │               │
│                     │ 分类/摘要/洞察      │  │ (Prometheus Gauge)│               │
│                     └─────────┬─────────┘  └──────────────────┘               │
│                               │                                                │
│                     ┌─────────▼─────────┐                                     │
│                     │ OSS Sink          │                                     │
│                     │ (Parquet / hn_raw) │                                     │
│                     └─────────┬─────────┘                                     │
└───────────────────────────────┼─────────────────────────────────────────────┘
                                │
┌───────────────────────────────┼─────────────────────────────────────────────┐
│  数仓建模层 (Data Warehousing) │                                               │
│                     ┌─────────▼─────────┐                                     │
│                     │ Periodic Sync     │ 每 300s                             │
│                     │ OSS → MaxCompute  │                                     │
│                     └─────────┬─────────┘                                     │
│                               │                                                │
│                     ┌─────────▼─────────┐                                     │
│                     │ dbt-mc run        │  dbt 分层模型                        │
│                     │                    │                                     │
│                     │  ┌── ODS ──┐      │ 原始接入层                          │
│                     │  │ hn_raw  │      │                                     │
│                     │  └────┬────┘      │                                     │
│                     │       │           │                                     │
│                     │  ┌── DWD ──┐      │ 明细层（清洗+AI增强）                │
│                     │  │ dwd_    │      │                                     │
│                     │  └────┬────┘      │                                     │
│                     │       │           │                                     │
│                     │  ┌── DWS ──┐      │ 汇总层（主题聚合）                   │
│                     │  │ dws_    │      │                                     │
│                     │  └────┬────┘      │                                     │
│                     │       │           │                                     │
│                     │  ┌── ADS ──┐      │ 应用层（前端供数）                   │
│                     │  │ ads_    │      │                                     │
│                     │  └────────┘       │                                     │
│                     └──────────────────┘                                      │
└───────────────────────────────┼─────────────────────────────────────────────┘
                                │
┌───────────────────────────────┼─────────────────────────────────────────────┐
│  服务层 (Data Serving)         │                                               │
│                               │                                                │
│  ┌────────────────────────────▼────────────────────────────┐                  │
│  │                 Streamlit (port 8501)                    │                  │
│  │  ┌────────────┐  ┌────────────┐  ┌───────────────────┐  │                  │
│  │  │  Timeline   │  │  Favorites  │  │  AI Assistant     │  │                  │
│  │  │ (文章浏览)   │  │ (收藏管理)  │  │ (RAG 语义检索)    │  │                  │
│  │  └────────────┘  └────────────┘  └────────┬──────────┘  │                  │
│  └────────────────────────────────────────────┼─────────────┘                  │
│                                                │                               │
│  ┌────────────────────────────────────────────▼─────────────┐                  │
│  │  Qdrant Vector DB (port 6333)                             │                  │
│  │  425+ 文章向量, HNSW 近似检索, 支持分类过滤                │                  │
│  └───────────────────────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 技术栈

### 数据管道

| 类别 | 技术 | 用途 | 版本 |
|------|------|------|------|
| 消息队列 | Apache Kafka (KRaft) | 采集与加工解耦，单节点多源缓冲 | 3.5 |
| 流处理 | Python KafkaConsumer | 批量消费（batch=10），异常处理 + 死信 | — |
| 数据湖 | Alibaba Cloud OSS | 原始/加工数据存储（Parquet）| — |
| 数据仓库 | Alibaba MaxCompute | MPP 数仓，SQL 分析，分区表 | — |
| 数据建模 | dbt (dbt-mc) | 分层建模、版本控制、数据测试 | — |
| 调度编排 | Bash + Mage Scheduler | 定时同步 & dbt run（后续计划 Airflow）| — |

### AI 管线

| 类别 | 技术 | 用途 |
|------|------|------|
| LLM 推理 | DashScope / Qwen3.6-Plus | 文章分类、摘要生成、深度洞察 |
| 文本嵌入 | DashScope / text-embedding-v2 | 1536 维向量，RAG 检索底座 |
| 向量数据库 | Qdrant | HNSW 近似检索，payload 过滤 |
| 质量校验 | 自研 5 维度校验 | 缺失率/分类/幻觉/JSON/长度 → Prometheus |

### 前端 & 服务

| 类别 | 技术 | 用途 |
|------|------|------|
| 前端框架 | Streamlit | 3 页面数据展示 + 交互 |
| 本地缓存 | SQLite | 用户收藏持久化 |

### 监控 & 基础设施

| 类别 | 技术 | 用途 |
|------|------|------|
| 指标采集 | Prometheus | 15s 采集，4 scrape job |
| 可视化 | Grafana | 9 面板 + 7 告警规则 |
| 基础设施 | Docker Compose | 7 容器本地部署 |
| 云资源管理 | OpenTofu / Terraform | 阿里云 OSS / 权限声明式管理 |

---

## 数据流详解

### 实时流

```
采集器 ──(push)──→ Kafka ──(poll batch=10)──→ Consumer ──(clean)──→ AI ──(check)──→ OSS
  6 scraper        raw_tech_feeds                   transform_fetch   Qwen3.6-Plus   5-DQ validate
```

1. **producer/main.py** — 6 个 scraper 轮流执行，每篇文章实时推入 Kafka topic `raw_tech_feeds`
2. **kafka_consumer.py** — 每次 poll 批量消费，batch=10，并行执行清洗、AI 增强、质检
3. **transform_fetch** — 数据格式统一、字段清洗、URL 去重
4. **transform_ai** — DashScope Qwen3.6-Plus 调用，输出 `ai_summary`、`tech_category`、`ai_insight`
5. **validate_batch** — 5 维度校验：缺失率、分类合法性、Others 占比、JSON 解析失败率、幻觉检测
6. **DeadLetterQueue** — 同批失败 3 次后写入 `logs/dead_letter.jsonl`，防止无限重试
7. **sink.batch_write** — 写入 OSS Parquet 文件，按天分区 `ds=YYYYMMDD`

### 批处理（每 300s）

```
OSS ──(read_parquet)──→ MaxCompute ──(dbt run)──→ ODPS Query
                                                      │
                                             Streamlit ←──┘
```

1. **periodic_sync.py** — 每 300s 扫描 OSS 新文件，写入 MaxCompute
2. **dbt-mc run** — 依次执行 staging → intermediate → marts 模型
3. **Streamlit** — 通过 ODPS SQL 直接从 dbt marts 查询供数

### 离线向量同步

```
MaxCompute ──(load_news_data)──→ DashScope Embedding ──(upsert)──→ Qdrant
```

1. **vector_sync.py** — 全量同步，读取 MaxCompute，生成 1536 维向量
2. Qdrant HNSW 索引 → 语义检索 <5ms

---

## 数仓分层

```
dbt_project.yml 目录结构        ←→  标准数仓分层   ←→  实际模型
────────────────────────────────────────────────────
models/sources.yml                     ODS           ←  hn_raw 源定义
models/staging/stg_tech_news.sql      DWD(明细)      ←  清洗去重，view
models/intermediate/int_article_enriched.sql  DWD(宽表)  ←  8 种窗口函数增强，table
models/marts/fact_article.sql         DWS(事实)      ←  文章粒度，table
models/marts/dim_source.sql           DWD(维度)      ←  来源静态维度，table
models/marts/mart_daily_summary.sql   DWS(汇总)      ←  按天/分类聚合，table
models/marts/mart_trend_analysis.sql  DWS(趋势)      ←  7日/30日滑动，table
```

当前项目使用 dbt 的标准子目录（staging → intermediate → marts）组织模型，同时可以按数仓分层对应映射：

| dbt 目录 | 分层 | 说明 | 物化方式 | 增量策略 |
|----------|------|------|---------|---------|
| `sources.yml` | **ODS** | 原始数据接入层 | 外部表引用 | ds 分区裁剪（近 30 天）|
| `staging/` | **DWD** | 明细层：字段清洗、类型转换、去重 | view | — |
| `intermediate/` | **DWD** | 宽表层：窗口函数增强、特征衍生 | table | 全量刷新（窗口函数需全量计算）|
| `marts/` | **DWS/ADS** | 汇总层：多维聚合 + 应用层供数 | `incremental` | `insert_overwrite` + `ds` 分区 |

**数据建模方法（Kimball 风格）：**

| 概念 | 对应模型 | 说明 |
|------|---------|------|
| 事实表（累积快照） | `fact_article` | 一行代表一篇文章的完整生命周期，粒度为 `article_id` |
| 事实表（周期快照） | `mart_daily_summary` | 一行代表一天的文章汇总体 |
| 维度表（标准） | `dim_date` | 日期维度，预计算 year/month/quarter/week/day_of_week |
| 维度表（SCD Type 2） | `dim_source` | 来源维度，[SCD Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2) |
| 退化维度 | `score_tier`, `global_rank` | 低基数维度直接存储在事实表中，避免维度表爆炸 |

数据流向：**ODS（原始）→ DWD（明细/宽表）→ DWS（事实/维度）→ ADS（聚合）→ 前端**

**特色：** `int_article_enriched` 模型内含 **8 种窗口函数面试考点**（`ROW_NUMBER`、`RANK`、`LAG/LEAD`、`FIRST_VALUE`、滑动计数、滑动平均），作为 SQL 面试复习题。

数据流向：**ODS（原始）→ DWD（明细/宽表）→ DWS（汇总）→ 前端应用**

---

## 指标体系

> 完整指标定义、口径、数据来源、监控映射请参阅 [docs/metrics.md](docs/metrics.md)。

### 文章指标

| 指标名 | 定义 | 来源 |
|--------|------|------|
| `total_articles` | 入库文章总数 | dbt marts |
| `daily_collected` | 当日采集量 | Kafka consumer log |
| `daily_deduped` | 去重后有效文章量 | dbt DWD 层 |
| `articles_by_source` | 各来源发文分布 | dbt DWS |
| `articles_by_category` | 各分类文章分布 | dbt DWS |

### AI 指标

| 指标名 | 定义 | 监控 |
|--------|------|------|
| `ai_success_rate` | AI 调用成功率 | Prometheus Counter |
| `ai_token_usage` | Token 消耗量 | Prometheus (model/operation label) |
| `ai_token_cost` | 预估费用（USD） | Prometheus Counter |
| `ai_rate_limit_hits` | 429 触发次数 | Prometheus Counter |
| `ai_processing_duration` | AI 处理耗时 | Prometheus Histogram |

### 质量指标

| 指标名 | 阈值 | 监控 |
|--------|------|------|
| `dq_ai_summary_missing_ratio` | <0.2 | Prometheus Gauge + Grafana 告警 |
| `dq_ai_category_missing_ratio` | <0.2 | Prometheus Gauge |
| `dq_others_category_ratio` | <0.4 | Prometheus Gauge + Grafana 告警 |
| `dq_json_parse_fail_ratio` | <0.2 | Prometheus Gauge + Grafana 告警 |
| `dq_ai_hallucination_ratio` | <0.1 | Prometheus Gauge + Grafana 告警 |

### 管道指标

| 指标名 | 定义 | 监控 |
|--------|------|------|
| `oss_write_duration` | OSS 写入耗时 | Prometheus Histogram |
| `kafka_consume_lag` | Kafka 消费积压 | Prometheus Gauge |
| `crawler_in_cooldown` | 爬虫冷却标记 | Prometheus Gauge |
| `mc_sync_success` | MaxCompute 同步状态 | Prometheus Gauge |

---

## 数据质量

### 质量主动校验

| 校验项 | 方式 | 覆盖率 | 位置 |
|--------|------|--------|------|
| 主键唯一性 | dbt `unique` test | ✅ 6 个模型 | `schema.yml` |
| 非空约束 | dbt `not_null` test | ✅ 15+ 字段（8 个 warn） | `schema.yml` |
| 枚举值 | dbt `accepted_values` test | ✅ 5 组 | `schema.yml` |
| 外键引用 | dbt `relationships` test | ✅ fact_article → dim_source | `schema.yml` |
| 业务断言 | singular test | ✅ 5 条（2 个 error 为真实问题）| `tests/*.sql` |
| AI 输出 | 自研 5 维度 | ✅ 实时 batch → Prometheus | `data_quality/validator.py` |
| pipeline 集成 | `dbt run → dbt test` | ✅ 每次同步后自动执行 | `oss_to_mc_runner.py` |
| **总计** | **42 个 test** | **32 PASS / 8 WARN / 2 ERROR** | — |

### 错误恢复机制

- **死信队列 (DLQ)** — 批量处理连续失败 3 次后写入 `logs/dead_letter.jsonl`，跳过但不阻塞管道
- **指数退避** — 重连间隔 `30s → 60s → 120s → 300s`，防止失败时高频重试
- **at-least-once 语义** — commit() 在全流程成功后才执行，失败不提交 offset

---

## 快速开始

### 环境要求

- Docker Engine 24+ & Docker Compose
- 4GB+ 可用 RAM
- 阿里云 DashScope API Key（试用可用）

### 启动

```bash
# 1. 克隆仓库
git clone <repo-url> techpulse-ai
cd techpulse-ai/mage-ai

# 2. 创建 .env（可选，已内置测试 key）
# 在生产环境建议通过环境变量注入，避免硬编码

# 3. 启动全部服务
docker compose up -d

# 4. 查看运行状态
docker compose ps
```

### 访问

| 服务 | 地址 | 说明 |
|------|------|------|
| Streamlit 前端 | http://localhost:8501 | 主界面 |
| Mage AI UI | http://localhost:6789 | 管道管理 |
| Prometheus | http://localhost:9090 | 指标查询 |
| Grafana | http://localhost:3000 (admin/admin) | 监控面板 |
| Qdrant | http://localhost:6333 | 向量数据库 |

---

## 服务说明

| 容器名 | 角色 | 关键端口 | 核心文件 |
|--------|------|---------|---------|
| `kafka-kraft` | 消息队列 | 9092 | — |
| `tech-crawler` | 采集器 | 8001 (metrics) | `producer/main.py`, `scrapers/*.py` |
| `techpulse-orchestrator` | 流处理 + AI + 建模 | 8002/8004 (metrics), 6789 (Mage) | `kafka_consumer.py`, `transformers/*.py` |
| `tech-frontend` | 前端展示 | 8501, 8003 (metrics) | `panels/*.py`, `vector_store.py` |
| `qdrant` | 向量数据库 | 6333/6334 | — |
| `prometheus` | 指标存储 | 9090 | `prometheus.yml` |
| `grafana` | 仪表盘 | 3000 | `dashboards/*.json`, `alerting/*.yml` |

---

## 监控面板

Grafana **TechPulse AI Monitoring** 仪表盘包含 9 个面板：

| 面板 | 类型 | 数据源 |
|------|------|--------|
| AI Token 消耗趋势 | 折线图 | rate(ai_token_usage_total[5m]) |
| 当日 Token 总量 | 单值 | sum(increase(ai_token_usage_total[24h])) |
| MaxCompute 扫描量 | 面积图 | rate(mc_query_scanned_bytes[5m]) |
| 429 触发计数 | 柱状图 | rate(ai_rate_limit_hits_total[5m]) |
| AI 字段缺失率 | 折线图 | dq_ai_summary/category_missing_ratio |
| Others 占比 | 折线图（带阈值） | dq_others_category_ratio |
| 端到端延迟 | 折线图 | pipeline_e2e_latency_seconds |
| Kafka 消费积压 | 折线图 | kafka_consume_lag |
| OSS 写入耗时 | 折线图 | oss_write_duration_seconds |

内置告警规则：爬虫冷却、Others 分类占比过高、AI 摘要缺失率高、幻觉率高、JSON 解析失败率高、端到端延迟 >1h、Kafka 积压 >1000

---

## 项目文件树

```
mage-ai/
├── docker-compose.yml                          # 7 容器编排
├── producer/                                   # 采集服务
│   ├── main.py                                 # 调度器 + 推入 Kafka
│   └── scrapers/                               # 6 个数据源采集器
│       ├── hackernews.py
│       ├── reddit.py
│       ├── github_trending.py
│       ├── devto.py
│       ├── lobsters.py
│       └── rss_tech.py
├── techpulse_intelligence/                     # 加工 + 存储 + 质量
│   ├── kafka_consumer.py                       # 批量消费管道
│   ├── metrics.py                              # Prometheus 指标
│   ├── transformers/                           # 数据转换
│   │   ├── billowing_hill.py                   # AI 分类增强
│   │   └── feature_engineer.py                 # 特征工程（规划中）
│   ├── data_quality/                           # 数据质量
│   │   ├── validator.py                        # 5 维度校验
│   │   └── dead_letter.py                      # 死信队列
│   ├── data_exporters/                         # OSS 输出
│   └── periodic_sync.py                        # 定时 OSS → MC 同步
├── techpulse_dbt/                              # dbt 数据建模
│   ├── models/
│   │   ├── sources.yml                         # ODS 源定义
│   │   ├── staging/                            # DWD 明细层
│   │   ├── intermediate/                       # DWD 中间层
│   │   └── marts/                              # DWS + ADS 汇总与应用层
│   └── dbt_project.yml
├── frontend/                                   # 前端展示
│   ├── panels/
│   │   ├── timeline.py                         # 时间线浏览
│   │   ├── favorites.py                        # 收藏管理
│   │   └── assistant.py                        # AI 助手 RAG
│   ├── vector_store.py                         # Qdrant 向量检索
│   └── vector_sync.py                          # 向量同步脚本
├── grafana/                                    # 监控
│   ├── dashboards/monitoring-dashboard.json
│   └── alerting/alert-rules.yml
├── prometheus/
│   └── prometheus.yml
└── docs/
    ├── superpowers/specs/                      # 设计文档
    └── superpowers/plans/                      # 实施计划
```

---

## 后续规划

### 近期（正在进行）

- [x] Kafka commit 时序修复 + 死信队列（P0）
- [x] AI 输出 5 维度质量校验 + Prometheus 告警（P1）
- [x] Qdrant 向量检索替代 O(n) Python cosine（P2）
- [ ] dbt 数仓分层重构（ODS/DWD/DWS/ADS）
- [ ] 指标体系文档 + SQL 指标看板

### 远期

- [ ] Airflow/Dagster 调度替换 bash 脚本
- [ ] dbt docs 自动生成数据文档
- [ ] OpenLineage 数据血缘追踪
- [ ] CI/CD 流水线（dbt test + Python lint）
- [ ] 增量向量同步（当前仅全量）

---

## License

MIT
