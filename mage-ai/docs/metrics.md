# TechPulse AI — 指标体系文档

> **统一所有指标口径。** 本文档定义项目中每个指标的含义、计算方式、数据来源和监控方式。指标口径以本文档为准。

---

## 1. 文章指标 (Article Metrics)

衡量数据采集和处理的量级、效率和覆盖情况。

### 1.1 文章采集量

| 属性 | 定义 |
|------|------|
| **指标名** | `crawler_articles_total` |
| **口径** | 爬虫每轮采集并推入 Kafka 的文章数，按 `source × status` 计数 |
| **类型** | Counter，单调递增 |
| **labels** | `source` (hn/reddit/github/devto/lobsters/rss), `status` (success/dedup/filtered) |
| **数据来源** | producer (port 8001) → Prometheus |
| **Grafana** | 无独立面板，可通过 Prometheus 查询 |
| **SQL 等价** | `SELECT COUNT(*) FROM hn_raw GROUP BY source` |

### 1.2 日采集量

| 属性 | 定义 |
|------|------|
| **指标名** | `article_cnt` (在 `mart_daily_summary` 中) |
| **口径** | 按天 + 来源 + 分类统计的去重文章数 |
| **类型** | dbt 模型字段 |
| **计算逻辑** | `dws` 层：`GROUP BY ds, source, tech_category → COUNT(*)` |
| **dbt 来源** | `models/marts/mart_daily_summary.sql` |
| **前端展示** | Streamlit Timeline 页 KPI（规划中）|

### 1.3 去重后有效文章量

| 属性 | 定义 |
|------|------|
| **指标名** | `articles_deduped` |
| **口径** | 经过 `DWD` 层 `ROW_NUMBER() OVER (PARTITION BY id)` 去重后的文章数 |
| **数据来源** | `stg_tech_news`（`staging` 层视图）|
| **SQL 等价** | `SELECT COUNT(DISTINCT id) FROM stg_tech_news` |

### 1.4 各来源发文分布

| 属性 | 定义 |
|------|------|
| **指标名** | —（dbt 模型字段）|
| **口径** | 按 `source` 维度聚合的文章数 |
| **dbt 来源** | `fact_article` → `mart_daily_summary`（`source_type` 维度）|
| **前端展示** | 无（可加）|

### 1.5 各分类文章分布

| 属性 | 定义 |
|------|------|
| **指标名** | —（dbt 模型字段）|
| **口径** | 按 `tech_category` 维度聚合的文章数 |
| **分类枚举** | `AI/ML, Security, CloudNative, Programming, Hardware, DataEngineering, Others` |
| **前端展示** | AI 助手对话中实时显示 `{分类} {N}篇` 分布 |

### 1.6 爬虫采集延迟

| 属性 | 定义 |
|------|------|
| **指标名** | `crawler_produce_lag_seconds` |
| **口径** | 文章发布时间到爬虫推送 Kafka 之间的秒数 |
| **类型** | Gauge，按 `source` 标记 |
| **数据来源** | producer (port 8001) → Prometheus |
| **Grafana 面板** | "爬虫采集延迟" |

---

## 2. AI 指标 (AI Metrics)

衡量 AI 管线的调用量、成功率、成本和质量。

### 2.1 Token 消耗量

| 属性 | 定义 |
|------|------|
| **指标名** | `ai_token_usage_total` |
| **口径** | DashScope API 返回的 `input_tokens + output_tokens` 总和 |
| **类型** | Counter，单调递增 |
| **labels** | `model` (glm-5.1), `operation` (classify) |
| **数据来源** | orchestrator (port 8002) → Prometheus |
| **Grafana** | "AI Token 消耗趋势"（rate 5m）、"当日 Token 总量"（24h sum）|

### 2.2 AI 费用估算

| 属性 | 定义 |
|------|------|
| **指标名** | `ai_token_cost_dollars` |
| **口径** | `input_tokens × 0.573/M + output_tokens × 2.58/M` |
| **类型** | Counter，单调递增 |
| **labels** | `model` (glm-5.1) |
| **数据来源** | orchestrator (port 8002) → Prometheus |

### 2.3 AI 处理耗时

| 属性 | 定义 |
|------|------|
| **指标名** | `ai_processing_duration_seconds` |
| **口径** | 单次 `dashscope.Generation.call()` 的响应时间 |
| **类型** | Histogram，buckets: `0.5, 1, 2, 5, 10, 20, 30, 60` |
| **labels** | `operation` (classify) |
| **数据来源** | orchestrator (port 8002) → Prometheus |

### 2.4 AI 调用成功率

| 属性 | 定义 |
|------|------|
| **指标名** | —（未独立统计，可从多指标推导）|
| **口径** | `1 - (rate_limit_hits + api_errors) / total_calls` |
| **推导逻辑** | `ai_rate_limit_hits_total` 递增 + 异常日志 → 间接估算 |
| **数据来源** | orchestrator (port 8002) → Prometheus |
| **建议** | 后续可加 `ai_success_total` / `ai_failure_total` Counter 简化此指标 |

### 2.5 429 触发次数

| 属性 | 定义 |
|------|------|
| **指标名** | `ai_rate_limit_hits_total` |
| **口径** | DashScope API 返回 `status_code=429` 的次数 |
| **类型** | Counter，单调递增 |
| **labels** | `model` (glm-5.1) |
| **Grafana 面板** | "429 触发计数" |

### 2.6 Embedding 覆盖率

| 属性 | 定义 |
|------|------|
| **指标名** | —（可间接计算）|
| **口径** | Qdrant 中向量数 / 数据库文章总数 |
| **计算方式** | `Qdrant count() / SELECT COUNT(*) FROM fact_article` |
| **当前值** | 425 / ≈425 ≈ 100% |
| **前端展示** | 无（可加）|

---

## 3. 数据质量指标 (DQ Metrics)

**⚠️ 数据质量是【数据开发面试】核心区分点。** 面试官关注你是否具备"数据可信"意识。

所有 DQ 指标均为 Prometheus Gauge，值域 `[0, 1]`（比率），每隔一个 batch 更新一次。

### 3.1 AI 摘要缺失率

| 属性 | 定义 |
|------|------|
| **指标名** | `dq_ai_summary_missing_ratio` |
| **口径** | `batch 中 ai_summary 为空的记录数 / batch 总记录数` |
| **阈值** | `< 0.2`（超过告警）|
| **计算位置** | `data_quality/validator.py` |
| **Grafana** | "AI 字段缺失率"面板 + 告警 `techpulse_summary_missing` |

### 3.2 AI 分类缺失/非法率

| 属性 | 定义 |
|------|------|
| **指标名** | `dq_ai_category_missing_ratio` |
| **口径** | `batch 中 tech_category 不在 7 个合法分类中的记录数 / batch 总记录数` |
| **合法分类** | `{AI/ML, Security, CloudNative, Programming, Hardware, DataEngineering, Others}` |
| **Grafana** | "AI 字段缺失率"面板 |

### 3.3 Others 分类占比

| 属性 | 定义 |
|------|------|
| **指标名** | `dq_others_category_ratio` |
| **口径** | `batch 中 tech_category='Others' 的记录数 / batch 总记录数` |
| **含义** | 过高（>0.4）说明 AI 分类模型异常或输入数据异常 |
| **阈值** | `< 0.4`（超过 15min 告警）|
| **Grafana** | "Others 占比"面板（橙线 0.3 / 红线 0.4）|

### 3.4 JSON 解析失败率

| 属性 | 定义 |
|------|------|
| **指标名** | `dq_json_parse_fail_ratio` |
| **口径** | `batch 中 _ai_parsed=False 的记录数 / batch 总记录数` |
| **阈值** | `< 0.2`（超过告警）|
| **触发条件** | AI 返回了非标准 JSON，或 LLM 输出被截断 |
| **Grafana 告警** | `techpulse_json_parse_fail` |

### 3.5 AI 幻觉检测率

| 属性 | 定义 |
|------|------|
| **指标名** | `dq_ai_hallucination_ratio` |
| **口径** | `batch 中包含幻觉模式（"作为AI"、"I cannot"等）的记录数 / batch 总记录数` |
| **检测模式** | `["作为AI", "作为一个AI", "无法获取", "无法访问", "抱歉", "I cannot", "I'm sorry", "As an AI", "我不具备", "我没有能力"]` |
| **阈值** | `< 0.1`（超过 5min 告警）|
| **Grafana 告警** | `techpulse_ai_hallucination` |

### 3.6 空内容率

| 属性 | 定义 |
|------|------|
| **指标名** | `dq_empty_content_ratio` |
| **口径** | 按 source 统计的 content_excerpt 为空的比例 |
| **类型** | Gauge，按 `source` 标记 |
| **数据来源** | frontend (port 8003) → Prometheus |

### 3.7 URL 重复率（24h）

| 属性 | 定义 |
|------|------|
| **指标名** | `dq_duplicate_urls_24h` |
| **口径** | 过去 24 小时内 URL 重复的被去重数量 |
| **类型** | Gauge，整数值 |
| **数据来源** | frontend `_collect_once()` → MaxCompute SQL → Prometheus |

---

## 4. 管道指标 (Pipeline Metrics)

衡量端到端 ETL 管道的运行状态和性能。

### 4.1 OSS 写入

| 指标名 | 口径 | 类型 | labels |
|--------|------|------|--------|
| `oss_write_total` | OSS 写入次数 | Counter | `target` (hn_raw), `status` (success/failure) |
| `oss_write_bytes` | OSS 写入字节数 | Counter | `target` (hn_raw) |
| `oss_write_duration_seconds` | OSS 写入耗时 | Histogram | `target` (hn_raw) |

### 4.2 Kafka 消费积压

| 属性 | 定义 |
|------|------|
| **指标名** | `kafka_consume_lag` |
| **口径** | `highwater - position`，即未消费消息数 |
| **类型** | Gauge，按 `partition` 标记 |
| **Grafana** | "Kafka 消费积压"面板 |
| **告警** | `> 1000` 触发 `techpulse_kafka_lag_high` |

### 4.3 MaxCompute 同步

| 指标名 | 口径 | 类型 |
|--------|------|------|
| `mc_sync_duration_seconds` | OSS→MC 同步耗时 | Gauge |
| `mc_sync_rows` | 本次同步行数 | Gauge |
| `mc_sync_success` | 同步成功标记（1=成功, 0=失败） | Gauge |
| `mc_query_scanned_bytes` | MaxCompute 扫描量 | Counter |
| `mc_query_duration` | MaxCompute 查询耗时 | Histogram |

### 4.4 dbt 运行

| 指标名 | 口径 | 类型 |
|--------|------|------|
| `dbt_run_duration_seconds` | dbt run 耗时 | Gauge |
| `dbt_run_success` | dbt run 成功标记（1=成功, 0=失败） | Gauge |

### 4.5 端到端延迟

| 属性 | 定义 |
|------|------|
| **指标名** | `pipeline_e2e_latency_seconds` |
| **口径** | `当前时间 - 文章最后 ingest_time`，按来源分 |
| **类型** | Gauge，按 `source` 标记 |
| **正常范围** | 通常 300-600s（受 OSS→MC 同步间隔 300s 影响）|
| **Grafana** | "端到端延迟"面板 |
| **告警** | `> 3600`（>1h）触发 `techpulse_e2e_latency_high` |

---

## 5. dbt 指标 (dbt Model Metrics)

dbt `marts` 层定义的结构化指标，直接通过 ODPS SQL 供前端查询。

### 5.1 mart_daily_summary

| 字段 | 口径 | 面试考点 |
|------|------|---------|
| `article_cnt` | 按 (ds, source, tech_category) 聚合的文章数 | `GROUP BY ROLLUP` |
| `avg_score` | 文章平均热度分 | — |
| `ai_related_cnt` | AI 相关文章数（标题含 AI/LLM/GPT 等关键词）| `CASE WHEN` |
| `ai_penetration_pct` | AI 相关文章占比 | `NULLIF(article_cnt, 0)` 安全除零 |
| `ma7_article_cnt` | 7 日滑动平均文章数 | `AVG() OVER (ROWS 6 PRECEDING)` |
| `ma7_avg_score` | 7 日滑动平均热度 | 同上 |
| `wow_change_pct` | 环比增长率 `(今天 - 昨天) / 昨天 * 100` | `LAG()` + `COALESCE` |

### 5.2 mart_trend_analysis

| 字段 | 口径 |
|------|------|
| `tech_category` | 分类 |
| `daily_cnt` | 该分类当日文章数 |
| `rank_in_day` | 当日分类排名 |

### 5.3 fact_article

| 字段 | 口径 |
|------|------|
| `id` | 文章 ID（主键）|
| `source` | 来源 |
| `title` | 标题 |
| `score` | 热度分 |
| `ai_insight` | AI 深度洞察 |
| `tech_category` | AI 分类结果 |
| `score_tier` | 热度分档（high≥100 / medium≥30 / low≥1 / unscored）|
| `is_ai_related` | 标题是否含 AI 关键词 |

---

## 6. 指标-监控映射表

| 指标 | 类型 | Prometheus | Grafana 面板 | 告警 |
|------|------|-----------|-------------|------|
| Token 消耗 | Counter | `ai_token_usage_total` | AI Token 消耗趋势 / 当日 Token 总量 | — |
| 费用 | Counter | `ai_token_cost_dollars` | — | — |
| 处理耗时 | Histogram | `ai_processing_duration_seconds` | — | — |
| 429 计数 | Counter | `ai_rate_limit_hits_total` | 429 触发计数 | — |
| AI 摘要缺失率 | Gauge | `dq_ai_summary_missing_ratio` | AI 字段缺失率 | summary_missing >0.2 |
| AI 分类缺失率 | Gauge | `dq_ai_category_missing_ratio` | AI 字段缺失率 | — |
| Others 占比 | Gauge | `dq_others_category_ratio` | Others 占比 | others >0.4 |
| JSON 解析失败率 | Gauge | `dq_json_parse_fail_ratio` | — | json_fail >0.2 |
| 幻觉率 | Gauge | `dq_ai_hallucination_ratio` | — | hallucination >0.1 |
| 爬虫采集延迟 | Gauge | `crawler_produce_lag_seconds` | 爬虫采集延迟 | — |
| Kafka 积压 | Gauge | `kafka_consume_lag` | Kafka 消费积压 | >1000 |
| OSS 写入耗时 | Histogram | `oss_write_duration_seconds` | OSS 写入耗时 | — |
| MC 同步状态 | Gauge | `mc_sync_success` | — | success==0 |
| dbt 运行状态 | Gauge | `dbt_run_success` | — | — |
| 端到端延迟 | Gauge | `pipeline_e2e_latency_seconds` | 端到端延迟 | >3600 |

---

## 7. 口径维护规则

1. **所有指标口径以本文档为准。** 新增指标必须先在此文档注册。
2. **指标命名规范：**
   - Prometheus 指标：`{domain}_{entity}_{metric}_{unit}`（如 `ai_token_usage_total`）
   - dbt 字段：`{entity}_{metric}`（如 `article_cnt`, `ai_penetration_pct`）
   - Gauge 用比率时：`{metric}_ratio`（如 `dq_ai_summary_missing_ratio`）
3. **比率指标值域 `[0, 1]`**，不在前端/告警中乘以 100。
4. **Counter 指标必须初始化 `inc(0)`** 确保 Prometheus 采集时即有数据点。
