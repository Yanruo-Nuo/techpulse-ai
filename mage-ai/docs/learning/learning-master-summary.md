# TechPulse AI — 大数据学习总纲

> 学习状态快照。包含：已读文件、待学概念、待答问题、项目知识图谱。

---

## 第一部分：项目架构总览（已掌握 ✅）

### 全链路数据流

```
producer/main.py          kafka_consumer.py              oss_to_mc_runner.py          Streamlit
┌──────────────┐          ┌──────────────────┐          ┌──────────────────┐         ┌──────────┐
│ 6 scrapers    │──push──→│ consumer poll     │──write──→│ OSS Parquet       │──dbt──→│ timeline │
│ 串行 30s 一轮  │  Kafka  │ batch=10          │   OSS    │                   │        │ KPI+图表  │
│ 冷却5次→1h     │          │                   │          │ OSS → MC (每300s)  │        ├──────────┤
│ 6个源         │          │ transform_fetch() │          │                   │        │ assistant│
└──────────────┘          │ → quixotic_illusion│          │ dbt-mc run         │        │ RAG 检索  │
                          │                   │          │ → fact_article     │        │ Qdrant   │
                          │ transform_ai()    │          │ → mart_daily_summary│        └──────────┘
                          │ → billowing_hill   │          └──────────────────┘
                          │ → GLM-5.1          │                │
                          │ → retry ×2          │                ▼
                          │ → fallback          │          fact_article (125,801 rows)
                          │                   │          dim_source (6 rows)
                          │ validate_batch()   │          dim_date (0 rows - 待填充)
                          │ → 5-DQ + Prometheus│          int_article_enriched
                          │ → dlq_records_total│          mart_daily_summary
                          │ → dlq_pending_total│          mart_trend_analysis
                          │                   │
                          │ sink.batch_write() │
                          │ → OSS Parquet      │
                          └──────────────────┘
```

### 监控体系

```
3 Prometheus 端点 (8001/8002/8003) → Prometheus → Grafana (9 panels + 9 alert rules)

爬虫端(8001):    crawler_articles_total, crawler_in_cooldown, crawler_produce_lag
加工端(8002):    ai_token_usage_total, ai_token_cost, ai_rate_limit, oss_write, kafka_lag
前端端(8003):    dq_*_ratio, mc_query_scanned, frontend_page_load, pipeline_e2e_latency
```

---

## 第二部分：待学概念（阶段 3）

按优先级排列，每项对应项目中的具体代码。

### 🔴 概念 1：Spark Shuffle vs MaxCompute Shuffle

| 学习项 | 项目关联 |
|--------|---------|
| Spark Shuffle 两种实现（Hash / Sort-based） | — |
| 数据倾斜处理（Salting / AQE / DISTRIBUTE BY） | `int_article_enriched.sql` — `PARTITION BY source`（HN 占 60%，倾斜）|
| MaxCompute DISTRIBUTE BY + SORT BY | 解决上面那个倾斜 |
| ✅ **本对话中已学了前半部分，需复习** | — |

### 🔴 概念 2：Kafka 分区与消费者并行度

| 学习项 | 项目关联 |
|--------|---------|
| 分区分配策略（Range / RoundRobin / Sticky） | `kafka_consumer.py` — 当前 1 topic 1 partition 1 consumer |
| consumer group rebalance | `group_id="techpulse-mage-consumer"` |
| 如何改成多分区架构 | 6 sources → 6 partitions |

### 🔴 概念 3：Airflow DAG 设计

| 学习项 | 项目关联 |
|--------|---------|
| DAG / Operator / Sensor / Task | `start_all.sh` — while True 替代方案 |
| 画出当前项目的 DAG 图 | KafkaSensor → Transform → OSS → MC_Sync → dbt → Frontend |
| Backfill 用途 | — |

### 🟡 概念 4：Flink 基础

| 学习项 | 项目关联 |
|--------|---------|
| Checkpoint + Barrier | `kafka_consumer.py` — at-least-once vs exactly-once |
| Managed State vs 内存 dict | `dlq._attempts` = Flink Keyed State |
| 为什么当前项目没直接用 Flink | — |

### 🟢 概念 5：Iceberg / Hudi / Delta Lake

| 学习项 | 项目关联 |
|--------|---------|
| 表格式解决什么问题（ACID / Time Travel / Compaction） | OSS Parquet 裸文件 vs Iceberg metadata |
| 小文件问题 | 每天 ~288 parquet，Iceberg compaction 可解决 |
| 当前数据量（125k rows）是否需要 Iceberg？ | — |

### 🟡 概念 6：MaxCompute 特有优化

| 学习项 | 项目关联 |
|--------|---------|
| `odps.sql.allow.fullscan=false` | `load_news_data()` 无 ds 限制 → 可用此保护 |
| MapJoin hint | — |
| Tunnel SDK（10x 写入性能） | `oss_to_mc_runner.py` 的 `open_writer` 替代方案 |

### 🟢 概念 7：CI/CD for Data

| 学习项 | 项目关联 |
|--------|---------|
| GitHub Actions + dbt CI | `dbt-mc test` 从未在 CI 中运行 |
| 环境隔离（dev/staging/prod） | 当前只有一个 `target: dev` |

---

## 第三部分：待答问题（14 个）

完整学习项目后统一回答。

### AI 管线

1. **AI 输入截断 8000 字符 → 核心内容丢失？**
   - `billowing_hill.py:57` — `content[:8000]`
   - 长文章核心内容在后面的情况

2. **AI 输出限制 2048 tokens → 摘要深度不足？**
   - `billowing_hill.py:62` — `max_tokens=2048`
   - 中文 1 字 ≈ 2 tokens，空间紧张

3. **AI prompt 模板的改进空间？**
   - 缺少 few-shot 示例
   - `tech_category` 枚举缺判定标准定义
   - prompt 中没有示例数据

### 管道设计

4. **batch_size=10 是否需要调整？**
   - `BATCH_SIZE = 10`
   - 每天 ~288 个 batch → 小文件问题
   - 增大/减小的优劣

5. **实时 batch 质检如何保证异常数据不落到下游？**
   - `validate_batch` 只上报不拦截
   - 是否是"标记而非拦截"？

6. **爬虫采集延迟指标是否有必要？**
   - `crawler_produce_lag_seconds`
   - 发布时间可能不准

7. **爬虫串行运行是否需要改成并行？**
   - `for scraper in scrapers` 串行
   - ThreadPoolExecutor 的风险

### 数据架构

8. **ODS 层只做引用声明是 dbt 最佳实践吗？**
   - `sources.yml` — 只声明不建表
   - 什么时候应该建物理 ODS 表？

9. **Counter / Gauge / Histogram 的分类和作用？**
   - 项目中 3 种类型的使用
   - 选错类型的代价

10. **为什么不能直接走 Grafana 而要 Prometheus 中转？**
    - `validator.py → Prometheus → Grafana`
    - Prometheus 的不可替代作用

11. **过滤在前端内存中执行 vs 下推到 SQL？**
    - `filtered_df = df_news.copy()`
    - 什么时候用前端过滤、什么时候下推

12. **get_rag_response 的完整链路？**
    - embedding 失败怎么办？
    - Qdrant 无结果怎么办？
    - prompt 中为什么混了全量 df 的计算？

13. **全量同步 vs 增量同步？**
    - `vector_sync.py` 全量
    - 增量同步怎么做？

### 数仓理论

14. **什么是 dbt marts？**
    - data mart = 数据集市
    - staging / intermediate / marts 的本质区别

---

## 第四部分：已读文件清单 ✅

| # | 文件 | 核心内容 |
|---|------|---------|
| 1 | `docker-compose.yml` | 7 容器架构、端口、依赖链 |
| 2 | `producer/main.py` | 6 爬虫串行调度、冷却机制、Kafka push |
| 3 | `kafka_consumer.py` | 消费→清洗→AI→质检→OSS，commit 时序 |
| 4 | `transformers/billowing_hill.py` | GLM-5.1 调用、prompt、重试、兜底 |
| 5 | `oss_to_mc_runner.py` | OSS→MC→dbt 离线批处理 |
| 6 | `models/sources.yml` | ODS 层引用声明 |
| 7 | `models/staging/stg_tech_news.sql` | DWD 层清洗 + 分区裁剪 |
| 8 | `models/intermediate/int_article_enriched.sql` | DWD 层 8 种窗口函数 |
| 9 | `models/marts/fact_article.sql` | DWS 事实表（代理键 + 外键 + 退化维度）|
| 10 | `models/marts/mart_daily_summary.sql` | ADS 层 ROLLUP + 滑动平均 + 环比 |
| 11 | `data_quality/validator.py` | 5 维度 AI 输出实时校验 |
| 12 | `data_quality/dead_letter.py` | 死信队列 + Prometheus 指标 |
| 13 | `techpulse_intelligence/metrics.py` | 3 种 Prometheus 指标类型 |
| 14 | `frontend/metrics_collector.py` | 离线侧周期性质量采集 |
| 15 | `frontend/panels/timeline.py` | KPI 横幅 + 图表 + 过滤 |
| 16 | `frontend/panels/assistant.py` | RAG 检索 + Qdrant |
| 17 | `frontend/vector_store.py` | Qdrant HNSW 封装 |
| 18 | `grafana/alerting/alert-rules.yml` | 9 条告警规则 |

---

## 第五部分：学习进度

| 阶段 | 内容 | 时间 | 完成 |
|------|------|------|------|
| 0 | 快速上手 | 30min | ✅ |
| 1 | 数据流理解（5 文件） | 2h | ✅ |
| 2 | 代码深入（13 文件） | 4h | ✅ |
| 3.1 | Spark Shuffle vs MaxCompute | 2h | 🟡 已学一半 |
| 3.2 | Kafka 分区与消费者 | 1h | ⬜ |
| 3.3 | Airflow DAG 话术 | 1h | ⬜ |
| 3.4 | Flink 基础 | 30min | ⬜ |
| 3.5 | Iceberg 表格式 | 30min | ⬜ |
| 3.6 | MaxCompute 优化 | 30min | ⬜ |
| 3.7 | CI/CD for Data | 30min | ⬜ |
| 4 | 面试演练 | 2h | ⬜ |
| 最后 | 统一回答 14 个问题 | 2h | ⬜ |
| **总计** | | **~14h** | **~50%** |
