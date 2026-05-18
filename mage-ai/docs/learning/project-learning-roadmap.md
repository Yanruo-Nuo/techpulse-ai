# TechPulse AI — 完整学习路径

> **目标：** 从"能用"到"能讲"，系统性掌握这个项目的大数据开发知识。
> **学习方式：** 按阶段逐级递进，每阶段标记预计时间。

---

## 阶段 0：快速上手（30 分钟）

先让项目跑起来，感受全链路。

```bash
cd mage-ai
# 查看所有运行中的服务
docker ps --format "table {{.Names}}\t{{.Status}}"

# 访问各端
# Streamlit:      http://localhost:8501
# Grafana:        http://localhost:3000 (admin/admin)
# Prometheus:     http://localhost:9090
# Qdrant:         http://localhost:6333/dashboard
```

**看什么：**
- Streamlit → 时间线页 → KPI 横幅（累计文章 / 热门分类 / 数据新鲜度）
- Streamlit → AI 助手页 → 发一条提问（触发 Qdrant 语义检索）
- Grafana → 看 AI Token 消耗趋势面板是否有曲线

---

## 阶段 1：数据流理解（2 小时）

### 1.1 核心管道图

```
producer ──(push)──→ Kafka ──(poll)──→ consumer ──→ AI ──→ OSS ──→ MC ──→ dbt ──→ Streamlit
  scraper              topic:raw        batch=10     GLM-5.1  Parquet   ODPS    分层建模   前端
```

### 1.2 读这 5 个文件（按顺序）

| 步骤 | 文件 | 读什么 | 时间 |
|------|------|--------|------|
| ① | `docker-compose.yml` | 7 个服务的关系、端口映射、依赖链 | 10min |
| ② | `producer/main.py` | 6 个 scraper 如何轮流执行、怎么 push 到 Kafka | 15min |
| ③ | `kafka_consumer.py` | batch 消费 → transform → AI → validate → OSS 的全流程 | 20min |
| ④ | `transformers/billowing_hill.py` | AI 怎么调 GLM-5.1、怎么解析 JSON、quality validator 入口 | 15min |
| ⑤ | `oss_to_mc_runner.py` | OSS → MaxCompute → dbt-run-test 的全流程 | 15min |

### 1.3 自测

1. 当一个新的 Hacker News 文章进来，它经过哪些步骤到达 Streamlit？
2. 如果 AI 调用失败（429），会怎么样？
3. 如果 OSS 写入失败，会怎么样？
4. dbt 跑完后数据从哪里让前端读到？

---

## 阶段 2：代码深入（4 小时）

### 2.1 dbt 建模链

读这 5 个 SQL 文件，**按依赖顺序**：

```
sources.yml → stg_tech_news → int_article_enriched → fact_article → mart_daily_summary
```

| 模型 | 物化方式 | 读什么 |
|------|---------|--------|
| `stg_tech_news.sql` | view | 分区裁剪、字段清洗、NULL 过滤 |
| `int_article_enriched.sql` | table | **8 种窗口函数复习题**（ROW_NUMBER/RANK/LAG/LEAD/FIRST_VALUE）|
| `fact_article.sql` | incremental | 事实表设计、代理键、增量配置 |
| `mart_daily_summary.sql` | incremental | **ROLLUP 聚合**、7 日滑动平均、环比 |
| `dim_date.sql` | table | 日期维度预计算、与事实表对齐 |

**自测：**
1. `ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC)` 做了什么事？
2. `mart_daily_summary` 的 `wow_change_pct` 怎么算的？为什么用 `NULLIF`？
3. `fact_article` 的增量条件 `where ds > (select max(ds) from {{ this }})` 保证了什么？

### 2.2 质量与监控链

| 文件 | 读什么 |
|------|--------|
| `data_quality/validator.py` | 5 维度校验（摘要缺失/分类非法/Others 占比/JSON 失败/幻觉）|
| `data_quality/dead_letter.py` | 死信队列机制 + Prometheus 指标上报 |
| `metrics.py`（orchestrator） | Prometheus Counter/Gauge/Histogram 的用法 |
| `metrics_collector.py`（frontend） | DQ gauges + MC 查询耗时 + 端到端延迟 |

### 2.3 前端展示

| 文件 | 读什么 |
|------|--------|
| `panels/timeline.py` | KPI 横幅的数据来源、图表渲染 |
| `panels/assistant.py` | Qdrant 检索流程、prompt 构建 |
| `vector_store.py` | Qdrant 客户端封装、HNSW 搜索 |

---

## 阶段 3：大数据概念串联（按 checklist 顺序）

打开 `docs/learning/big-data-concepts-checklist.md`，按优先级学习。

### 3.1 Spark Shuffle vs MaxCompute（2 小时）

**关联代码：** `int_article_enriched.sql` 的 `PARTITION BY source`

```
你的代码                     对应的分布式概念
────────────────────────────────────────────────
PARTITION BY source    →     Spark 中的 partitionBy
ORDER BY score DESC    →     全局排序需要 shuffle
6 个 source 数据不均    →    数据倾斜（data skew）
```

**你需要能回答：**
- Spark Shuffle 是什么时候发生的？（`groupBy`, `join`, `orderBy`, `distinct`）
- 你项目中哪个查询有数据倾斜？怎么发现？怎么优化？
- MaxCompute 的 `DISTRIBUTE BY` 和 Spark 的 `partitionBy` 是什么关系？

### 3.2 Kafka 分区与消费者（1 小时）

**关联代码：** `kafka_consumer.py`

```
当前设计                          面试对标
────────────────────────────────────────────────
1 topic, 1 partition          → 无并行瓶颈
1 consumer, batch=10          → 无容错保证
commit 在 try 块内（at-least-once）→ 可能有重复
```

**你需要能回答：**
- 如果要处理 10 倍数据量，你怎么加分区？
- consumer group 的 rebalance 是什么？怎么避免？
- 当前是 at-least-once，如果要做 exactly-once 需要改什么？

### 3.3 Airflow 调度话术（1 小时）

**关联代码：** `start_all.sh`

**画个 DAG 图：**

```
sensor_kafka >> consumer >> oss_sync >> mc_sync >> dbt_build >> refresh_metric
```

**你需要能说：**
- 我用 `while True` 是因为项目初期不需要 Airflow——但我设计的 DAG 是..."
- KafkaSensor 检测 `raw_tech_feeds` 有新消息才触发下游
- dbt operator 跑完之后自动触发 `dbt test`
- 失败走自定义 retry + 死信

### 3.4 Flink Checkpoint（30 分钟）

**关联代码：** `kafka_consumer.py` 的 `_run_loop` + `DeadLetterQueue`

| 你的代码 | Flink 等价概念 |
|---------|---------------|
| `buffer` | Operator State |
| `dlq._attempts` | Keyed State |
| `consumer.commit()` | Checkpoint Barrier |
| 进程崩溃 → buffer 丢失 | Managed State 自动恢复 |

**面试话术：**
> "我当前的状态管理是内存 dict，一旦进程崩溃就丢了。换成 Flink 后，`_attempts` 可以存在 Flink 的 Keyed State 里，自动 checkpoint 到 RocksDB + S3，做到 exactly-once。"

---

## 阶段 4：面试演练（2 小时）

### 4.1 项目介绍（30 秒版）

> "TechPulse AI 是一个 AI 增强的数据工程平台。6 个爬虫采集技术新闻 → Kafka 解耦 → DashScope AI 自动分类和摘要 → OSS 数据湖 → MaxCompute 数仓 → dbt 分层建模（ODS→DWD→DWS→ADS）→ Streamlit 前端。核心工程：死信队列、5 维度数据质量校验、Qdrant 向量检索、Prometheus + Grafana 监控。"

### 4.2 常见面试题

| 问题 | 回答要点 | 关联 |
|------|---------|------|
| "项目规模多大？" | 日均 100-500 篇，425 向量，6 来源 | stage 1 |
| "怎么保证数据质量？" | 5 维度 batch 校验 + Prometheus 告警 + dbt test | stage 2 |
| "数仓怎么分层的？" | ODS/sources → DWD/staging+intermediate → DWS-ADS/marts | stage 2 |
| "为什么用这个技术栈？" | 阿里云生态+个人项目→ MaxCompute 免费配额；AI 能力→ DashScope | stage 1 |
| "数据量大了怎么办？" | Kafka 分区扩展 + consumer 并行 + dbt 增量 + OSS 分区 | stage 3 |
| "你怎么调度的？" | 当前 while True（创业初期），DAG 设计应该是... | stage 3 |
| "你知道 Spark 吗？" | 和 MaxCompute 的异同 + 你项目的窗口函数类比 | stage 3 |

---

## 阶段 5：命令行速查

```bash
# ─── Docker ───
docker ps                          # 7 个容器状态
docker logs techpulse-orchestrator --tail 50   # 看 pipeline 日志
docker exec tech-frontend python /app/vector_sync.py  # 手动同步向量

# ─── Prometheus ───
curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=ai_token_usage_total' | python3 -m json.tool

# ─── Grafana ───
# http://localhost:3000/d/techpulse  admin/admin
# 9 个面板，关键看: Token 消耗、429 触发、Kafka 积压

# ─── dbt ───
# 容器内:
docker exec techpulse-orchestrator bash -c \
  "cd /home/src/techpulse_dbt && dbt-mc run --select fact_article+"

# ─── Qdrant ───
curl -s http://localhost:6333/collections/tech_news | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['points_count'])"
# 应看到: 425+ 条向量
```

---

## 📋 学习进度表

| 阶段 | 内容 | 预计时间 | 完成 |
|------|------|---------|------|
| 0 | 快速上手，确认项目运行 | 30min | ⬜ |
| 1 | 数据流理解，读 5 个核心文件 | 2h | ⬜ |
| 2 | 代码深入（dbt 链 + 质量链 + 前端链） | 4h | ⬜ |
| 3.1 | Spark Shuffle vs MaxCompute | 2h | ⬜ |
| 3.2 | Kafka 分区与消费者 | 1h | ⬜ |
| 3.3 | Airflow 调度话术 | 1h | ⬜ |
| 3.4 | Flink Checkpoint | 30min | ⬜ |
| 4 | 面试演练 | 2h | ⬜ |
| 总 | | ~13h | ⬜ |

---

> **最后更新：** 2026-05-10

---

## 📝 学习过程中的疑问（待解答）

以下问题在完整学习项目后统一回答。

### 问题 1：AI 输入截断 8000 字符 → 核心内容丢失？

**来源：** `billowing_hill.py:57` — `truncated = content[:8000] if content else title`

**疑问：** 一篇文章的核心论点可能在开头之后才出现，8000 字符的硬截断会导致 AI 只读到前半部分，可能根本看不到关键内容。如果文章很长（如技术论文），截断后 AI 的质量会急剧下降。

### 问题 2：AI 输出限制 2048 tokens → 摘要深度不足？

**来源：** `billowing_hill.py:62` — `max_tokens=2048`

**疑问：** `max_tokens=2048` 的输出限制，要求 AI 同时输出 `ai_summary`(200-300字)、`tech_category`(1个词)、`ai_insight`(300-500字)。中文一个字 ≈ 2 tokens，2048 tokens 约等于 1000 中文字。光 `ai_insight` 就要求 300-500 字，加上 `ai_summary` 的 200-300 字，实际可用空间非常紧张。会不会导致 AI 被迫省略关键分析？

### 问题 3：实时 batch 质检如何保证异常数据不落到下游？

**来源：** `kafka_consumer.py:89` — `validate_batch(ai_result)` + `report_metrics(dq_checks)`

**疑问：** 文档说"实时 batch 级质检，异常数据不会落到下游表"。但代码中 `validate_batch` 只做校验+上报指标，没有过滤或阻断异常数据的逻辑。校验通过/不通过，数据都会继续走到 `sink.batch_write()`。这样真的能阻止异常数据入库吗？还是说异常数据只是"被标记"而非"被拦截"？

### 问题 4：batch_size=10 是否需要调整？

**来源：** `kafka_consumer.py:16` — `BATCH_SIZE = 10`

**疑问：** 当前 batch_size=10，但一天可能有 288 个 batch（每 5 分钟一个），产生大量小文件。增大 batch_size 的好处和坏处各是什么？什么场景下应该增大/减小？

### 问题 5：爬虫采集延迟指标是否有必要？

**来源：** `producer/main.py:67-71` — `crawler_produce_lag_seconds`

**疑问：** 指标计算 `lag = now - article.published_at`，技术新闻的发布时间可能不准（来源不同、时区问题），而且 6 个爬虫的 published_at 格式不一致。这个指标的值可靠吗？还是说有虚假延迟？它真的有监控价值，还是只是"指标好看"？

### 问题 6：爬虫串行运行是否需要改成并行？

**来源：** `producer/main.py:49-80` — `for scraper in scrapers:` 串行执行

**疑问：** 当前 6 个爬虫依次执行。如果一个爬虫卡住（网络超时 / API 限流），后续所有爬虫都会延迟，推入 Kafka 的时间会整体偏移。改成 `concurrent.futures.ThreadPoolExecutor` 会有什么问题？（资源竞争 / API 限流同时触发 / 数据乱序）

### 问题 7：AI prompt 模板的改进空间

**来源：** `billowing_hill.py:37-58` — `CLASSIFY_PROMPT_TEMPLATE`

**疑问：** 当前 prompt 要求 AI 输出"技术摘要"和"深度分析"，但 prompt 中没有给出任何参考示例（few-shot）。对于 GLM-5.1 这类模型，few-shot 能显著提升输出格式和内容质量。是否应该加入 2-3 个示例？另外，`tech_category` 的枚举值（如`AI/ML`、`CloudNative`）在 prompt 中只列出名字没有定义，模型可能不理解边界。是否需要给每个分类的判定标准？

### 问题 8：ODS 层只做引用声明是 dbt 最佳实践吗？

**来源：** `sources.yml` — `tables: [{name: hn_raw}]`

**疑问：** dbt 文档确实推荐 ODS 层用 `sources.yml` 做引用声明，不在 ODS 层建物理表。但这样有什么实际好处？与在 ODS 层建物理表有什么区别？有没有场景应该建物理 ODS 表？

### 问题 9：Counter / Gauge / Histogram 的分类和作用

**来源：** `metrics.py` — 三种 Prometheus 指标类型

**疑问：** 项目中同时用到了 Counter（token 用量）、Gauge（Kafka 积压）、Histogram（AI 处理耗时）。在实际的场景中，什么指标应该用 Counter、什么应该用 Gauge、什么应该用 Histogram？选择依据是什么？选错了类型的代价是什么？

### 问题 10：实时检测的作用点——为什么不能直接走 Grafana 而要 Prometheus 中转？

**来源：** `validator.py` → Prometheus → Grafana

**疑问：** `validator.py` 计算出 5 维度比率后，不是直接推给 Grafana，而是通过 Prometheus Gauge 存一份，Grafana 再从 Prometheus 拉。为什么多这一层中转？直接推给 Grafana 不行吗？Prometheus 在这中间起到了什么不可替代的作用？

### 问题 11：过滤在前端内存中执行 vs 下推到 SQL

**来源：** `timeline.py` — `filtered_df = df_news.copy()` + `.isin()` + `.sort_values()`

**疑问：** 分类过滤、来源过滤、排序打分全部在 DataFrame 上用 pandas 操作。数据量目前 ~400 条无感，但增加到 10 万条时，前端就会卡死。更好的做法是把过滤条件作为 SQL WHERE 子句，让 MaxCompute 只返回需要的数据。两者各自的优劣是什么？什么时候应该用前端过滤、什么时候该下推 SQL？

### 问题 12：get_rag_response 的完整链路详解

**来源：** `assistant.py:16-80` — `get_rag_response()`

**疑问：**
- 第一步 DashScope embedding 调用失败怎么办？有重试吗？
- Qdrant search 如果没结果（空向量库），有兜底吗？
- LLM prompt 中拼接了 `dist_text`（分类分布）和 `hot_text`（热门排行），但这些内容是从 `df` 全量计算的——它绕过了 Qdrant。这个设计背后的考虑是什么？
- 多次对话时 `history_text` 拼接最近 6 条消息，但 prompt 如何保证 AI 不跑偏？

### 问题 13：全量同步 vs 增量同步

**来源：** `vector_sync.py` — `sync_all()`

**疑问：** 当前的 `vector_sync.py` 只实现了全量同步：每次从 MaxCompute 拉全部文章 → 重新生成 embedding → 覆盖写入 Qdrant。如果文章数增长到 10 万篇，全量同步的时间和经济成本都很高。增量同步应该怎么做？如何判断哪些文章是新文章、哪些需要更新 embedding？

### 问题 14：什么是 dbt marts？

**来源：** 前端加载 `fact_article(dbt marts)`

**疑问：** dbt 项目中 `marts/` 目录存放的是 DWS/ADS 层模型。但"marts"这个术语在数仓中的具体含义是什么？它和"data mart"（数据集市）是什么关系？dbt 的 marts 目录和 staging、intermediate 的本质区别是什么？一个项目应该有多少 marts？

**来源：** `validator.py` → Prometheus → Grafana

**疑问：** `validator.py` 计算出 5 维度比率后，不是直接推给 Grafana，而是通过 Prometheus Gauge 存一份，Grafana 再从 Prometheus 拉。为什么多这一层中转？直接推给 Grafana 不行吗？Prometheus 在这中间起到了什么不可替代的作用？

**来源：** `sources.yml` — `tables: [{name: hn_raw}]`

**疑问：** dbt 文档确实推荐 ODS 层用 `sources.yml` 做引用声明，不在 ODS 层建物理表。但这样有什么实际好处？与在 ODS 层建物理表有什么区别？有没有场景应该建物理 ODS 表？

**来源：** `kafka_consumer.py:16` — `BATCH_SIZE = 10`

**疑问：** 当前 batch_size=10，但一天可能有 288 个 batch（每 5 分钟一个），产生大量小文件。增大 batch_size 的好处和坏处各是什么？什么场景下应该增大/减小？

