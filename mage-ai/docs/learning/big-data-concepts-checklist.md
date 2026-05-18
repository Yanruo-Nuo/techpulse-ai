# TechPulse AI — 大数据开发概念学习清单

> ⏳ **待学习** — 项目相关但不需要修改代码的知识点，后续学习时会引用此文件。
> 每学完一条标记 ✅

---

## 1. Spark Shuffle 与 MaxCompute Shuffle 对比 🔴

**为什么需要：** 面试必问。你项目用 MaxCompute 但说不清楚它的计算引擎和 Spark 的异同。

**学习目标：**
- [ ] Spark Shuffle 的两种实现（Hash / Sort-based）
- [ ] MaxCompute 的分布式执行原理（Worker + Shuffle）
- [ ] 数据倾斜的处理方式（Spark: Salting / MaxCompute: DISTRIBUTE BY）
- [ ] 你能在项目中找到的一个数据倾斜场景

**项目关联：**
- `int_article_enriched.sql` 中 `PARTITION BY source` — 6 个来源，HN 占 60%
- 这就是一个数据倾斜的典型案例

**推荐资源：**
- [Spark Shuffle Deep Dive (Databricks)](https://www.databricks.com/blog/2020/07/29/a-deep-dive-into-shuffle-operations-in-apache-spark.html)
- MaxCompute 官方文档：`DISTRIBUTE BY` + `SORT BY`

---

## 2. Kafka 分区与消费者并行度 🔴

**为什么需要：** 你项目用了 Kafka，但当前是单分区单消费者。面试会问"数据量大了怎么办"。

**学习目标：**
- [ ] Kafka 分区分配策略（RangeAssignor / RoundRobin / Sticky）
- [ ] partition.assignment.strategy 配置
- [ ] consumer group rebalance 触发条件
- [ ] 当前项目如何改成多分区架构

**项目关联：**
- `kafka_consumer.py` 配置了 `group_id="techpulse-mage-consumer"` 但只有一个 partition
- 如果改为 6 partitions（按 source），6 个 consumer 可以并行消费

**推荐资源：**
- [Kafka Consumer Design (Confluent)](https://docs.confluent.io/platform/current/clients/consumer.html)
- 自己项目里的 `kafka_consumer.py:26-30` 的 KafkaConsumer 配置

---

## 3. Airflow DAG 设计（不实操）🟡

**为什么需要：** 当前调度是 `while True + time.sleep`，面试时不能这么说。

**学习目标：**
- [ ] Airflow 核心概念：DAG / Operator / Sensor / Task / TaskInstance
- [ ] 你能画出一个 DAG 图把当前项目的调度流程串起来吗
- [ ] Backfill 是什么、怎么用
- [ ] Airflow 和 Dagster 的核心理念区别

**项目关联：**
- `start_all.sh` 把 `kafka_consumer.py` 和 `periodic_sync.py` 丢到后台跑
- DAG 应该包含：KafkaSensor → Transform → OSS_Sink → MaxComputeSync → dbt_run → RefreshDashboard

**推荐资源：**
- [Airflow 官方教程](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- 不需要装 Airflow，能画 DAG 图 + 讲概念即可

---

## 4. Flink 基础概念 🟡

**为什么需要：** 你项目有流处理（Kafka Consumer），面试官可能问"为什么不直接用 Flink"。

**学习目标：**
- [ ] Flink checkpoint 工作原理（Barrier）
- [ ] exactly-once 语义与 Kafka consumer offset 的关系
- [ ] 你当前项目是 at-least-once（commit 在 try 块内），Flink 能到 exactly-once
- [ ] Flink 的状态管理与你的 Python dict 状态管理的对比

**项目关联：**
- `kafka_consumer.py` 中 `dlq._attempts` 是内存 dict——这就是 Flink 里的 Managed State，不过你的版本无法故障恢复
- 同样语义差异：当前 `at-least-once`（可能有重复）vs Flink `exactly-once`

**推荐资源：**
- [Flink Checkpointing (Apache)](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/)

---

## 5. Iceberg / Hudi / Delta Lake 🟢

**为什么需要：** 你项目用 OSS + Parquet（N 表格式），面试可能问"知道表格式吗"。

**学习目标：**
- [ ] 表格式解决了什么问题（ACID / Time Travel / Schema Evolution / Compaction）
- [ ] Iceberg vs Hudi vs Delta Lake 的区别
- [ ] 为什么你当前项目不适合引入（数据量 < 5000 行，metadata 比数据大）

**项目关联：**
- OSS 路径结构：`processed_data/hn/ds={ds}/batch_{timestamp}.parquet` — 裸文件
- 如果换成 Iceberg，会变成一个 `metadata/` + `data/` 的目录结构
- 小文件问题（每天 ~288 个 parquet）是 Iceberg compaction 可以解决的问题

**推荐资源：**
- [What is Iceberg (Apache)](https://iceberg.apache.org/)
- 能讲清楚概念即可，不需要实操

---

## 6. MaxCompute 特有优化技巧 🟡

**为什么需要：** 项目用了 MaxCompute，但当前 SQL 没有用任何 MaxCompute 特有的优化手段。

**学习目标：**
- [ ] `odps.sql.allow.fullscan=false`（防止误扫全表）
- [ ] MapJoin hint：小表广播到每个 worker
- [ ] Tunnel SDK：批量上传/下载
- [ ] LogView：执行计划可视化分析

**项目关联：**
- `load_news_data()` 从 `fact_article` 查询时没有限制 ds，可以用 `odps.sql.allow.fullscan=false` 保护
- `oss_to_mc_runner.py` 可以用 Tunnel SDK 替代 pandas to_sql 提升 10x 写入性能

**推荐资源：**
- [MaxCompute SQL 优化官方文档](https://help.aliyun.com/zh/maxcompute/user-guide/sql-optimization-overview/)

---

## 7. CI/CD for Data Pipelines 🟢

**为什么需要：** 项目零 CI/CD，面试可以提到你了解它但不能实操。

**学习目标：**
- [ ] GitHub Actions 触发条件（push / schedule / workflow_dispatch）
- [ ] dbt CI 最佳实践：`dbt build --select state:modified+`
- [ ] 环境管理：dev/staging/prod 的 dbt 项目怎么隔离
- [ ] 数据管线 CI/CD 和普通 CI/CD 的区别（不能随便 rollback，数据有后向兼容性）

**项目关联：**
- 如果加 CI，第一步应该是：PR 触发 → `dbt-mc build --select fact_article+` → `dbt-mc test` → 报告结果

**推荐资源：**
- [dbt CI/CD Best Practices](https://docs.getdbt.com/guides/best-practices)
