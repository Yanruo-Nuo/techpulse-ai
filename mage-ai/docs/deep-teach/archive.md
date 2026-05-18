# deep-teach 教学卡片归档

> 自动记录每次编码操作的深度技术解析卡片。
> 格式：`YYYY-MM-DD — STEP N: <操作>`

---

### STEP 14 — 大数据开发深度补强

**代码改动：**
- `techpulse_dbt/models/marts/dim_date.sql` — 新增日期维度表
- `techpulse_dbt/models/marts/schema.yml` — 新增 dim_date 测试定义
- `README.md` — 新增 Kimball 建模方法表
- `docs/learning/sql-execution-plan-analysis.md` — 执行计划 + 优化分析

**学习清单（docs/learning/big-data-concepts-checklist.md）：**

| # | 主题 | 优先级 | 项目关联 |
|---|------|--------|---------|
| 1 | Spark Shuffle vs MaxCompute | 🔴 | int_article_enriched 数据倾斜 |
| 2 | Kafka 分区与并行 | 🔴 | kafka_consumer.py 消费模型 |
| 3 | Airflow DAG 话术 | 🔴 | while True 调度替代方案 |
| 4 | Flink checkpoint | 🟡 | at-least-once vs exactly-once |
| 5 | Iceberg 表格式 | 🟢 | OSS 裸文件 vs Iceberg |
| 6 | MaxCompute 优化 | 🟡 | odps.sql.allow.fullscan, MapJoin |
| 7 | CI/CD for Data | 🟢 | GitHub Actions + dbt test |

当后续会话中提到"大数据学习"或"概念学习"时，应优先打开 `docs/learning/big-data-concepts-checklist.md` 文件，按优先级逐项进行。

---

### STEP 11 — dbt 增量策略 + 分区过滤

**改动文件：**
- `techpulse_dbt/dbt_project.yml` — marts 层默认 `incremental` + `insert_overwrite`
- `techpulse_dbt/models/staging/stg_tech_news.sql` — 分区裁剪，限扫 30 天
- `techpulse_dbt/models/marts/fact_article.sql` — 改为 incremental，`unique_key=article_id`
- `techpulse_dbt/models/marts/mart_daily_summary.sql` — 改为 incremental，按 ds 增量处理

**面试可讲：**
- "我将 dbt marts 从全量刷新改为 insert_overwrite 增量模式，仅处理新分区"
- "staging 层加了 30 天分区裁剪，避免全表扫描"
- "int_article_enriched 保留全量刷新——窗口函数需要完整数据集才能正确计算 RANK/LAG"
- "这是分层建模中 view→table→incremental 的典型策略选择"

---

### STEP 12 — dbt tests 补全 + pipeline 集成

**改动文件：**
- `techpulse_dbt/models/staging/schema.yml` — 15+ 字段测试覆盖
- `techpulse_dbt/models/marts/schema.yml` — 6 个模型，含 relationships / accepted_values
- `techpulse_dbt/tests/custom_assertions.sql` — 5 条自定义业务断言
- `techpulse_intelligence/oss_to_mc_runner.py` — `dbt run` → `dbt test` 自动执行

**测试统计：**
- `unique`: 6 个 key 字段
- `not_null`: 15+ 核心字段
- `accepted_values`: 5 组枚举值
- `relationships`: fact_article → dim_source 外键
- `custom`: score≥0, article_cnt>0, 时效性, 空标题, 分区内唯一

**面试可讲：**
- "dbt 测试分三层：schema 级通用测试 → 自定义 singular 断言 → pipeline 集成自动执行"
- "tests 失败不阻断 pipeline（避免假阳性阻塞），但记录到 Prometheus 日志可追踪"
- "这个项目的核心数据质量原则：每张 marts 表都有一个 unique + not_null 的主键"

---

### STEP 13 — DLQ 监控补全

**改动文件：**
- `data_quality/dead_letter.py` — 新增 `dlq_records_total` Counter + `dlq_pending_total` Gauge
- `grafana/alerting/alert-rules.yml` — 2 条 DLQ 告警（pending + rate）

**告警规则：**
- `dlq_pending_total > 0` for 5m → Warning（有待重试批次）
- `rate(dlq_records_total[5m]) > 0` for 5m → Critical（持续有死信产生）

**面试可讲：**
- "死信队列不仅有文件记录，还通过 Prometheus 上报积压数量和写入速率"
- "没有 DLQ 告警 = 数据丢了不知道，加了才能说'数据可靠性有保障'"

---

**改动文件：**
- `techpulse_dbt/models/staging/schema.yml` — 15+ 字段测试覆盖
- `techpulse_dbt/models/marts/schema.yml` — 6 个模型，含 relationships / accepted_values
- `techpulse_dbt/tests/custom_assertions.sql` — 5 条自定义业务断言
- `techpulse_intelligence/oss_to_mc_runner.py` — `dbt run` → `dbt test` 自动执行

**测试统计：**
- `unique`: 6 个 key 字段
- `not_null`: 15+ 核心字段
- `accepted_values`: 5 组枚举值
- `relationships`: fact_article → dim_source 外键
- `custom`: score≥0, article_cnt>0, 时效性, 空标题, 分区内唯一

**面试可讲：**
- "dbt 测试分三层：schema 级通用测试 → 自定义 singular 断言 → pipeline 集成自动执行"
- "tests 失败不阻断 pipeline（避免假阳性阻塞），但记录到 Prometheus 日志可追踪"
- "这个项目的核心数据质量原则：每张 marts 表都有一个 unique + not_null 的主键"

**改动文件：**
- `techpulse_dbt/dbt_project.yml` — marts 层默认 `incremental` + `insert_overwrite`
- `techpulse_dbt/models/staging/stg_tech_news.sql` — 分区裁剪，限扫 30 天
- `techpulse_dbt/models/marts/fact_article.sql` — 改为 incremental，`unique_key=article_id`
- `techpulse_dbt/models/marts/mart_daily_summary.sql` — 改为 incremental，按 ds 增量处理

**面试可讲：**
- "我将 dbt marts 从全量刷新改为 insert_overwrite 增量模式，仅处理新分区"
- "staging 层加了 30 天分区裁剪，避免全表扫描"
- "int_article_enriched 保留全量刷新——窗口函数需要完整数据集才能正确计算 RANK/LAG"
- "这是分层建模中 view→table→incremental 的典型策略选择"

---

## 2026-05-10

### STEP 1 — 项目架构全解析

**对应会话：** 初次项目分析
**状态：** ✅ 已归档（对话记录）

---

### STEP 2 — 求职策略与项目升级路线分析

**对应会话：** 求职路径规划
**状态：** ✅ 已归档（对话记录）

---

### STEP 3 — README 重写完成

**文件：** `mage-ai/README.md`
**状态：** ✅ 已归档（对话记录）

---

### STEP 4 — dbt 文档一致性改造完成

**文件：** `mage-ai/techpulse_dbt/README.md`
**状态：** ✅ 已归档（对话记录）

---

### STEP 5 — 指标体系文档完成

**文件：** `mage-ai/docs/metrics.md`
**状态：** ✅ 已归档（对话记录）

---

### STEP 6 — KPI 横幅改造完成

**文件：** `mage-ai/frontend/panels/timeline.py`
**状态：** ✅ 已归档（对话记录）

---

### STEP 7 — 新鲜度计算 bug 修复

**文件：** `mage-ai/frontend/panels/timeline.py`
**状态：** ✅ 已归档（对话记录）

---

### STEP 8 — 项目大数据开发面试审计

**对应会话：** 审计评估
**状态：** ✅ 已归档（对话记录）

---

### STEP 9 — 前端改为查询 dbt fact_article

**文件：** `mage-ai/frontend/maxcompute.py`, `mage-ai/techpulse_dbt/models/**/*.sql`
**状态：** ✅ 已归档（对话记录）

---
