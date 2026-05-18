# TechPulse AI — dbt 数仓建模

> **ODS → DWD → DWS/ADS** 三层数据仓库建模，基于 MaxCompute 实现的技术新闻指标体系。

---

## 项目概览

TechPulse AI 的 dbt 项目负责将**经过 AI 增强的原始文章数据**，按照标准数仓分层方法论，从原始接入逐步转换为可直接供前端使用的分析表。

**数据来源：** MaxCompute 表 `techpulse_dw.default.hn_raw`（从阿里云 OSS 定时同步）
**数据平台：** 阿里云 MaxCompute（ODPS 2.0 + SQL兼容）
**方言适配：** `dbt-mc`（dbt MaxCompute 社区适配器）

---

## 数仓分层

### ODS 层 — 原始数据接入

| 文件名 | 类型 | 说明 |
|--------|------|------|
| `sources.yml` | 源定义 | 声明 `techpulse_dw.default.hn_raw` 为原始数据源 |

ODS 层不创建新表，而是通过 `sources.yml` 声明外部数据源，供下游模型引用。`hn_raw` 表由 `oss_to_mc_runner.py` 定时从 OSS Parquet 全量同步。

### DWD 层 — 明细与宽表

| 模型 | 物化 | 说明 | 行数 |
|------|------|------|------|
| `stg_tech_news` | view | 基础清洗：字段裁剪、NULL 过滤、简单分类规则 | ≈全量 |
| `int_article_enriched` | table | **宽表增强**：8 种窗口函数衍生特征，每日刷新 | ≈全量 |

**`int_article_enriched` 的窗口函数考点（SQL 面试复习用）：**

| # | 函数 | 作用 | 面试题对应 |
|---|------|------|-----------|
| ① | `ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC)` | 来源内按分排名去重 | "每个来源的最高分文章" |
| ② | `RANK() OVER (ORDER BY score DESC)` | 全局排名（跳跃序号） | "全局文章 Top-N" |
| ③ | `LAG(score) OVER (PARTITION BY source ORDER BY created_at)` | 来源内上一篇文章分差 | "文章热度的时序变化" |
| ④ | `LEAD(score)` | 来源内下一篇文章分差 | ③的对偶 |
| ⑤ | `FIRST_VALUE(title) OVER (PARTITION BY source ORDER BY score DESC)` | 来源内最高分文章标题 | "每个分类下 Top-1 标题" |
| ⑥ | `COUNT(*) OVER (PARTITION BY source)` | 来源累计文章数 | "来源文章分布" |
| ⑦ | `ROW_NUMBER() OVER (PARTITION BY source ORDER BY created_at)` | 来源内采集时序编号 | "采集延时分析" |
| ⑧ | `COALESCE(score - LAG(score))` | 分差安全处理 | "防御性 SQL 写法" |

### DWS/ADS 层 — 汇总与应用

| 模型 | 物化 | 粒度 | 说明 |
|------|------|------|------|
| `fact_article` | table | 每篇文章一条 | 文章基础事实表 |
| `dim_source` | table | 每个来源一条 | 来源维度表 |
| `mart_daily_summary` | table | 每天一条 | 按天/分类汇总（采集量、AI 成功率） |
| `mart_trend_analysis` | table | 每天一条 | 7日/30日滑动聚合、趋势分析 |

---

## 物化策略

```yaml
# dbt_project.yml
models:
  techpulse_dbt:
    staging:
      +materialized: view        # 轻量，实时映射源表
    intermediate:
      +materialized: table        # 需要物化用于下游查询
    marts:
      +materialized: table        # 汇总表，每日全量刷新
```

---

## dbt tests

**位置：** `models/staging/schema.yml`

| 测试类型 | 目标字段 | 说明 |
|---------|---------|------|
| `unique` | id | 文章 ID 唯一性 |
| `not_null` | id, title, source | 核心字段非空 |
| `accepted_values` | tech_category | 限制为 7 个合法分类值 |

---

## 快速使用

```bash
# 查看 dbt 模型依赖图
dbt-mc docs generate
dbt-mc docs serve

# 运行全部模型
dbt-mc run

# 运行并测试
dbt-mc build

# 只跑指定模型及其下游
dbt-mc run --select int_article_enriched+
```

---

## 面试关联

这个 dbt 项目在面试中可以展示的 SQL 能力：

1. **窗口函数** (`ROW_NUMBER`, `RANK`, `LAG/LEAD`, `FIRST_VALUE`) — 高频面试考点
2. **CTE 多层嵌套** — 复杂查询分解能力
3. **防御性 SQL** (`COALESCE`, `NULLIF`) — 生产级写法意识
4. **Source freshness** — 数据新鲜度验证
5. **模型物化策略** — view vs table 的选择依据

---

## 文档引用

完整的架构和项目说明请参考 [主项目 README](../README.md)。
