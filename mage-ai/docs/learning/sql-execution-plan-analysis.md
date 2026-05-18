# TechPulse AI SQL 执行计划与性能分析

> 用于面试时展示 SQL 优化意识。每份分析包含：
> - 原始 SQL
> - 执行计划产出
> - 瓶颈分析
> - 优化建议

---

## 分析 1：fact_article 全量查询

### SQL

```sql
SELECT article_id, title, source, tech_category, score
FROM fact_article
WHERE ds IS NOT NULL
ORDER BY article_created_at DESC
```

### 执行计划（MaxCompute explain 输出）

```
计划中应看到：
Stage 1: TableScan → MergeSort → Output
  - 扫描表: fact_article
  - 分区裁剪: 无（WHERE ds IS NOT NULL 无法裁剪）
  - 排序: ORDER BY 触发全量 MergeSort
```

### 瓶颈

1. **无分区裁剪** — 全部数据加载后再 WHERE
2. **全量排序** — ORDER BY 触发 MergeSort，扫描全表

### 优化

```sql
-- ✅ 利用 dim_date 做分区裁剪
SELECT f.article_id, f.title, f.source, f.tech_category, f.score
FROM fact_article f
JOIN dim_date d ON f.ds = d.date_key
WHERE d.month = extract(month from getdate())  -- 只查当月
ORDER BY f.article_created_at DESC
```

---

## 分析 2：窗口函数 ROW_NUMBER 全量计算

### SQL

参考 `int_article_enriched.sql` 中的 `ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC)`

### 执行计划预期

```
Stage 1: TableScan (stg_tech_news)
Stage 2: WindowOperator
  - PartitionBy: source
  - OrderBy: score DESC
  - 数据分布: 6 个 source 分区，每分区独立排序
Stage 3: Output (write to int_article_enriched table)
```

### 瓶颈

- `PARTITION BY source` 按 6 个来源分布，各分区数据量不均（HN ≈ 60%，其他 ≈ 40%）
- 这是数据倾斜——HN 分区需要处理更多数据

### 优化

```sql
-- 对倾斜的分区单独处理（实验性，面试展示用）
SELECT *, ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC) AS rn
FROM stg_tech_news
WHERE source = 'hackernews'  -- 大分区单独处理
UNION ALL
SELECT *, ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC) AS rn
FROM stg_tech_news
WHERE source != 'hackernews'  -- 小分区放一起
```

---

## 如何手动获取执行计划

```bash
docker exec techpulse-orchestrator bash -c "
    cd /home/src/techpulse_dbt
    echo 'explain select count(*) from fact_article;' | \
    dbt-mc run --select fact_article
"
```
