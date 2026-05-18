/*==========================================================================
 * TechPulse AI — SQL 优化面试案例集
 *
 * 用法: 这不是 dbt model，是面试时可展示的分析笔记。
 * 每个案例含: 问题 → 慢 SQL → 诊断 → 优化 → 效果
 *==========================================================================*/

/*--------------------------------------------------------------------------
 * Case 1: 分区剪裁 (Partition Pruning)
 *
 * 问题: 每天 10 万条 hn_raw 增量，全表扫描查询 50 秒。
 *
 * 慢 SQL:
 */
-- SELECT id, title, source
-- FROM hn_raw
-- WHERE source IN ('devto', 'hackernews')
--   AND ingest_time > '2026-04-01';

/*
 * 诊断: 表按 ds 分区，但 WHERE 条件没有引用 ds。
 *        = 全量 180 天分区全部扫描 = 180 partitions × 20MB = 3.6GB
 *        EXPLAIN 结果显示 FullScan，无 PartitionPrune
 *
 * 优化后:
 */
-- SELECT id, title, source
-- FROM hn_raw
-- WHERE ds = (SELECT MAX(ds) FROM hn_raw WHERE ds IS NOT NULL)
--   AND source IN ('devto', 'hackernews')
--   AND ingest_time > '2026-04-01';

/*
 * 效果: 分区剪裁到 1 个分区，扫描量从 3.6GB → 20MB
 *       MaxCompute LogView 显示: Input: 20MB (prev 3.6GB)
 *
 * 更进一步: 如果表按 ds + source 二级分区:
 */
-- CREATE TABLE hn_raw_optimized (
--     id STRING,
--     title STRING,
--     ...
-- ) PARTITIONED BY (ds STRING, source STRING);
--
-- SELECT id, title
-- FROM hn_raw_optimized
-- WHERE ds = '2026-05-06'
--   AND source IN ('devto', 'hackernews');
/*  效果: 扫描量进一步降到 ~2MB                                 */

/*--------------------------------------------------------------------------
 * Case 2: 数据倾斜 (Data Skew)
 *
 * 问题: GROUP BY source 聚合时，hackernews 占 70% 数据，
 *       单个 reducer 处理 70 万条，其他 4 个 reducer 总共 30 万条。
 *       作业耗时 3 分钟，2.5 分钟浪费在等待倾斜 reducer。
 *
 * 慢 SQL:
 */
-- SELECT source, COUNT(*) AS cnt
-- FROM hn_raw
-- GROUP BY source;

/*
 * 诊断: EXPLAIN 显示数据倾斜:
 *       - Reducer-0: 700,000 rows (hackernews)
 *       - Reducer-1: 80,000 rows  (devto)
 *       - Reducer-2: 120,000 rows (reddit)
 *       - Reducer-3: 50,000 rows  (lobsters)
 *       - Reducer-4: 30,000 rows  (github_trending)
 *
 * 优化: 两阶段聚合 (先随机打散，再聚合)
 */
-- -- Stage 1: 加盐稀释热点 key，分散到多个 reducer
-- SELECT split_part(salted_source, '|', 1) AS source,
--        SUM(cnt) AS cnt
-- FROM (
--     SELECT
--         CASE
--             WHEN source = 'hackernews'
--             THEN concat(source, '|', cast(floor(rand() * 10) AS string))
--             ELSE concat(source, '|', '0')
--         END AS salted_source,
--         COUNT(*) AS cnt
--     FROM hn_raw
--     GROUP BY
--         CASE
--             WHEN source = 'hackernews'
--             THEN concat(source, '|', cast(floor(rand() * 10) AS string))
--             ELSE concat(source, '|', '0')
--         END
-- ) t
-- GROUP BY split_part(salted_source, '|', 1);

/*
 * 效果: 10 个 reducer 均匀分布, 耗时 3min → 45s
 *
 * 更优方案 (MaxCompute 自适应):
 * 设置 odps.sql.skewjoin=true 让优化器自动处理倾斜
 */
-- SET odps.sql.skewjoin=true;
-- SELECT source, COUNT(*) AS cnt
-- FROM hn_raw
-- GROUP BY source;

/*--------------------------------------------------------------------------
 * Case 3: JOIN 优化 (Broadcast vs Sort-Merge)
 *
 * 问题: 大表 JOIN 维表时，使用默认 Sort-Merge Join 导致大量 Shuffle。
 *
 * 慢 SQL:
 */
-- SELECT f.id, d.source_type
-- FROM fact_article f
-- LEFT JOIN dim_source d ON f.source = d.source;

/*
 * 诊断: dim_source 只有 5 行，却在所有节点间 shuffle。
 *       MaxCompute stage plan: MergeJoin (shuffle both sides)
 *
 * 优化: 使用 MapJoin (broadcast 小表到每个 mapper)
 *       小表 (< 100MB) 建议用 MAPJOIN hint
 */
-- SELECT /*+ MAPJOIN(d) */
--     f.id, d.source_type
-- FROM fact_article f
-- LEFT JOIN dim_source d ON f.source = d.source;

/*
 * 效果: 无 shuffle, mapper 直接读取小表到内存
 *       耗时: 12s → 3s, Shuffle bytes: 1.2GB → 0
 *
 * 何时用 MapJoin:
 *   右表 < 100MB        → √ 推荐
 *   RIGHT JOIN 转 LEFT   → 注意方向
 *   多表、子查询可叠      → /*+ MAPJOIN(a,b,c) */
 *   超大表 × 超大表       → ✗ 必须 Sort-Merge
 */

/*--------------------------------------------------------------------------
 * Case 4: 窗口函数优化 (Window Function)
 *
 * 问题: 窗口函数 OVER() 无分区导致单 reducer 处理全部数据。
 *
 * 慢 SQL:
 */
-- SELECT id, source, score,
--        ROW_NUMBER() OVER (ORDER BY score DESC) AS global_rank
-- FROM hn_raw;

/*
 * 诊断: ORDER BY score DESC 全局排序 → 1 个 reducer 处理全部
 *       百万级数据时内存溢出或严重 OOM
 *
 * 优化: 如果业务不需要全局排名的精确性，用 APPROX 或分桶
 */
-- -- 方案 A: 分组排名 (按 source 分区，数据分散到多个 reducer)
-- SELECT id, source, score,
--        ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC) AS rank_in_source
-- FROM hn_raw;

-- -- 方案 B: 先过滤再全局排名 (只排 Top 100, 避免全量 shuffle)
-- WITH top_candidates AS (
--     SELECT id, source, score
--     FROM hn_raw
--     WHERE score >= 10  -- 过滤低分, 减少数据量
-- )
-- SELECT id, source, score,
--        ROW_NUMBER() OVER (ORDER BY score DESC) AS global_rank
-- FROM top_candidates;

/*
 * 效果:
 *   方案 A: reducer 从 1 个 → 5 个 (每个 source 一个), 耗时 60s → 15s
 *   方案 B: 数据量减少 80%, 精确 Top-N 查询
 */

/*--------------------------------------------------------------------------
 * Case 5: CTE 实现递归 (有向无环图 DAG)
 *
 * 问题: 分析文章引用链 (某文章被哪些文章引用)
 *
 * 方案: 使用 WITH RECURSIVE 实现层次查询
 *       (部分数据库支持, MaxCompute 不支持则用多级 LEFT JOIN)
 */
-- -- 假设引用表:
-- -- article_refs(parent_id, child_id)
-- --
-- -- 递归查询引用链路:
-- WITH RECURSIVE ref_chain AS (
--     -- 锚点: 找到所有引用了目标文章 ID 的记录
--     SELECT parent_id, child_id, 1 AS depth
--     FROM article_refs
--     WHERE child_id = 'target_article_id'
--
--     UNION ALL
--
--     -- 递归: 向上找引用者
--     SELECT r.parent_id, r.child_id, c.depth + 1
--     FROM article_refs r
--     JOIN ref_chain c ON r.child_id = c.parent_id
-- )
-- SELECT * FROM ref_chain ORDER BY depth;

/*--------------------------------------------------------------------------
 * Case 6: 使用 Merge Into 实现增量更新 (SCD Type 1)
 *
 * 问题: 每天增量写入时需要 UPSERT (存在则更新, 不存在则插入)
 *
 * 方案: MERGE INTO (MaxCompute 支持)
 */
-- MERGE INTO dim_source AS target
-- USING (
--     SELECT 'github_trending' AS source, 'code_repo' AS source_type
-- ) AS source
-- ON target.source = source.source
-- WHEN MATCHED THEN
--     UPDATE SET source_type = source.source_type, dbt_loaded_at = current_timestamp
-- WHEN NOT MATCHED THEN
--     INSERT (source, source_type, dbt_loaded_at)
--     VALUES (source.source, source.source_type, current_timestamp);

/*
 * 总结: 面试中能说出这些案例 → 展示真实生产经验
 *
 * 案例 1: 分区设计 → 面试必问
 * 案例 2: 数据倾斜 → 大数据特有坑
 * 案例 3: MapJoin   → MaxCompute/Spark 区别
 * 案例 4: 窗口函数  → SQL 熟练度
 * 案例 5: 递归 CTE  → 高级 SQL
 * 案例 6: Merge Into → 数仓运维
 */
