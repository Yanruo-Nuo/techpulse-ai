/*-------------------------------------------------------------------
 * fact_article — 新闻事实表
 *
 * 面试考点:
 *  - 事实表设计: 可加性度量 (additive measures)
 *  - 代理键关联维度表 (surrogate key)
 *  - LEFT JOIN 保留事实
 *  - CASE WHEN 度量归类
 *  - 类型转换安全处理
 *  - 数据质量检查行
 *  - 增量物化: insert_overwrite + ds 分区
 *-------------------------------------------------------------------*/

{{ config(
    materialized='table'
) }}

with enriched as (
    select * from {{ ref('int_article_enriched') }}
),

source_dim as (
    select * from {{ ref('dim_source') }}
    where is_current = 1
),

joined as (
    select
        md5(concat(coalesce(cast(enriched.id as string), ''), '|', coalesce(enriched.source, '')))
            as fact_article_sk,

        enriched.id as article_id,
        source_dim.dim_source_sk,
        source_dim.source,
        source_dim.source_type,
        source_dim.ingestion_method,

        enriched.title,
        enriched.url,
        enriched.author,
        enriched.ai_summary,
        enriched.ai_insight,
        enriched.tech_category,
        enriched.ds,

        -- 数字度量 (可加)
        coalesce(cast(enriched.score as bigint), 0) as score,
        cast(enriched.title_length as int) as title_length,

        -- 窗口排名 (退化维度, 直接存在事实表减少 JOIN)
        enriched.rn_score_desc,
        enriched.global_rank,
        enriched.source_article_cnt,

        -- 分数段
        enriched.score_tier,
        enriched.is_ai_related,

        -- 时间
        enriched.created_at as article_created_at,
        getdate() as dbt_loaded_at
    from enriched
    left join source_dim
        on lower(enriched.source) = source_dim.source_key
)

select * from joined
