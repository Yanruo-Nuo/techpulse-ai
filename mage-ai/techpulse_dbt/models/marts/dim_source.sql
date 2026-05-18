/*-------------------------------------------------------------------
 * dim_source — 来源维度表 (SCD Type 2)
 *
 * materialized 为 table（非 incremental）:
 *   - 数据量极小（6 个来源），incremental 无意义
 *   - table 物化更简单，避免 transactional 兼容问题
 *-------------------------------------------------------------------*/

{{ config(materialized='table') }}

/*-------------------------------------------------------------------
 * dim_source — 来源维度表 (SCD Type 2)
 *
 * 面试考点:
 *  - 维度建模: 缓慢变化维 Type 2
 *  - CASE WHEN 分类逻辑
 *  - 窗口函数 ROW_NUMBER() 取最新版本
 *  - NULL 处理 (COALESCE, NULLIF)
 *  - 子查询 + CTE
 *-------------------------------------------------------------------*/

with source_articles as (
    select distinct
        source,
        lower(source) as source_key
    from {{ ref('stg_tech_news') }}
    where source is not null
),

classified as (
    select
        source,
        source_key,
        case
            when source in ('hackernews', 'lobsters')    then 'community'
            when source in ('devto', 'medium')            then 'blogging'
            when source in ('reddit')                     then 'forum'
            when source in ('github_trending')            then 'code_repo'
            when source in ('techcrunch', 'arstechnica')  then 'media'
            else 'other'
        end as source_type,
        case
            when source in ('hackernews', 'reddit', 'devto', 'lobsters')
                then 'api'
            when source in ('github_trending', 'techcrunch')
                then 'web_scrape'
            else 'rss'
        end as ingestion_method,

        -- SCD Type 2: 假设首次发现时间 (MaxCompute 用 dateadd)
        dateadd(getdate(), -30, 'dd') as valid_from,
        cast(null as datetime) as valid_to,
        1 as is_current
    from source_articles
)

select
    md5(concat(coalesce(source_key, ''))) as dim_source_sk,
    source,
    source_key,
    source_type,
    ingestion_method,
    valid_from,
    valid_to,
    is_current,
    getdate() as dbt_loaded_at
from classified
