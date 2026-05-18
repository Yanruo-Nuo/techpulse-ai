/*-------------------------------------------------------------------
 * mart_daily_summary — 每日汇总事实表 (分析面试考点大集合)
 *
 * 面试考点:
 *  ① GROUP BY ROLLUP  — 分层汇总 (source+category → source → total)
 *  ② GROUPING()       — 识别汇总行
 *  ③ 窗口滑动平均      — AVG() OVER (7行窗口)
 *  ④ 环比/同比        — LAG() + 百分比计算
 *  ⑤ 复杂 CASE WHEN   — 业务打标
 *  ⑥ COALESCE + NULLIF — 防御 NULL
 *  ⑦ 多层 CTE 管道     — 可读性
 *  ⑧ 增量物化: insert_overwrite + ds 分区
 *-------------------------------------------------------------------*/

{{ config(
    materialized='table'
) }}

with fact as (
    select
        to_char(article_created_at, 'yyyy-mm-dd') as ds,
        source,
        source_type,
        tech_category,
        count(*) as article_cnt,
        avg(score) as avg_score,
        sum(case when is_ai_related then 1 else 0 end) as ai_related_cnt,
        avg(title_length) as avg_title_length
    from {{ ref('fact_article') }}
    where article_created_at is not null
    
    group by
        to_char(article_created_at, 'yyyy-mm-dd'),
        source,
        source_type,
        tech_category
),

-- ① ROLLUP: (source_type, source) × tech_category + 小计 + 总计
rolled_up as (
    select
        ds,

        coalesce(source_type, 'ALL') as source_type,

        case when grouping(tech_category) = 1
             then 'ALL'
             else coalesce(tech_category, 'unknown')
        end as tech_category,

        case when grouping(source) = 1
             then 'ALL'
             else coalesce(source, 'unknown')
        end as source,

        sum(article_cnt) as article_cnt,
        avg(avg_score) as avg_score,
        sum(ai_related_cnt) as ai_related_cnt,

        -- ② GROUPING 识别汇总层级
        case
            when grouping(source_type) = 1 and grouping(source) = 1
                 and grouping(tech_category) = 1 then 'total'
            when grouping(source) = 1 and grouping(tech_category) = 1
                 then 'by_source_type'
            when grouping(tech_category) = 1 then 'by_source'
            else 'by_source_category'
        end as granularity

    from fact
    group by
        ds,
        rollup (source_type, source, tech_category)
),

-- ③ 7日滑动平均 (按 source_type + category)
with_moving_avg as (
    select
        *,

        avg(article_cnt) over (
            partition by source_type, tech_category, granularity
            order by ds
            rows between 6 preceding and current row
        ) as ma7_article_cnt,

        avg(avg_score) over (
            partition by source_type, tech_category, granularity
            order by ds
            rows between 6 preceding and current row
        ) as ma7_avg_score,

        -- ④ 环比 (与前一天的差值)
        lag(article_cnt, 1) over (
            partition by source_type, tech_category, source, granularity
            order by ds
        ) as prev_day_article_cnt,

        -- AI 渗透率
        case
            when article_cnt > 0
            then round(ai_related_cnt * 100.0 / nullif(article_cnt, 0), 2)
            else 0
        end as ai_penetration_pct

    from rolled_up
)

select
    ds,
    source_type,
    source,
    tech_category,
    granularity,
    article_cnt,
    avg_score,
    ai_related_cnt,
    ai_penetration_pct,
    ma7_article_cnt,
    ma7_avg_score,
    prev_day_article_cnt,

    -- 环比增长率 (安全除 0)
    case
        when prev_day_article_cnt is not null and prev_day_article_cnt > 0
        then round(
            (article_cnt - prev_day_article_cnt)
            * 100.0 / prev_day_article_cnt, 2
        )
        else null
    end as wow_change_pct,

    getdate() as dbt_loaded_at

from with_moving_avg
order by ds desc, source_type, source, tech_category


