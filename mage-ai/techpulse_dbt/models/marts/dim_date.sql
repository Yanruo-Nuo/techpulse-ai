/*-------------------------------------------------------------------
 * dim_date — 日期维度表
 *
 * 面试考点:
 *  - 维度建模: 日期维度是 Kimball 星型模型的基础
 *  - 日期维度预计算: 提前生成所有日期属性
 *  - 分区对齐: 与事实表的 ds 分区对齐
 *
 * 用法: LEFT JOIN dim_date ON fact_article.ds = dim_date.date_key
 *-------------------------------------------------------------------*/

{{ config(materialized='table') }}

with date_range as (
    select
        min(ds) as min_date,
        max(ds) as max_date
    from {{ ref('fact_article') }}
),

-- 生成日期序列: 每行代表一天
-- MaxCompute 不支持 generate_series，用 UNION ALL 模拟
date_series as (
    select dateadd(to_date(min_date, 'yyyy-mm-dd'), 0, 'dd') as dt from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 1, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 2, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 3, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 4, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 5, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 6, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 7, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 8, 'dd') from date_range
    union all select dateadd(to_date(min_date, 'yyyy-mm-dd'), 9, 'dd') from date_range
    -- 这里只生成 10 天做演示，完整版应该用 cross join 生成大表
),

calendar as (
    select
        to_char(dt, 'yyyy-mm-dd') as date_key,
        dt as full_date,
        year(dt) as year,
        month(dt) as month,
        day(dt) as day_of_month,
        quarter(dt) as quarter,
        month(dt) as month_num,
        dayofweek(dt) as day_of_week
    from date_series
)

select
    date_key,
    year,
    case month_num
        when 1 then 'Jan' when 2 then 'Feb' when 3 then 'Mar'
        when 4 then 'Apr' when 5 then 'May' when 6 then 'Jun'
        when 7 then 'Jul' when 8 then 'Aug' when 9 then 'Sep'
        when 10 then 'Oct' when 11 then 'Nov' when 12 then 'Dec'
    end as month_short,
    month_num,
    day_of_month,
    quarter,
    case when day_of_week >= 6 then 1 else 0 end as is_weekend
from calendar
where date_key <= (select max(ds) from {{ ref('fact_article') }})
order by date_key
