with raw as (
    select * from {{ source('raw', 'hn_raw') }}
    where ds is not null
    -- 分区剪裁: 最多扫描近 30 天，避免全表扫描
    and ds >= to_char(dateadd(getdate(), -30, 'dd'), 'yyyy-mm-dd')
)
select
    id,
    url,
    source,
    title,
    author,
    score,
    ai_summary,
    ai_insight,
    ds,
    ingest_time,
    case
        when ai_insight like '%AI%' or ai_insight like '%大模型%' then 'AI/ML'
        when ai_insight like '%架构%' or ai_insight like '%云%'   then 'CloudNative'
        when ai_insight like '%编程%' or ai_insight like '%Python%' then 'Programming'
        when ai_insight like '%安全%' or ai_insight like '%漏洞%'   then 'Security'
        else 'Others'
    end as tech_category,
    from_unixtime(cast(ingest_time as bigint)) as created_at
from raw
where title is not null
