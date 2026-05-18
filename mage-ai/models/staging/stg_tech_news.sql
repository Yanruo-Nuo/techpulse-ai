

with raw as (
    select * from techpulse_dw.dim_tech_news_analysis
)
select
    id,
    title,
    author,
    ai_insight,
    case
        when ai_insight like '%AI%' or ai_insight like '%大模型%' then 'AI/ML'
        when ai_insight like '%架构%' or ai_insight like '%云%'   then 'CloudNative'
        when ai_insight like '%编程%' or ai_insight like '%Python%' then 'Programming'
        when ai_insight like '%安全%' or ai_insight like '%漏洞%'  then 'Security'
        else 'Others'
    end as tech_category,
    from_unixtime(ingest_time) as created_at
from raw
where title is not null
