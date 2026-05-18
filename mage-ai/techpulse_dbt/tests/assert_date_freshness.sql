-- article_created_at 不能早于 2024-01-01（数据时效性）
select article_id, article_created_at
from {{ ref('fact_article') }}
where article_created_at < to_date('2024-01-01', 'yyyy-mm-dd')
