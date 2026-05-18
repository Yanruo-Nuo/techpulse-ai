-- score 必须 >= 0
select article_id, score
from {{ ref('fact_article') }}
where score < 0
