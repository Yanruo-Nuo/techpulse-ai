-- 同一 ds 分区内 article_id 唯一
select ds, article_id, count(*) as cnt
from {{ ref('fact_article') }}
group by ds, article_id
having count(*) > 1
