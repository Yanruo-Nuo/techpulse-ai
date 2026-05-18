-- 标题不为空字符串
select title
from {{ ref('int_article_enriched') }}
where title = '' or title is null
