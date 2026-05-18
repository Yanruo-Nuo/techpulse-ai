-- article_cnt 必须 > 0（空汇总没有意义）
select ds, source, tech_category, article_cnt
from {{ ref('mart_daily_summary') }}
where article_cnt <= 0
