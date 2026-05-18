

with staged as (
    select * from 
),
daily_counts as (
    select
        to_char(created_at, 'yyyy-mm-dd') as ds,
        tech_category,
        count(*) as daily_cnt
    from staged
    group by to_char(created_at, 'yyyy-mm-dd'), tech_category
)
select
    t1.ds,
    t1.tech_category,
    t1.daily_cnt,
    round(
        (t1.daily_cnt - t2.daily_cnt) / nullif(t2.daily_cnt, 0) * 100,
        2
    ) as trend_score_pct
from daily_counts t1
left join daily_counts t2
    on  t1.tech_category = t2.tech_category
    and t1.ds > t2.ds
