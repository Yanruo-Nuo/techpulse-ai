/*-------------------------------------------------------------------
 * int_article_enriched — 新闻中间层 (SQL 面试考点合集)
 *
 * 涵盖面试高频考点:
 *  ① ROW_NUMBER()   — 按 source 去重取最新
 *  ② RANK()         — 按 score 排名
 *  ③ LAG() / LEAD() — 前后文章分差
 *  ④ FIRST_VALUE()  — 每个 source 最高分文章
 *  ⑤ COUNT(*) OVER  — 窗口累计计数
 *  ⑥ AVG() OVER     — 滑动平均
 *  ⑦ 多层 CTE
 *  ⑧ NULLIF / COALESCE 防御性处理
 *-------------------------------------------------------------------*/

with base as (
    select
        id,
        url,
        source,
        title,
        author,
        score,
        ai_summary,
        ai_insight,
        tech_category,
        ds,
        created_at
    from {{ ref('stg_tech_news') }}
    where id is not null
),

ranked as (
    select
        *,

        -- ① 每个来源内按分数降序编号（去重/取Top用）
        row_number() over (
            partition by source
            order by score desc nulls last
        ) as rn_score_desc,

        -- ② 全局排名（同分并列，跳跃序号）
        rank() over (
            order by score desc nulls last
        ) as global_rank,

        -- ③ 该来源上一篇文章的分差
        lag(score, 1) over (
            partition by source
            order by created_at asc
        ) as prev_article_score,

        -- ④ 该来源下一篇文章的分差（对比LEAD）
        lead(score, 1) over (
            partition by source
            order by created_at asc
        ) as next_article_score,

        -- ⑤ 每个来源最高分文章的标题
        first_value(title) over (
            partition by source
            order by score desc nulls last
        ) as top_article_title,

        -- ⑥ 每个来源累计文章数
        count(*) over (
            partition by source
        ) as source_article_cnt,

        -- ⑦ 来源内创建时间顺序排名（看采集延时）
        row_number() over (
            partition by source
            order by created_at asc
        ) as rn_chronological

    from base
),

with_metrics as (
    select
        *,

        -- 分差（安全处理 NULL）
        coalesce(score - prev_article_score, 0) as score_diff_from_prev,
        coalesce(score - next_article_score, 0) as score_diff_from_next,

        -- 标题长度特征
        length(trim(title)) as title_length,

        -- score 分段
        case
            when score >= 100 then 'high'
            when score >= 30  then 'medium'
            when score >= 1   then 'low'
            else 'unscored'
        end as score_tier,

        -- 标题是否含 AI 相关关键词（MaxCompute 不支持 ilike，用 upper 替代）
        case
            when upper(title) like '%AI%' or upper(title) like '%GPT%'
                 or upper(title) like '%LLM%' or upper(title) like '%MACHINE LEARNING%'
                 or upper(title) like '%DEEP LEARNING%' or upper(title) like '%NEURAL%'
            then true
            else false
        end as is_ai_related

    from ranked
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
    tech_category,
    ds,
    created_at,
    rn_score_desc,
    global_rank,
    prev_article_score,
    next_article_score,
    score_diff_from_prev,
    score_diff_from_next,
    top_article_title,
    source_article_cnt,
    rn_chronological,
    title_length,
    score_tier,
    is_ai_related
from with_metrics
