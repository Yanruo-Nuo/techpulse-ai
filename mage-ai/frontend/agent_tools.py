"""Agent 可调用的工具集 — 知识库搜索、趋势查询、图谱检索

供 assistant.py 的 agentic_rag 函数调用，实现多步推理。
"""

from typing import Any

from dashscope import TextEmbedding


def search_kb(query: str, vector_store: Any, top_k: int = 5) -> list[dict[str, Any]]:
    """工具 1: 在知识库中语义搜索相关段落

    Args:
        query: 搜索关键词或自然语言问题
        vector_store: VectorStore 实例
        top_k: 返回结果数

    Returns:
        [{"title": ..., "block_preview": ..., "source": ..., "score": ...}, ...]
    """
    q_emb_resp = TextEmbedding.call(
        model=TextEmbedding.Models.text_embedding_v2, input=query
    )
    if q_emb_resp.status_code != 200:
        return []
    q_embedding = q_emb_resp.output['embeddings'][0]['embedding']
    return vector_store.search_blocks(q_embedding, top_k=top_k)


def get_trend_summary(df_trend: Any) -> str:
    """工具 2: 获取技术趋势概览

    Args:
        df_trend: 趋势 DataFrame (来自 load_trend_data)

    Returns:
        趋势文本描述
    """
    if df_trend is None or df_trend.empty:
        return "（无趋势数据）"

    latest_date = df_trend['ds'].max()
    latest = df_trend[df_trend['ds'] == latest_date]
    if latest.empty:
        return "（无最新趋势数据）"
    ranked = latest.sort_values('daily_cnt', ascending=False)
    return " > ".join(
        f"{r['tech_category']}({int(r['daily_cnt'])}条)"
        for _, r in ranked.iterrows()
    )


def get_entity_relations(
    query: str,
    df_news: Any,
    max_relations: int = 15,
) -> str:
    """工具 3: 获取与查询相关的实体关系

    Args:
        query: 用户查询
        df_news: 文章 DataFrame
        max_relations: 最大返回关系数

    Returns:
        关系文本
    """
    keywords = set(query.lower().split())
    relations: list[str] = []
    seen: set[tuple[str, str, str]] = set()

    for _, r in df_news.iterrows():
        for t in r.get('ai_triples', []) or []:
            subj = (t.get('subject', '')).lower()
            obj = (t.get('object', '')).lower()
            # 检查是否与查询相关
            if any(kw in subj or kw in obj for kw in keywords):
                key = (t.get('subject', ''), t.get('predicate', ''), t.get('object', ''))
                if key not in seen:
                    seen.add(key)
                    relations.append(
                        f"  {t['subject']} --{t['predicate']}--> {t['object']}"
                    )
            if len(relations) >= max_relations:
                break
        if len(relations) >= max_relations:
            break
    return "\n".join(relations) if relations else "（无相关实体关系）"


def format_search_results(results: list[dict[str, Any]]) -> str:
    """格式化搜索结果为文本"""
    if not results:
        return "（未找到相关内容）"
    return "\n\n".join(
        f"【{r['title']}】\n{r.get('block_preview', '')}"
        f"\n（来源：{r['source']}，相关度：{r['score']:.2f}）"
        for r in results
    )
