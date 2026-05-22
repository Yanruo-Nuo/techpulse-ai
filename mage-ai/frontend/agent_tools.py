"""Agent 可调用的工具集 — 知识库搜索、趋势查询、图谱检索

所有搜索均走 Qdrant 向量检索（O(log n)），不遍历 DataFrame。
"""

from typing import Any

from dashscope import TextEmbedding


def _embed_query(query: str) -> list[float]:
    """嵌入用户查询"""
    resp = TextEmbedding.call(
        model=TextEmbedding.Models.text_embedding_v2, input=query
    )
    if resp.status_code != 200:
        return []
    return resp.output['embeddings'][0]['embedding']


def search_kb(vector_store: Any, query: str, top_k: int = 5) -> list[dict]:
    """工具 1: 在知识库中语义搜索相关段落（Qdrant，O(log n)）

    Args:
        vector_store: VectorStore 实例
        query: 搜索关键词或自然语言问题
        top_k: 返回结果数

    Returns:
        [{"title", "block_preview", "source", "score"}, ...]
    """
    q_emb = _embed_query(query)
    if not q_emb:
        return []
    return vector_store.search_blocks(q_emb, top_k=top_k)


def get_trend_summary(df_trend: Any) -> str:
    """工具 2: 获取技术趋势概览

    Args:
        df_trend: 趋势 DataFrame（来自 load_trend_data）

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


def search_entity_relations(
    vector_store: Any,
    query: str,
    top_k: int = 5,
) -> str:
    """工具 3: 语义检索实体关系（Qdrant tech_entities，O(log n)）

    替代原 df.iterrows() 遍历全量数据，Qdrant HNSW 索引保证检索时间不随数据量增长。

    Args:
        vector_store: VectorStore 实例
        query: 用户查询
        top_k: 返回关系数

    Returns:
        关系文本，如:
          Rust --对比--> Go（来源：article1, article2）
    """
    q_emb = _embed_query(query)
    if not q_emb:
        return "（无相关实体关系）"

    hits = vector_store.search_triples(q_emb, top_k=top_k)
    if not hits:
        return "（无相关实体关系）"

    # 按 subject+object 去重（同一实体对可能来自不同文章）
    seen: set[tuple[str, str, str]] = set()
    lines: list[str] = []
    for h in hits:
        key = (h["subject"], h["predicate"], h["object"])
        if key in seen:
            continue
        seen.add(key)
        lines.append(
            f"  {h['subject']} --{h['predicate']}--> {h['object']} "
            f"（来源：{h['source']}，相关度：{h['score']:.2f}）"
        )

    return "\n".join(lines)


def format_search_results(results: list[dict]) -> str:
    """格式化搜索结果为文本"""
    if not results:
        return "（未找到相关内容）"
    return "\n\n".join(
        f"【{r['title']}】\n{r.get('block_preview', '')}"
        f"\n（来源：{r['source']}，相关度：{r['score']:.2f}）"
        for r in results
    )
