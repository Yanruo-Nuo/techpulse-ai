"""页面 3：AI 助手 — 多轮对话 RAG + Agentic 推理"""

import streamlit as st
import pandas as pd
import dashscope
from dashscope import TextEmbedding
from config import COLORS, get_css
from components import render_empty_state
from maxcompute import load_news_data, load_trend_data
from vector_store import VectorStore


# 全局单例 — 首次导入时自动连接 Qdrant
vector_store = VectorStore()


def agentic_rag(query, df, df_trend=None, max_steps=3):
    """多步推理 RAG: 搜 → 判 → 再搜 → 回答（注意：当前版本保留原 get_rag_response 作为默认路径，
    此函数用于未来切换到 agent 模式时的入口）"""
    from agent_tools import search_kb, get_trend_summary, get_entity_relations, format_search_results

    collected_results = []
    search_query = query
    for step in range(max_steps):
        results = search_kb(search_query, vector_store, top_k=3)
        collected_results.extend(results)
        if len(collected_results) >= 6 or step == max_steps - 1:
            break
        search_query = f"{query} 补充信息" if step == 0 else query

    context = format_search_results(collected_results[:8])
    entity_graph = get_entity_relations(query, df)
    trend_text = get_trend_summary(df_trend)

    prompt = f"""你是TechPulse技术专家。回答时引用文章标题支撑观点。

📊 知识库概况（共{len(df)}篇）
🔗 相关知识图谱：\n{entity_graph}
📈 趋势：{trend_text}
📌 相关资讯（{min(len(collected_results), 8)} 段）：\n{context}

用户问题：{query}

要求：1.先概括核心要点 2.引用文章标题支撑 3.指出实体关系 4.简短总结。用中文回答。"""

    import dashscope
    gen_resp = dashscope.Generation.call(
        model="glm-5.1",
        messages=[{"role": "user", "content": prompt}],
        result_format='message',
    )
    return gen_resp.output.choices[0]['message']['content'] if gen_resp.status_code == 200 else f"模型错误: {gen_resp.message}"


def get_rag_response(query, df, df_trend=None, messages=None):
    if df.empty:
        return "知识库暂无数据，请先同步数据。"

    # 1. 向量化用户问题
    q_emb_resp = TextEmbedding.call(
        model=TextEmbedding.Models.text_embedding_v2, input=query
    )
    if q_emb_resp.status_code != 200:
        return f"向量生成失败：{q_emb_resp.message}"
    q_embedding = q_emb_resp.output['embeddings'][0]['embedding']

    # 2. Qdrant 块级语义检索 — 返回匹配的具体段落（非整篇文章）
    top_results = vector_store.search_blocks(q_embedding, top_k=5)
    context = "\n\n".join(
        f"【{r['title']}】\n{r.get('block_preview', '')}\n（来源：{r['source']}，相关度：{r['score']:.2f}）"
        for r in top_results
    ) if top_results else "暂无相关文章"

    # 3. 分类分布
    cat_counts = df['tech_category'].value_counts()
    dist_text = "、".join(f"{k} {v}篇" for k, v in cat_counts.items())

    # 4. 热门排行
    hot_text = "（无热度数据）"
    if 'score' in df.columns:
        hot_df = df.dropna(subset=['score']).copy()
        hot_df['score'] = pd.to_numeric(hot_df['score'], errors='coerce').fillna(0)
        hot_df = hot_df.nlargest(10, 'score')
        hot_list = [f"  🔥 {r['title'][:60]} | 热度 {int(r['score'])} | {r['tech_category']}" for _, r in hot_df.iterrows()]
        hot_text = "\n".join(hot_list)

    # 5. 趋势概览
    trend_overview = ""
    if df_trend is not None and not df_trend.empty:
        latest_date = df_trend['ds'].max()
        latest = df_trend[df_trend['ds'] == latest_date]
        if not latest.empty:
            ranked = latest.sort_values('daily_cnt', ascending=False)
            ranking = " > ".join(f"{r['tech_category']}({int(r['daily_cnt'])}条)" for _, r in ranked.iterrows())
            trend_overview = f"📈 最新热榜（{latest_date.strftime('%Y-%m-%d')}）：{ranking}"

    # 5. 知识图谱实体关系（从 ai_triples 提取，最多 30 条）
    entity_graph_lines = []
    seen_triples = set()
    for _, r in df.iterrows():
        for t in r.get('ai_triples', []) or []:
            key = (t.get('subject',''), t.get('predicate',''), t.get('object',''))
            if key in seen_triples:
                continue
            seen_triples.add(key)
            entity_graph_lines.append(
                f"  {t['subject']} --{t['predicate']}--> {t['object']}"
            )
        if len(entity_graph_lines) >= 30:
            break
    entity_graph = "\n".join(entity_graph_lines) if entity_graph_lines else "（暂无实体关系数据）"

    # 6. 文章索引（标题 + 分类 + 热度，取热度 TOP 50 避免过长）
    article_lines = []
    idx_df = df.copy()
    if 'score' in idx_df.columns:
        idx_df['score'] = pd.to_numeric(idx_df['score'], errors='coerce').fillna(0)
        idx_df = idx_df.nlargest(50, 'score')
    for i, (_, r) in enumerate(idx_df.iterrows()):
        cat = r.get('tech_category', '')
        title = str(r.get('title', ''))[:60]
        article_lines.append(f"{i+1}. [{title}] | {cat}")
    article_index = "\n".join(article_lines) if article_lines else "（暂无）"

    # 10. 历史对话
    history_text = ""
    if messages and len(messages) > 1:
        history_parts = []
        for msg in messages[-6:-1]:
            history_parts.append(f"{'用户' if msg['role'] == 'user' else '助手'}：{msg['content']}")
        if history_parts:
            history_text = "历史对话：\n" + "\n".join(history_parts) + "\n\n"

    prompt = f"""你是TechPulse技术专家，回答用户关于技术资讯和趋势的问题。必须引用具体文章标题来支撑回答。

📊 知识库概况（共{len(df)}篇）：
分类分布：{dist_text}

🔗 知识图谱（实体间关系）：
{entity_graph}

{trend_overview}

🔥 最热门资讯 TOP 10：
{hot_text}

📚 全部资讯索引：
{article_index}

📌 与你问题最相关的资讯详情：
{context}

{history_text}用户问题：{query}

回答要求：
1. 引用具体文章标题来支撑观点，如「根据《xxx》这篇文章所述……」
2. 如果问到热门话题，指出具体哪些文章热度最高及其核心观点
3. 如果问到某方面资讯，列出相关文章标题和摘要
4. 如果问到趋势排名，引用趋势数据和分类分布
5. 用中文回答，简洁专业"""

    gen_resp = dashscope.Generation.call(
        model="glm-5.1",
        messages=[{"role": "user", "content": prompt}],
        result_format='message'
    )
    return gen_resp.output.choices[0]['message']['content'] if gen_resp.status_code == 200 else f"大模型错误：{gen_resp.message}"


def render_assistant():
    st.markdown(get_css(), unsafe_allow_html=True)

    st.markdown(f"""
    <div class="topbar">
        <div style="font-size:24px;font-weight:700;color:{COLORS['text_primary']};">🤖 AI 研究助手</div>
        <div style="display:flex;gap:8px;">
            <span style="color:{COLORS['text_muted']};font-size:13px;">多轮对话 · RAG 检索增强</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 初始化对话历史
    if "rag_messages" not in st.session_state:
        st.session_state.rag_messages = []
    if "assistant_context_loaded" not in st.session_state:
        st.session_state.assistant_context_loaded = False

    # 加载知识库数据
    with st.spinner("📡 加载知识库数据..."):
        df_news = load_news_data()
        df_trend = load_trend_data()

    # 知识库状态栏
    kb_size = len(df_news)
    st.markdown(f"""
    <div style="background:{COLORS['bg_card']};border:1px solid {COLORS['border']};
                border-radius:8px;padding:8px 16px;margin-bottom:16px;
                display:flex;justify-content:space-between;font-size:13px;color:{COLORS['text_muted']};">
        <span>📚 知识库：{kb_size} 篇文章</span>
        <span>🔄 自动检索增强生成</span>
    </div>
    """, unsafe_allow_html=True)

    # 快捷按钮
    quick_cols = st.columns(3)
    quick_prompts = {
        "🔥 今日热点": "今天技术领域有哪些最热门的话题？请列出具体文章。",
        "🔍 深度分析": "请分析当前技术趋势，哪些领域正在快速发展？引用具体文章。",
        "📊 趋势总览": "目前各技术分类的热度排名如何？哪些分类增长最快？",
    }
    for idx, (label, prompt) in enumerate(quick_prompts.items()):
        with quick_cols[idx]:
            if st.button(label, use_container_width=True, key=f"quick_{idx}"):
                st.session_state.rag_messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)
                with st.chat_message("assistant"):
                    with st.spinner("🤖 思考中..."):
                        ans = get_rag_response(prompt, df_news, df_trend, st.session_state.rag_messages)
                        st.markdown(ans)
                st.session_state.rag_messages.append({"role": "assistant", "content": ans})
                st.rerun()

    # 聊天历史
    for msg in st.session_state.rag_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # 聊天输入
    if query := st.chat_input("输入你想了解的技术问题..."):
        st.session_state.rag_messages.append({"role": "user", "content": query})
        with st.chat_message("user"):
            st.markdown(query)
        with st.chat_message("assistant"):
            with st.spinner("🤖 正在从知识库中检索相关内容并思考..."):
                try:
                    answer = get_rag_response(query, df_news, df_trend, st.session_state.rag_messages)
                    st.markdown(answer)
                except Exception as e:
                    answer = f"❌ 问答出错：{str(e)}"
                    st.error(answer)
        st.session_state.rag_messages.append({"role": "assistant", "content": answer})
        st.rerun()

    # 清空对话
    if st.session_state.rag_messages:
        if st.button("🗑️ 清空对话", use_container_width=False):
            st.session_state.rag_messages = []
            st.rerun()
