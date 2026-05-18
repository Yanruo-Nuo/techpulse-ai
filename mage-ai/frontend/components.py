"""可复用 UI 组件：新闻卡片、分页、图表、KPI"""

import streamlit as st
import pandas as pd
import plotly.express as px
from config import COLORS, CATEGORY_COLORS, ITEMS_PER_PAGE


def render_kpi_card(label: str, value: str, delta: str = None, color: str = COLORS["primary_light"]):
    """KPI 指标卡片"""
    st.markdown(f"""
    <div style="background:{COLORS['bg_card']};border:1px solid {COLORS['border']};
                border-radius:8px;padding:16px;margin-bottom:8px;
                border-left:4px solid {color};">
        <div style="color:{COLORS['text_secondary']};font-size:14px;">{label}</div>
        <div style="color:{COLORS['text_primary']};font-size:28px;font-weight:700;">{value}</div>
        {f'<div style="color:{color};font-size:14px;">{delta}</div>' if delta else ''}
    </div>
    """, unsafe_allow_html=True)


def render_trend_chart(df_trend: pd.DataFrame, height: int = 350):
    """趋势折线图"""
    if df_trend.empty:
        st.info("暂无趋势数据")
        return
    fig = px.line(
        df_trend, x="ds", y="daily_cnt", color="tech_category",
        title="每日技术热点统计",
        template="plotly_white",
        markers=True,
        color_discrete_map=CATEGORY_COLORS,
        labels={"daily_cnt": "热度指数", "ds": "日期", "tech_category": "技术分类"}
    )
    fig.update_layout(
        height=height, margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", y=-0.2),
        paper_bgcolor=COLORS["bg_main"], plot_bgcolor=COLORS["bg_card"],
        font=dict(color=COLORS["text_primary"])
    )
    st.plotly_chart(fig, use_container_width=True)


def render_pie_chart(df_trend: pd.DataFrame, height: int = 350):
    """分类占比环形图"""
    if df_trend.empty:
        st.info("暂无占比数据")
        return
    latest_date = df_trend['ds'].max()
    latest_df = df_trend[df_trend['ds'] == latest_date]
    if latest_df.empty:
        st.info("暂无占比数据")
        return
    fig = px.pie(
        latest_df, values='daily_cnt', names='tech_category',
        hole=0.4, title=f"最新占比（{latest_date.strftime('%Y-%m-%d')}）",
        template="plotly_white",
        color_discrete_map=CATEGORY_COLORS,
    )
    fig.update_layout(
        height=height, margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor=COLORS["bg_main"], plot_bgcolor=COLORS["bg_card"],
        font=dict(color=COLORS["text_primary"]),
        showlegend=True,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_news_card(article: pd.Series):
    """单条新闻卡片渲染（返回内容供操作按钮使用）"""
    cat = article.get('tech_category', 'Others')
    cat_color = CATEGORY_COLORS.get(cat, COLORS["text_muted"])
    score = int(article.get('score', 0) or 0)
    title = str(article.get('title', '无标题') or '无标题')
    insight = str(article.get('ai_insight', '暂无分析') or '暂无分析')
    ingest_time = article.get('ingest_time', '') or ''
    article_url = article.get('url', '') or ''
    source = str(article.get('source', '') or '')

    heat_pct = min(100, score)

    st.markdown(f"""
    <div class="news-card">
        <div style="display:flex;gap:12px;">
            <div class="cat-indicator" style="background:{cat_color};"></div>
            <div style="flex:1;">
                <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
                    <span class="tag" style="background:{cat_color}22;color:{cat_color};">{cat}</span>
                    <span class="tag" style="background:#E2E8F0;color:#475569;">{source}</span>
                    <span style="color:{COLORS['text_muted']};font-size:13px;">{ingest_time}</span>
                </div>
                <div style="font-size:16px;font-weight:600;margin:6px 0;color:{COLORS['text_primary']};">{title}</div>
                <div style="color:{COLORS['text_secondary']};font-size:14px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;">
                    {insight[:200]}
                </div>
                <div style="display:flex;align-items:center;gap:16px;margin-top:8px;">
                    <span style="color:{COLORS['text_muted']};font-size:13px;">🔥 {score}</span>
                    <div class="heat-bar" style="width:{heat_pct}%;background: {'#22C55E' if score > 70 else '#EAB308' if score > 30 else '#6B7280'};"></div>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    return title, insight, cat, article_url, score, source


def render_pagination(total_items: int, page_key: str = "news_page"):
    """分页控件"""
    total_pages = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    if total_pages <= 1:
        return 0

    if page_key not in st.session_state:
        st.session_state[page_key] = 0

    col_p, col_i, col_n = st.columns([1, 3, 1])
    with col_p:
        if st.session_state[page_key] > 0:
            if st.button("← 上一页", key=f"prev_{page_key}", use_container_width=True):
                st.session_state[page_key] -= 1
                st.rerun()
    with col_i:
        st.markdown(
            f"<p style='text-align:center;margin-top:6px;color:{COLORS['text_secondary']};'>"
            f"第 {st.session_state[page_key] + 1}/{total_pages} 页（共 {total_items} 条）</p>",
            unsafe_allow_html=True
        )
    with col_n:
        if st.session_state[page_key] < total_pages - 1:
            if st.button("下一页 →", key=f"next_{page_key}", use_container_width=True):
                st.session_state[page_key] += 1
                st.rerun()
    return st.session_state[page_key]


def render_empty_state(icon: str, title: str, description: str):
    """空状态占位"""
    st.markdown(f"""
    <div style="text-align:center;padding:60px 20px;color:{COLORS['text_secondary']};">
        <div style="font-size:48px;margin-bottom:16px;">{icon}</div>
        <div style="font-size:20px;font-weight:600;color:{COLORS['text_primary']};">{title}</div>
        <div style="font-size:14px;margin-top:8px;">{description}</div>
    </div>
    """, unsafe_allow_html=True)
