"""页面 1：时间线 — 热点资讯流 + 看板图表 + 分类过滤"""

import streamlit as st
import pandas as pd
from config import COLORS, CATEGORY_ORDER, ITEMS_PER_PAGE, get_css
from components import (
    render_trend_chart, render_pie_chart, render_news_card,
    render_pagination, render_kpi_card
)
from maxcompute import load_trend_data, load_news_data
from db import is_bookmarked


def render_timeline():
    st.markdown(get_css(), unsafe_allow_html=True)

    # 顶栏
    st.markdown(f"""
    <div class="topbar">
        <div style="font-size:24px;font-weight:700;color:{COLORS['text_primary']};">
            📰 技术热点时间线
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 加载数据
    df_trend = load_trend_data()
    df_news = load_news_data()

    # ====== KPI 指标行 ======
    if not df_news.empty:
        # ── 采集概览 ──
        total_articles = len(df_news)
        sources = df_news['source'].nunique() if 'source' in df_news.columns else 0
        categories = df_news['tech_category'].nunique()
        avg_score = int(df_news['score'].astype(float).mean() or 0) if 'score' in df_news.columns else 0

        # ── 分类多维度 ──
        top_cat = df_news['tech_category'].value_counts().idxmax() if not df_news.empty else "N/A"
        top_cat_cnt = df_news['tech_category'].value_counts().max() if not df_news.empty else 0

        # ── 新鲜度 ──
        latest_ingest = df_news['ingest_time'].max() if 'ingest_time' in df_news.columns else None
        if latest_ingest is not None:
            from datetime import datetime
            now = datetime.utcnow()
            # ingest_time 可能是字符串或 pd.Timestamp，统一用 pandas 解析
            ingest_dt = pd.to_datetime(latest_ingest)
            freshness_min = int((now - ingest_dt).total_seconds() / 60)
            freshness_str = f"{freshness_min} 分钟前" if freshness_min < 60 else f"{freshness_min // 60}h{freshness_min % 60}m"
        else:
            freshness_str = "未知"

        # ── 热门来源（新增）──
        top_source = df_news['source'].value_counts().idxmax() if sources > 0 else "N/A"

        kcols = st.columns(4)
        with kcols[0]:
            render_kpi_card("📊 累计文章", f"{total_articles}", delta=f"{sources} 个数据源 · {categories} 个分类", color=COLORS["chart_blue"])
        with kcols[1]:
            render_kpi_card("🏆 热门分类", f"{top_cat}", delta=f"{top_cat_cnt} 篇 · 占比 {top_cat_cnt/total_articles*100:.0f}%", color=COLORS["chart_purple"])
        with kcols[2]:
            render_kpi_card("🔥 热门来源", f"{top_source}", delta=f"共 {sources} 个活跃数据源", color=COLORS["chart_amber"])
        with kcols[3]:
            render_kpi_card("⏱ 数据新鲜度", f"{freshness_str}", delta=f"平均热度 {avg_score}", color=COLORS["chart_green"])
        st.markdown("<hr style='border-color:" + COLORS["border"] + "'>", unsafe_allow_html=True)

    # ====== 图表行 ======
    chart_col1, chart_col2 = st.columns([2, 1])
    with chart_col1:
        st.subheader("热度趋势")
        render_trend_chart(df_trend)
    with chart_col2:
        st.subheader("分类占比")
        render_pie_chart(df_trend)

    st.markdown("<hr style='border-color:" + COLORS["border"] + "'>", unsafe_allow_html=True)

    # ====== 过滤栏 ======
    filter_col1, filter_col2 = st.columns([2, 1])
    with filter_col1:
        selected_categories = st.multiselect(
            "按分类过滤",
            options=["全部"] + CATEGORY_ORDER,
            default=["全部"],
            key="timeline_cat_filter"
        )
    with filter_col2:
        sort_by = st.selectbox(
            "排序方式",
            ["最新发布", "热度最高"],
            key="timeline_sort"
        )

    # 来源过滤
    if 'source' not in df_news.columns:
        df_news['source'] = 'hackernews'  # 向后兼容旧数据
    all_sources = df_news['source'].unique().tolist() if not df_news.empty else []
    source_filter_col1, source_filter_col2 = st.columns([2, 1])
    with source_filter_col1:
        selected_sources = st.multiselect(
            "按来源过滤",
            options=["全部"] + all_sources,
            default=["全部"],
            key="timeline_source_filter"
        )

    # 应用过滤
    filtered_df = df_news.copy()
    if "全部" not in selected_categories and selected_categories:
        filtered_df = filtered_df[filtered_df['tech_category'].isin(selected_categories)]

    if "全部" not in selected_sources and selected_sources:
        filtered_df = filtered_df[filtered_df['source'].isin(selected_sources)]

    if sort_by == "热度最高" and 'score' in filtered_df.columns:
        filtered_df = filtered_df.copy()
        filtered_df['score'] = pd.to_numeric(filtered_df['score'], errors='coerce').fillna(0)
        filtered_df = filtered_df.sort_values('score', ascending=False)

    # ====== 资讯列表 ======
    if filtered_df.empty:
        st.subheader("资讯列表（共 0 条）")
        st.info("当前筛选条件下没有资讯")
        return

    page = render_pagination(len(filtered_df), "timeline_page")
    start = page * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    start_display = start + 1
    end_display = min(end, len(filtered_df))
    st.subheader(f"资讯列表（第 {start_display}-{end_display} 条，共 {len(filtered_df)} 条）")

    for i, (_, row) in enumerate(filtered_df.iloc[start:end].iterrows()):
        title, insight, cat, url, score, source = render_news_card(row)
        bookmarked = is_bookmarked(title)

        # 操作按钮行
        action_cols = st.columns([1, 1, 1, 3])
        with action_cols[0]:
            if url and url.startswith('http'):
                st.markdown(f'<a href="{url}" target="_blank" style="font-size:14px;">🔗 原文链接</a>', unsafe_allow_html=True)
        if not bookmarked:
            with action_cols[1]:
                if st.button("⭐ 收藏", key=f"bookmark_{i}_{page}"):
                    from db import add_bookmark, resolve_collection_id
                    col_id = resolve_collection_id(cat)
                    add_bookmark(col_id, {
                        "title": title,
                        "url": url,
                        "tech_category": cat,
                        "ai_summary": "",
                        "ai_insight": insight,
                        "score": score,
                    })
                    st.rerun()
        else:
            with action_cols[1]:
                st.markdown(f"<span style='color:{COLORS['accent']};font-size:14px;'>⭐ 已收藏</span>", unsafe_allow_html=True)
            with action_cols[2]:
                if st.button("取消收藏", key=f"unbookmark_{i}_{page}"):
                    from db import remove_bookmark_by_title
                    remove_bookmark_by_title(title)
                    st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)
