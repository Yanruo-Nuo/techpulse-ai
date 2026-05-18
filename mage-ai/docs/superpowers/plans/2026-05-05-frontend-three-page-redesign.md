# TechPulse AI 三页面前端重构实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将单页 Streamlit 前端重构为三页面架构（时间线/收藏/AI助手），新增 SQLite 个人收藏夹功能，并应用 Data-Dense Dashboard 设计系统。

**Architecture:** 以 `app.py` 作为入口 → 侧边栏导航 → 路由到三个独立页面模块。共享层（MaxCompute 连接、SQLite 操作、设计 tokens）抽取为独立模块。SQLite 持久化收藏夹数据，无额外依赖。

**Tech Stack:** Streamlit, Pandas, Plotly, pyODPS, DashScope, SQLite3（stdlib）

---

## 文件结构

```
frontend/
  app.py              # 入口：侧边栏导航 + 页面路由 + 共享状态初始化
  config.py           # 设计 tokens（CSS 暗色主题、颜色常量）
  db.py               # SQLite 初始化 + CRUD（收藏夹、书签、历史）
  maxcompute.py       # MaxCompute 数据加载（从 app.py 抽取）
  components.py       # 可复用组件：新闻卡片、分页、图表、KPI 卡片
  pages/
    __init__.py       # 空文件
    timeline.py       # 页面 1：时间线（热点资讯流 + 看板图表 + 过滤）
    favorites.py      # 页面 2：我的收藏（树状收藏夹 + 详情面板）
    assistant.py      # 页面 3：AI 助手（对话式 RAG）
  Dockerfile          # 无变化（sqlite3 是 stdlib）
  requirements.txt    # 无变化
```

---

### Task 1: 创建共享模块 — config.py、db.py、maxcompute.py、components.py

**Files:**
- Create: `frontend/config.py`
- Create: `frontend/db.py`
- Create: `frontend/maxcompute.py`
- Create: `frontend/components.py`

#### 1a: config.py — 设计系统 tokens + CSS

- [ ] **Step: 创建 config.py**

```python
"""TechPulse AI 设计系统 tokens 与 CSS"""

# === Color Tokens (Data-Dense Dashboard 暗色主题) ===
COLORS = {
    "primary": "#1E40AF",
    "primary_light": "#3B82F6",
    "accent": "#D97706",
    "bg_main": "#0F172A",
    "bg_card": "#1E293B",
    "bg_hover": "#334155",
    "text_primary": "#F1F5F9",
    "text_secondary": "#94A3B8",
    "text_muted": "#64748B",
    "border": "#334155",
    "heat_high": "#22C55E",
    "heat_mid": "#EAB308",
    "heat_low": "#6B7280",
    "destructive": "#DC2626",
    "chart_blue": "#3B82F6",
    "chart_amber": "#D97706",
    "chart_green": "#22C55E",
    "chart_purple": "#A855F7",
    "chart_cyan": "#06B6D4",
    "chart_rose": "#F43F5E",
    "chart_indigo": "#6366F1",
}

CATEGORY_COLORS = {
    "AI/ML": "#3B82F6",
    "Programming": "#22C55E",
    "CloudNative": "#06B6D4",
    "Security": "#DC2626",
    "Hardware": "#D97706",
    "DataEngineering": "#A855F7",
    "Others": "#64748B",
}

ITEMS_PER_PAGE = 10

CATEGORY_ORDER = [
    "AI/ML", "Programming", "CloudNative", "Security",
    "Hardware", "DataEngineering", "Others"
]


def get_css():
    """返回暗色主题全局 CSS"""
    return f"""
    <style>
        /* 全局暗色主题 */
        .stApp {{ background-color: {COLORS['bg_main']}; color: {COLORS['text_primary']}; }}
        .stApp header {{ background-color: {COLORS['bg_main']}; }}
        
        /* 卡片样式 */
        .news-card {{
            background: {COLORS['bg_card']};
            border: 1px solid {COLORS['border']};
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 12px;
            transition: background 0.2s;
        }}
        .news-card:hover {{ background: {COLORS['bg_hover']}; }}
        
        /* 热度条 */
        .heat-bar {{
            height: 4px;
            border-radius: 2px;
            margin-top: 8px;
            transition: width 0.3s;
        }}
        
        /* 标签 */
        .tag {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
            margin-right: 4px;
        }}
        
        /* 分类色条 */
        .cat-indicator {{
            width: 3px;
            height: 100%;
            min-height: 60px;
            border-radius: 2px;
            flex-shrink: 0;
        }}
        
        /* 顶栏 */
        .topbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 8px 0;
            margin-bottom: 16px;
            border-bottom: 1px solid {COLORS['border']};
        }}
        
        /* 侧边栏导航 */
        section[data-testid="stSidebar"] {{
            background-color: {COLORS['bg_card']};
            border-right: 1px solid {COLORS['border']};
        }}
        
        /* 链接 */
        a {{ color: {COLORS['primary_light']}; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        
        /* 选中高亮 */
        .highlight {{ border-left: 3px solid {COLORS['accent']}; padding-left: 12px; }}
        
        /* scrollbar */
        ::-webkit-scrollbar {{ width: 6px; }}
        ::-webkit-scrollbar-track {{ background: {COLORS['bg_main']}; }}
        ::-webkit-scrollbar-thumb {{ background: {COLORS['border']}; border-radius: 3px; }}
    </style>
    """
```

#### 1b: db.py — SQLite 收藏夹系统

- [ ] **Step: 创建 db.py**

```python
"""SQLite 持久化：收藏夹、书签、阅读历史"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "techpulse_user.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """初始化表结构（幂等）"""
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            icon TEXT DEFAULT '📁',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id INTEGER NOT NULL,
            article_title TEXT NOT NULL,
            article_url TEXT DEFAULT '',
            tech_category TEXT DEFAULT '',
            ai_summary TEXT DEFAULT '',
            ai_insight TEXT DEFAULT '',
            score REAL DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS reading_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_title TEXT NOT NULL,
            article_url TEXT DEFAULT '',
            tech_category TEXT DEFAULT '',
            read_at TEXT DEFAULT (datetime('now'))
        );

        -- 默认收藏夹
        INSERT OR IGNORE INTO collections (id, name, icon) VALUES (1, '全部收藏', '📂');
        INSERT OR IGNORE INTO collections (id, name, icon) VALUES (2, 'AI/ML', '🤖');
        INSERT OR IGNORE INTO collections (id, name, icon) VALUES (3, '编程', '💻');
    """)
    conn.commit()
    conn.close()


# ---- 收藏夹 CRUD ----

def list_collections():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM collections ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_collection(name: str, icon: str = "📁"):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO collections (name, icon) VALUES (?, ?)", (name, icon))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def delete_collection(collection_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
    conn.commit()
    conn.close()


# ---- 书签 CRUD ----

def add_bookmark(collection_id: int, article: dict):
    conn = get_conn()
    conn.execute("""
        INSERT INTO bookmarks (collection_id, article_title, article_url, tech_category, ai_summary, ai_insight, score)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        collection_id,
        article.get("title", ""),
        article.get("url", ""),
        article.get("tech_category", ""),
        article.get("ai_summary", ""),
        article.get("ai_insight", ""),
        float(article.get("score", 0) or 0),
    ))
    conn.commit()
    conn.close()


def remove_bookmark(bookmark_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM bookmarks WHERE id = ?", (bookmark_id,))
    conn.commit()
    conn.close()


def list_bookmarks(collection_id: int = None):
    conn = get_conn()
    if collection_id:
        rows = conn.execute(
            "SELECT b.*, c.name as collection_name FROM bookmarks b JOIN collections c ON b.collection_id = c.id WHERE b.collection_id = ? ORDER BY b.added_at DESC",
            (collection_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT b.*, c.name as collection_name FROM bookmarks b JOIN collections c ON b.collection_id = c.id ORDER BY b.added_at DESC"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def is_bookmarked(article_title: str) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT 1 FROM bookmarks WHERE article_title = ? LIMIT 1", (article_title,)).fetchone()
    conn.close()
    return row is not None


# ---- 阅读历史 ----

def add_to_history(article: dict):
    conn = get_conn()
    conn.execute(
        "INSERT INTO reading_history (article_title, article_url, tech_category) VALUES (?, ?, ?)",
        (article.get("title", ""), article.get("url", ""), article.get("tech_category", ""))
    )
    conn.commit()
    conn.close()


def list_history(limit: int = 50):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM reading_history ORDER BY read_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
```

#### 1c: maxcompute.py — 数据加载层

- [ ] **Step: 创建 maxcompute.py**

```python
"""MaxCompute 数据加载（从 app.py 抽取）"""

import os
import streamlit as st
import pandas as pd
from odps import ODPS


def get_odps():
    return ODPS(
        os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID'),
        os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET'),
        project=os.getenv('MAXCOMPUTE_PROJECT'),
        endpoint=os.getenv('MAXCOMPUTE_ENDPOINT')
    )


@st.cache_data(ttl=600, show_spinner="📊 加载趋势数据...")
def load_trend_data():
    """从 dbt mart 层加载趋势数据"""
    try:
        o = get_odps()
        sql = """
            SELECT ds, tech_category, daily_cnt
            FROM mart_trend_analysis
            WHERE ds IS NOT NULL
            ORDER BY ds ASC
        """
        with o.execute_sql(sql, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
            df = reader.to_pandas()
            df['ds'] = pd.to_datetime(df['ds'], errors='coerce')
            df = df.dropna(subset=['ds'])
            return df
    except Exception as e:
        st.error(f"❌ 趋势数据加载失败：{str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner="📰 加载新闻数据...")
def load_news_data():
    """从 hn_raw 加载去重后的新闻数据"""
    try:
        o = get_odps()
        sql = """
            SELECT id, title, url, score, ai_summary, ai_insight,
                   tech_category, ingest_time, ds
            FROM (
                SELECT id, title, url, score, ai_summary, ai_insight,
                       tech_category, ingest_time, ds,
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingest_time DESC) AS rn
                FROM hn_raw
                WHERE ds IS NOT NULL
            ) t
            WHERE rn = 1
            ORDER BY ingest_time DESC
        """
        with o.execute_sql(sql, hints={'odps.namespace.schema': 'true'}).open_reader() as reader:
            return reader.to_pandas()
    except Exception as e:
        st.error(f"❌ 新闻数据加载失败：{str(e)}")
        return pd.DataFrame()
```

#### 1d: components.py — 可复用 UI 组件

- [ ] **Step: 创建 components.py**

```python
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
        template="plotly_dark",
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
        template="plotly_dark",
        color_discrete_map=CATEGORY_COLORS,
    )
    fig.update_layout(
        height=height, margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor=COLORS["bg_main"], plot_bgcolor=COLORS["bg_card"],
        font=dict(color=COLORS["text_primary"]),
        showlegend=True,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_news_card(article: pd.Series, index: int, is_bookmarked: bool = False):
    """单条新闻卡片渲染（返回点击动作）"""
    cat = article.get('tech_category', 'Others')
    cat_color = CATEGORY_COLORS.get(cat, COLORS["text_muted"])
    score = int(article.get('score', 0) or 0)
    title = article.get('title', '无标题')
    insight = article.get('ai_insight', '暂无分析')
    ingest_time = article.get('ingest_time', '')
    article_url = article.get('url', '')

    heat_pct = min(100, score)

    col1, col2 = st.columns([20, 1])
    with col1:
        st.markdown(f"""
        <div class="news-card">
            <div style="display:flex;gap:12px;">
                <div class="cat-indicator" style="background:{cat_color};"></div>
                <div style="flex:1;">
                    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
                        <span class="tag" style="background:{cat_color}22;color:{cat_color};">{cat}</span>
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
    with col2:
        if is_bookmarked:
            st.markdown(f"<div style='color:{COLORS['accent']};font-size:20px;'>⭐</div>", unsafe_allow_html=True)
    return title, insight, cat, article_url, score


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
```

- [ ] **Step: 创建 pages/__init__.py**

```python
# pages package
```

---

### Task 2: 实现页面 1 — 时间线 (Timeline)

**Files:**
- Create: `frontend/pages/timeline.py`

#### 2a: 时间线页面

- [ ] **Step: 创建 pages/timeline.py**

```python
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
        total_articles = len(df_news)
        categories = df_news['tech_category'].nunique()
        avg_score = int(df_news['score'].astype(float).mean()) if 'score' in df_news.columns else 0
        kcols = st.columns(4)
        with kcols[0]:
            render_kpi_card("📄 资讯总数", f"{total_articles}", color=COLORS["chart_blue"])
        with kcols[1]:
            render_kpi_card("📂 覆盖分类", f"{categories}", color=COLORS["chart_green"])
        with kcols[2]:
            render_kpi_card("🔥 平均热度", f"{avg_score}", color=COLORS["chart_amber"])
        with kcols[3]:
            if not df_trend.empty:
                latest_date = df_trend['ds'].max().strftime('%m-%d')
                render_kpi_card("📈 最新趋势日", latest_date, color=COLORS["chart_purple"])
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

    # 应用过滤
    filtered_df = df_news.copy()
    if "全部" not in selected_categories and selected_categories:
        filtered_df = filtered_df[filtered_df['tech_category'].isin(selected_categories)]

    if sort_by == "热度最高" and 'score' in filtered_df.columns:
        filtered_df = filtered_df.copy()
        filtered_df['score'] = pd.to_numeric(filtered_df['score'], errors='coerce').fillna(0)
        filtered_df = filtered_df.sort_values('score', ascending=False)

    # ====== 资讯列表 ======
    st.subheader(f"资讯列表（共 {len(filtered_df)} 条）")
    if filtered_df.empty:
        st.info("当前筛选条件下没有资讯")
        return

    page = render_pagination(len(filtered_df), "timeline_page")
    start = page * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE

    for i, (_, row) in enumerate(filtered_df.iloc[start:end].iterrows()):
        bookmarked = is_bookmarked(row.get('title', ''))
        title, insight, cat, url, score = render_news_card(row, i, bookmarked)

        # 操作按钮行
        action_cols = st.columns([1, 1, 4])
        with action_cols[0]:
            if url and url.strip():
                st.markdown(f'<a href="{url}" target="_blank" style="font-size:14px;">🔗 原文链接</a>', unsafe_allow_html=True)
        with action_cols[1]:
            if not bookmarked:
                if st.button("⭐ 收藏", key=f"bookmark_{i}_{page}"):
                    from db import add_bookmark
                    add_bookmark(1, {
                        "title": title,
                        "url": url,
                        "tech_category": cat,
                        "ai_summary": "",
                        "ai_insight": insight,
                        "score": score,
                    })
                    st.success("已收藏")
                    st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)
```

---

### Task 3: 实现页面 2 — 我的收藏 (Favorites)

**Files:**
- Create: `frontend/pages/favorites.py`

#### 3a: 收藏页面

- [ ] **Step: 创建 pages/favorites.py**

```python
"""页面 2：我的收藏 — 树状收藏夹 + 详情面板"""

import streamlit as st
from config import COLORS, get_css
from components import render_empty_state
from db import (
    init_db, list_collections, create_collection, delete_collection,
    list_bookmarks, add_bookmark, remove_bookmark
)


def render_favorites():
    st.markdown(get_css(), unsafe_allow_html=True)
    init_db()

    st.markdown(f"""
    <div class="topbar">
        <div style="font-size:24px;font-weight:700;color:{COLORS['text_primary']};">⭐ 我的收藏</div>
    </div>
    """, unsafe_allow_html=True)

    # ====== 左侧收藏夹 + 右侧文章列表 ======
    left, right = st.columns([1, 2])

    with left:
        st.markdown(f"<div style='color:{COLORS['text_secondary']};font-size:14px;margin-bottom:8px;'>收藏夹</div>", unsafe_allow_html=True)

        collections = list_collections()
        selected_id = st.session_state.get("selected_collection", collections[0]["id"] if collections else None)

        for col in collections:
            btn_label = f"{col['icon']} {col['name']}"
            if st.button(btn_label, key=f"col_{col['id']}",
                         use_container_width=True,
                         type="secondary" if selected_id != col['id'] else "primary"):
                st.session_state.selected_collection = col['id']
                st.session_state.selected_article = None
                st.rerun()

        # 新建收藏夹
        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("➕ 新建收藏夹"):
            new_name = st.text_input("名称", key="new_col_name", label_visibility="collapsed", placeholder="收藏夹名称")
            if st.button("创建", use_container_width=True) and new_name:
                if create_collection(new_name):
                    st.success(f"收藏夹「{new_name}」已创建")
                    st.rerun()
                else:
                    st.error("名称已存在")

    with right:
        col_id = st.session_state.get("selected_collection", selected_id)
        bookmarks = list_bookmarks(col_id) if col_id else []

        col_name = "全部"
        for c in collections:
            if c["id"] == col_id:
                col_name = c["name"]
                break

        st.markdown(f"<div style='color:{COLORS['text_secondary']};font-size:14px;margin-bottom:8px;'>{col_name}（{len(bookmarks)} 篇）</div>", unsafe_allow_html=True)

        if not bookmarks:
            render_empty_state("📂", "还没有收藏", "在时间线页面点击 ⭐ 收藏按钮来保存你感兴趣的文章")
            return

        # 文章列表
        for bm in bookmarks:
            with st.container():
                border_color = COLORS["accent"] if st.session_state.get("selected_article") == bm["id"] else COLORS["border"]
                st.markdown(f"""
                <div style="background:{COLORS['bg_card']};border:1px solid {border_color};
                            border-radius:8px;padding:12px;margin-bottom:8px;cursor:pointer;">
                    <div style="font-size:15px;font-weight:600;color:{COLORS['text_primary']};">{bm['article_title']}</div>
                    <div style="display:flex;gap:12px;margin-top:6px;font-size:13px;color:{COLORS['text_muted']};">
                        <span>🔥 {int(bm['score'])}</span>
                        <span>📂 {bm['tech_category']}</span>
                        <span>📅 {bm['added_at'][:10]}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                detail_cols = st.columns([1, 1, 3])
                with detail_cols[0]:
                    if st.button("📖 查看详情", key=f"detail_{bm['id']}"):
                        st.session_state.selected_article = bm["id"]
                        st.rerun()
                with detail_cols[1]:
                    if st.button("🗑️ 删除", key=f"del_{bm['id']}"):
                        remove_bookmark(bm["id"])
                        st.rerun()
                with detail_cols[2]:
                    if bm.get("article_url"):
                        st.markdown(f'<a href="{bm["article_url"]}" target="_blank" style="font-size:14px;">🔗 原文链接</a>', unsafe_allow_html=True)

        # ====== 详情面板 ======
        sel_id = st.session_state.get("selected_article")
        if sel_id:
            target = next((b for b in bookmarks if b["id"] == sel_id), None)
            if target:
                st.markdown(f"<hr style='border-color:{COLORS['border']};'>", unsafe_allow_html=True)
                st.markdown(f"""
                <div style="background:{COLORS['bg_card']};border:1px solid {COLORS['border']};
                            border-radius:8px;padding:20px;">
                    <div style="font-size:20px;font-weight:700;color:{COLORS['text_primary']};">{target['article_title']}</div>
                    <div style="display:flex;gap:12px;margin:12px 0;font-size:13px;color:{COLORS['text_muted']};">
                        <span>🔥 {int(target['score'])}</span>
                        <span>📂 {target['tech_category']}</span>
                        <span>📅 {target['added_at'][:10]}</span>
                    </div>
                    <div style="margin-top:16px;">
                        <div style="font-size:14px;font-weight:600;color:{COLORS['text_primary']};">📝 AI 摘要</div>
                        <div style="color:{COLORS['text_secondary']};font-size:14px;margin-top:4px;">{target.get('ai_summary') or '（无）'}</div>
                    </div>
                    <div style="margin-top:16px;">
                        <div style="font-size:14px;font-weight:600;color:{COLORS['text_primary']};">💡 AI 洞察</div>
                        <div style="color:{COLORS['text_secondary']};font-size:14px;margin-top:4px;">{target.get('ai_insight') or '（无）'}</div>
                    </div>
                    {"<div style='margin-top:16px;'><a href='" + target['article_url'] + "' target='_blank' style='font-size:14px;'>🔗 查看原文</a></div>" if target.get('article_url') else ""}
                </div>
                """, unsafe_allow_html=True)
```

---

### Task 4: 实现页面 3 — AI 助手 (Assistant)

**Files:**
- Create: `frontend/pages/assistant.py`

#### 4a: AI 助手页面

- [ ] **Step: 创建 pages/assistant.py**

```python
"""页面 3：AI 助手 — 多轮对话 RAG"""

import streamlit as st
import pandas as pd
import math
import dashscope
from dashscope import TextEmbedding
from config import COLORS, CATEGORY_ORDER, get_css
from components import render_empty_state
from maxcompute import load_news_data, load_trend_data


def cosine_similarity(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    return dot / (norm_a * norm_b) if norm_a and norm_b else 0


@st.cache_data(ttl=3600, show_spinner="🧠 生成知识库向量索引...")
def compute_news_embeddings(texts):
    batch_size = 25
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        emb_resp = TextEmbedding.call(
            model=TextEmbedding.Models.text_embedding_v2,
            input=batch
        )
        if emb_resp.status_code != 200:
            st.error(f"❌ 向量生成失败（批次 {i // batch_size}）：{emb_resp.message}")
            return None
        all_embeddings.extend(e['embedding'] for e in emb_resp.output['embeddings'])
    return all_embeddings


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

    # 2. 构建知识库
    kb_texts = df.apply(
        lambda row: f"标题：{row['title']}\n分析：{row.get('ai_insight', '')}\n分类：{row.get('tech_category', '')}",
        axis=1
    ).tolist()

    # 3. 缓存的向量
    emb = st.session_state.get('news_embeddings')
    if emb is None:
        emb = compute_news_embeddings(kb_texts)
        if emb is None:
            st.session_state.pop('news_embeddings', None)
            return "知识库向量化失败，请重试。"
        st.session_state.news_embeddings = emb
    news_embs = emb

    # 4. 相似度排序 top-5
    scored = [(cosine_similarity(q_embedding, ne), i) for i, ne in enumerate(news_embs)]
    scored.sort(key=lambda x: x[0], reverse=True)
    top_indices = [scored[i][1] for i in range(min(5, len(scored)))]

    # 5. 文章索引
    article_index_lines = []
    for cat in CATEGORY_ORDER:
        cat_articles = df[df['tech_category'] == cat]
        if not cat_articles.empty:
            titles = "\n".join(f"  「{r['title'][:60]}」" for _, r in cat_articles.iterrows())
            article_index_lines.append(f"【{cat}】共{len(cat_articles)}篇：\n{titles}")
    article_index = "\n\n".join(article_index_lines)

    # 6. 分类分布
    cat_counts = df['tech_category'].value_counts()
    dist_text = "、".join(f"{k} {v}篇" for k, v in cat_counts.items())

    # 7. 热门排行
    hot_text = "（无热度数据）"
    if 'score' in df.columns:
        hot_df = df.dropna(subset=['score']).copy()
        hot_df['score'] = pd.to_numeric(hot_df['score'], errors='coerce').fillna(0)
        hot_df = hot_df.nlargest(10, 'score')
        hot_list = [f"  🔥 {r['title'][:60]} | 热度 {int(r['score'])} | {r['tech_category']}" for _, r in hot_df.iterrows()]
        hot_text = "\n".join(hot_list)

    # 8. 趋势概览
    trend_overview = ""
    if df_trend is not None and not df_trend.empty:
        latest_date = df_trend['ds'].max()
        latest = df_trend[df_trend['ds'] == latest_date]
        if not latest.empty:
            ranked = latest.sort_values('daily_cnt', ascending=False)
            ranking = " > ".join(f"{r['tech_category']}({int(r['daily_cnt'])}条)" for _, r in ranked.iterrows())
            trend_overview = f"📈 最新热榜（{latest_date.strftime('%Y-%m-%d')}）：{ranking}"

    # 9. 检索详情
    context_parts = []
    for idx in top_indices:
        row = df.iloc[idx]
        context_parts.append(
            f"- 标题：{row['title']}\n  分析：{row.get('ai_insight', '无')}\n  分类：{row.get('tech_category', '未知')}\n  热度：{row.get('score', '未知')}"
        )
    context = "\n\n".join(context_parts)

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
        model="deepseek-v4-flash",
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
                # Generate answer immediately
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
```

---

### Task 5: 重构 app.py — 入口 + 导航路由

**Files:**
- Modify: `frontend/app.py`

#### 5a: 重写 app.py

- [ ] **Step: 重写 app.py 为入口 + 导航**

```python
"""TechPulse AI — 三页面前端入口"""

import os
import streamlit as st
from config import COLORS, get_css

# -------------------------- 页面配置 --------------------------
st.set_page_config(
    page_title="TechPulse AI",
    layout="wide",
    page_icon="⬛"
)

# -------------------------- 环境校验 --------------------------
REQUIRED_ENV = [
    "ALIBABA_CLOUD_ACCESS_KEY_ID",
    "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
    "MAXCOMPUTE_PROJECT",
    "MAXCOMPUTE_ENDPOINT",
    "DASHSCOPE_API_KEY",
]
missing = [e for e in REQUIRED_ENV if not os.getenv(e)]
if missing:
    st.error(f"❌ 缺失环境变量：{', '.join(missing)}，请配置后运行！")
    st.stop()

# -------------------------- 全局 CSS --------------------------
st.markdown(get_css(), unsafe_allow_html=True)

# -------------------------- 侧边栏导航 --------------------------
st.sidebar.markdown(f"""
<div style="font-size:22px;font-weight:700;color:{COLORS['text_primary']};padding:12px 0;border-bottom:1px solid {COLORS['border']};margin-bottom:16px;">
    ⬛ TechPulse AI
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown(f"<div style='color:{COLORS['text_muted']};font-size:13px;margin-bottom:8px;'>导航</div>", unsafe_allow_html=True)

page = st.sidebar.radio(
    "页面",
    ["📰 时间线", "⭐ 我的收藏", "🤖 AI 助手"],
    label_visibility="collapsed",
    key="nav_radio"
)

st.sidebar.markdown(f"<hr style='border-color:{COLORS['border']};margin:16px 0;'>", unsafe_allow_html=True)

# 刷新按钮
if st.sidebar.button("🔄 刷新数据", type="primary", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown(f"""
<div style="margin-top:16px;font-size:12px;color:{COLORS['text_muted']};">
    📌 数据源：MaxCompute 数仓<br>
    ⚡ 自动 10 分钟缓存
</div>
""", unsafe_allow_html=True)

# -------------------------- 页面路由 --------------------------
if page == "📰 时间线":
    from pages.timeline import render_timeline
    render_timeline()
elif page == "⭐ 我的收藏":
    from pages.favorites import render_favorites
    render_favorites()
elif page == "🤖 AI 助手":
    from pages.assistant import render_assistant
    render_assistant()
```

---

### Task 6: 更新 Docker Compose（开发模式卷挂载）

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step: 为 tech-frontend 添加 volume 挂载**

```yaml
  # 修改 tech-frontend 服务，添加 volumes 实现热更新
  tech-frontend:
    build:
      context: ./frontend
    container_name: tech-frontend
    ports:
      - "8501:8501"
    environment:
      - ALIBABA_CLOUD_ACCESS_KEY_ID=${ALIBABA_CLOUD_ACCESS_KEY_ID}
      - ALIBABA_CLOUD_ACCESS_KEY_SECRET=${ALIBABA_CLOUD_ACCESS_KEY_SECRET}
      - MAXCOMPUTE_PROJECT=techpulse_dw
      - MAXCOMPUTE_ENDPOINT=http://service.cn-hongkong.maxcompute.aliyun.com/api
      - DASHSCOPE_API_KEY=${DASHSCOPE_API_KEY}
    volumes:                          # ← 新增
      - ./frontend:/app               # ← 挂载 frontend 目录实现热更新
    restart: always
```

---

### Task 7: 构建部署与验证

- [ ] **Step: 创建目录结构**

```bash
mkdir -p /root/techpulse-ai/mage-ai/frontend/pages
touch /root/techpulse-ai/mage-ai/frontend/pages/__init__.py
```

- [ ] **Step: 重建前端容器**

```bash
cd /root/techpulse-ai/mage-ai
docker compose up -d --build tech-frontend
```

- [ ] **Step: 验证三页面导航**

打开 `http://localhost:8501`，检查：
1. 左侧导航栏显示三个页面选项
2. 时间线页面展示 KPI 卡片 + 图表 + 过滤 + 资讯列表
3. 收藏页面展示收藏夹树 + 文章列表 + 详情面板
4. AI 助手页面展示对话界面 + 快捷按钮
5. 全页面暗色主题一致

- [ ] **Step: 验证 SQLite 持久化**

```bash
# 确认 SQLite 文件已创建
ls -la /root/techpulse-ai/mage-ai/frontend/techpulse_user.db
# 验证表结构
sqlite3 /root/techpulse-ai/mage-ai/frontend/techpulse_user.db ".tables"
```

---

## Self-Review Checklist

| 需求 | 对应 Task |
|------|-----------|
| 三页面导航（时间线/收藏/AI助手） | Task 5 (app.py 路由) |
| 时间线：KPI 卡片 + 趋势图 + 饼图 | Task 2a (components.py kpi + chart) |
| 时间线：分类过滤 + 排序 | Task 2a (timeline.py filter) |
| 时间线：热度条 + 收藏按钮 | Task 2a (news_card + bookmark action) |
| 收藏：树状收藏夹 | Task 3a (favorites.py left panel) |
| 收藏：详情面板（摘要+洞察） | Task 3a (favorites.py detail panel) |
| 收藏：新建/删除收藏夹 | Task 3a (create_collection/delete_collection) |
| AI 助手：多轮对话 | Task 4a (assistant.py chat loop) |
| AI 助手：快捷按钮 | Task 4a (quick_prompts) |
| AI 助手：引用来源 | Task 4a (prompt with citations) |
| 暗色主题设计系统 | Task 1a (config.py CSS + tokens) |
| SQLite 持久化 | Task 1b (db.py CRUD) |
| 数据层抽取 | Task 1c (maxcompute.py) |
| 可复用组件 | Task 1d (components.py) |
| Volume 热更新 | Task 6 (docker-compose.yml) |
