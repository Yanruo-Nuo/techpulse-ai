"""TechPulse AI — 前端入口"""

import os
import streamlit as st
from config import COLORS, get_css
from db import init_db
from metrics_collector import start_metrics_server, init_background_collector

init_db()

# Start Prometheus metrics (idempotent — safe for Streamlit re-runs)
start_metrics_server()
init_background_collector()

st.set_page_config(
    page_title="TechPulse AI",
    layout="wide",
    page_icon="⬛"
)

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

st.markdown(get_css(), unsafe_allow_html=True)

# 侧边栏导航
if "nav_page" not in st.session_state:
    st.session_state.nav_page = "📰 时间线"

with st.sidebar:
    st.markdown(f"""
    <div style="font-size:20px;font-weight:700;color:{COLORS['text_primary']};padding:8px 0 16px 0;">
        ⬛ TechPulse AI
    </div>
    """, unsafe_allow_html=True)

    for item in ["📰 时间线", "⭐ 我的收藏", "🤖 AI 助手"]:
        is_active = st.session_state.nav_page == item
        if st.button(
            item,
            use_container_width=True,
            type="primary" if is_active else "secondary",
            key=f"nav_{item}"
        ):
            st.session_state.nav_page = item
            st.rerun()

# 渲染对应页面
if st.session_state.nav_page == "📰 时间线":
    from panels.timeline import render_timeline
    render_timeline()
elif st.session_state.nav_page == "⭐ 我的收藏":
    from panels.favorites import render_favorites
    render_favorites()
elif st.session_state.nav_page == "🤖 AI 助手":
    from panels.assistant import render_assistant
    render_assistant()
