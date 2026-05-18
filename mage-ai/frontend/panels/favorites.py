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
        # "全部收藏" 无 collection 过滤，展示所有书签
        if col_id == 1:
            bookmarks = list_bookmarks()
        else:
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
            border_color = COLORS["accent"] if st.session_state.get("selected_article") == bm["id"] else COLORS["border"]
            st.markdown(f"""
            <div style="background:{COLORS['bg_card']};border:1px solid {border_color};
                        border-radius:8px;padding:12px;margin-bottom:8px;">
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
                    if st.session_state.get("selected_article") == bm["id"]:
                        st.session_state.selected_article = None
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
