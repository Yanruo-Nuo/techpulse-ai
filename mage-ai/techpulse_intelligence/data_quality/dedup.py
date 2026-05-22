"""标题相似度去重 + URL 归一化"""

import hashlib
from typing import Any
from urllib.parse import urlparse, urlunparse


def normalize_url(url: str) -> str:
    """去除 URL 中的 tracking 参数和 fragment"""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        # 去掉 fragment 和常见 tracking query params
        clean_query = ""
        if parsed.query:
            clean_params = [
                q for q in parsed.query.split("&")
                if not any(
                    q.lower().startswith(t)
                    for t in ("utm_", "ref=", "source=", "fbclid=", "gclid=")
                )
            ]
            clean_query = "&".join(clean_params)
        clean = urlunparse(
            (parsed.scheme, parsed.netloc.lower(), parsed.path, "", clean_query, "")
        )
        return hashlib.md5(clean.encode()).hexdigest()[:16]
    except Exception:
        return hashlib.md5(url.encode()).hexdigest()[:16]


def compute_similarity(title_a: str, title_b: str) -> float:
    """简化的 Jaccard 相似度（基于单词集合）"""
    set_a = set((title_a or "").lower().split())
    set_b = set((title_b or "").lower().split())
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def is_duplicate(
    new_title: str,
    new_url: str,
    existing: list[dict[str, Any]],
    threshold: float = 0.7,
) -> bool:
    """检查新文章是否与已有文章重复

    Args:
        new_title: 新文章标题
        new_url: 新文章 URL
        existing: 已有文章列表，每项需包含 title 和 url 字段
        threshold: 标题相似度阈值，超过此值视为重复

    Returns:
        True 如果已存在
    """
    new_hash = normalize_url(new_url)
    for article in existing:
        # 检查 URL hash
        existing_url = article.get("url", "")
        if existing_url and normalize_url(existing_url) == new_hash:
            return True
        # 检查标题相似度
        existing_title = article.get("title", "")
        if compute_similarity(new_title, existing_title) > threshold:
            return True
    return False
