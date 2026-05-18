"""文档分块引擎 — 按来源类型路由，分块为 512 tokens/块，overlap 64 tokens

用法:
  from chunker import chunk_article, ChunkConfig
  chunks = chunk_article({"title": "...", "content": "...", "source": "hackernews"})

输出:
  list[dict] — 每个 chunk 包含 text, block_index, metadata
"""

import re
from typing import Any


CHUNK_SIZE = 512    # tokens（约 800 中文字符）
CHUNK_OVERLAP = 64  # tokens（约 100 字符，避免上下文断裂）


def chunk_article(article: dict) -> list[dict]:
    """主入口：根据来源类型路由到不同的分块策略"""
    source = (article.get("source") or "").lower()
    title = article.get("title") or ""
    content = article.get("content_excerpt") or article.get("content") or ""

    if not content.strip():
        return _chunk_fallback(title, content)

    # 按来源路由
    if source in ("hackernews", "lobsters", "devto"):
        return _chunk_by_paragraph(title, content, source)
    elif source == "reddit":
        return _chunk_reddit(title, content, source)
    elif source == "github_trending":
        return _chunk_github(title, content, source)
    elif source in ("rss", "techcrunch", "arstechnica", "verge", "wired"):
        return _chunk_by_paragraph(title, content, source)
    else:
        return _chunk_fallback(title, content)


def _split_into_chunks(text: str, max_chars: int = 800, overlap_chars: int = 100) -> list[str]:
    """按字符数切割 + overlap，保留段落边界"""
    if len(text) <= max_chars:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        if end >= len(text):
            chunks.append(text[start:])
            break

        # 向前查找段落边界（\n\n），避免在句子中间切断
        boundary = text.rfind("\n\n", start, end)
        if boundary > start + max_chars // 2:
            end = boundary + 2
        else:
            # 没有段落边界，向前查找句号
            boundary = text.rfind("。", start, end)
            if boundary > start + max_chars // 2:
                end = boundary + 1

        chunks.append(text[start:end])
        start = end - overlap_chars  # overlap

    return chunks


def _build_chunk(title: str, text: str, source: str, index: int, chunk_type: str = "paragraph") -> dict:
    return {
        "text": text.strip(),
        "block_index": index,
        "metadata": {
            "title": title,
            "source": source,
            "block_type": chunk_type,
            "block_total_chars": len(text),
        },
    }


# ─── 分块策略 ───

def _chunk_by_paragraph(title: str, content: str, source: str) -> list[dict]:
    """策略 1: 按段落分割（HN / Lobsters / Dev.to / RSS）"""
    paragraphs = [p.strip() for p in content.split("\n\n") if p.strip()]
    chunks = []
    current_text = ""
    current_len = 0

    for para in paragraphs:
        para_len = len(para)
        if current_len + para_len > 800 and current_text:
            chunks.append(_build_chunk(title, current_text, source, len(chunks)))
            current_text = para
            current_len = para_len
        else:
            if current_text:
                current_text += "\n\n"
            current_text += para
            current_len += para_len

    if current_text:
        chunks.append(_build_chunk(title, current_text, source, len(chunks)))

    if not chunks:
        chunks.append(_build_chunk(title, content[:800], source, 0))

    return chunks


def _chunk_reddit(title: str, content: str, source: str) -> list[dict]:
    """策略 2: Reddit — 正文 + 高赞评论"""
    # 通常 content 已经包含正文 + 评论
    chunks = _chunk_by_paragraph(title, content, source)
    # 标记第一个块为"post"，后续为"comment"
    for i, c in enumerate(chunks):
        c["metadata"]["block_type"] = "post" if i == 0 else "comment"
    return chunks


def _chunk_github(title: str, content: str, source: str) -> list[dict]:
    """策略 3: GitHub Trending — README 按章节分割"""
    chunks = []
    # 尝试按 Markdown 标题分割
    sections = re.split(r"\n(?=#{1,3}\s)", content)
    current_section = ""

    for section in sections:
        if not section.strip():
            continue
        # 提取章节标题
        heading_match = re.match(r"(#{1,3})\s+(.+)", section)
        if heading_match:
            current_section = heading_match.group(2).strip()
        if len(section) > 800:
            sub_chunks = _split_into_chunks(section, max_chars=800, overlap_chars=100)
            for i, sc in enumerate(sub_chunks):
                chunks.append(_build_chunk(title, sc, source, len(chunks),
                                          chunk_type=f"section:{current_section}"))
        else:
            chunks.append(_build_chunk(title, section, source, len(chunks),
                                      chunk_type=f"section:{current_section}"))

    if not chunks:
        chunks.append(_build_chunk(title, content[:800], source, 0, chunk_type="readme"))

    return chunks


def _chunk_fallback(title: str, content: str) -> list[dict]:
    """兜底：简单分段"""
    text = content or title
    raw_chunks = _split_into_chunks(text, max_chars=800, overlap_chars=100)
    return [
        _build_chunk(title, c, "unknown", i, chunk_type="fallback")
        for i, c in enumerate(raw_chunks)
    ]
