#!/usr/bin/env python3
"""向量同步脚本 v2：MaxCompute → DashScope embedding → Qdrant（块级）

支持两种模式：
  - 文章级：旧数据无分块，以文章为单位 embedding（兼容）
  - 块级：新数据有 ai_chunks，以块为单位 embedding（精度更高）

用法:
    python vector_sync.py                          # 全量同步
    python vector_sync.py --mode block             # 强制块级（忽略无分块的文章）
    python vector_sync.py --incremental            # 增量（按 ds 过滤）
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from vector_store import VectorStore
from maxcompute import load_news_data
from dashscope import TextEmbedding

BATCH_SIZE = 25


def sync_all(mode: str = "auto"):
    """全量同步到 Qdrant

    Args:
        mode: "auto" — 有分块用块级，否则用文章级
              "block" — 强制块级，跳过无分块的文章
              "article" — 强制文章级（旧模式）
    """
    store = VectorStore()
    df = load_news_data()
    if df.empty:
        logger.warning("No news data found, nothing to sync")
        return

    records = df.to_dict("records")
    has_chunks = any(r.get("ai_chunks") for r in records)

    if mode == "article" or (mode == "auto" and not has_chunks):
        _sync_article_level(store, records)
    elif mode == "block" or (mode == "auto" and has_chunks):
        _sync_block_level(store, records)
    else:
        _sync_article_level(store, records)


def _sync_article_level(store: VectorStore, records: list[dict]):
    """文章级 embedding（旧模式，兼容）"""
    texts = [
        f"标题：{r.get('title', '')}\n摘要：{r.get('ai_summary', '')}"
        for r in records
    ]
    all_embeddings = _batch_embed(texts)
    if not all_embeddings:
        return

    store.upsert_batch(records, all_embeddings)
    logger.info(f"✅ Article-level sync done: {store.count()} vectors")


def _sync_block_level(store: VectorStore, records: list[dict]):
    """块级 embedding（新模式）"""
    # 将 ai_chunks 展平为 texts 列表，同时保留索引映射
    all_texts = []
    for r in records:
        chunks = r.get("ai_chunks", [])
        if chunks:
            for c in chunks:
                prefix = f"标题：{r.get('title', '')}（片段{c.get('block_index', 0)}）\n"
                all_texts.append(prefix + (c.get("text", "")[:1500]))
        else:
            # 无分块的文章，以文章为单位
            prefix = f"标题：{r.get('title', '')}（全文）\n"
            all_texts.append(prefix + (r.get("ai_summary", "")[:1500]))

    all_embeddings = _batch_embed(all_texts)
    if not all_embeddings:
        return

    count = store.upsert_chunks(records, all_embeddings)
    logger.info(f"✅ Block-level sync done: {count} chunks written, {store.count()} total vectors")


def _batch_embed(texts: list[str]) -> list[list[float]] | None:
    """批量向量化"""
    all_embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i: i + BATCH_SIZE]
        resp = TextEmbedding.call(model=TextEmbedding.Models.text_embedding_v2, input=batch)
        if resp.status_code != 200:
            logger.error(f"Embedding failed at batch {i // BATCH_SIZE}: {resp.message}")
            return None
        all_embeddings.extend(e["embedding"] for e in resp.output["embeddings"])
        logger.info(f"  Embedded {i + len(batch)} / {len(texts)}")
    return all_embeddings


if __name__ == "__main__":
    mode = "auto"
    if "--mode" in sys.argv:
        idx = sys.argv.index("--mode")
        if idx + 1 < len(sys.argv):
            mode = sys.argv[idx + 1]
    sync_all(mode=mode)
