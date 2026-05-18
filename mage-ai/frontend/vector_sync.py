#!/usr/bin/env python3
"""向量同步脚本：MaxCompute → DashScope embedding → Qdrant

首次运行：全量同步所有文章向量到 Qdrant
后续：可加 --incremental 做增量（按 ds 过滤）

用法:
    python vector_sync.py                # 全量
    python vector_sync.py --incremental  # 增量
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from vector_store import VectorStore
from maxcompute import load_news_data
from dashscope import TextEmbedding

BATCH_SIZE = 25


def sync_all():
    store = VectorStore()
    df = load_news_data()
    if df.empty:
        logger.warning("No news data found, nothing to sync")
        return

    texts = df.apply(
        lambda r: f"标题：{r['title']}\n摘要：{r.get('ai_summary', '')}",
        axis=1,
    ).tolist()
    records = df.to_dict("records")

    all_embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        resp = TextEmbedding.call(model=TextEmbedding.Models.text_embedding_v2, input=batch)
        if resp.status_code != 200:
            logger.error(f"Embedding failed at batch {i // BATCH_SIZE}: {resp.message}")
            raise RuntimeError(f"Embedding API error: {resp.message}")
        all_embeddings.extend(e["embedding"] for e in resp.output["embeddings"])
        logger.info(f"Embedded {i + len(batch)} / {len(texts)}")

    store.upsert_batch(records, all_embeddings)
    logger.info(f"✅ Sync complete: {store.count()} vectors in Qdrant")


if __name__ == "__main__":
    sync_all()
