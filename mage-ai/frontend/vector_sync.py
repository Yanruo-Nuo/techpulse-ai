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
    from sync_metrics import sync_total
    from sync_metrics import sync_duration_seconds as duration

    store = VectorStore()
    df = load_news_data()
    if df.empty:
        sync_total.labels(collection="tech_news", status="empty").inc()
        logger.warning("No news data found, nothing to sync")
        return

    records = df.to_dict("records")
    has_chunks = any(r.get("ai_chunks") for r in records)
    import time
    start = time.time()

    if mode == "article" or (mode == "auto" and not has_chunks):
        _sync_article_level(store, records)
        sync_total.labels(collection="tech_news", status="success").inc()
    elif mode == "block" or (mode == "auto" and has_chunks):
        _sync_block_level(store, records)
        sync_total.labels(collection="tech_news", status="success").inc()
    else:
        _sync_article_level(store, records)
        sync_total.labels(collection="tech_news", status="success").inc()

    elapsed = time.time() - start
    duration.labels(collection="tech_news").observe(elapsed)
    logger.info(f"⏱️  Sync completed in {elapsed:.1f}s")


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
                # chunker 生成的分块 ≤800 字符，DSS 上限 3000 字符，不截断
                all_texts.append(prefix + c.get("text", ""))
        else:
            # 无分块的文章，以文章为单位；摘要最长截断到 2500 字符（DSS 上限 ~3000）
            prefix = f"标题：{r.get('title', '')}（全文）\n"
            all_texts.append(prefix + (r.get("ai_summary", "")[:2500]))

    all_embeddings = _batch_embed(all_texts)
    if not all_embeddings:
        return

    count = store.upsert_chunks(records, all_embeddings)
    logger.info(f"✅ Block-level sync done: {count} chunks written, {store.count()} total vectors")

    # 实体关系三元组同步
    _sync_triples(store, records)


def _sync_triples(store: VectorStore, records: list[dict]):
    """将文章中的实体关系三元组写入 tech_entities"""
    from sync_metrics import triple_sync_total, articles_with_zero_triples, triple_sync_articles_scanned

    triples_per_article = []
    texts = []
    empty_article_count = 0
    total_articles = len(records)

    for r in records:
        article_triples = r.get("ai_triples", [])
        source = r.get("source", "unknown")
        triple_sync_articles_scanned.labels(source=source).inc()

        if not article_triples:
            empty_article_count += 1
            articles_with_zero_triples.labels(source=source).inc()
            triples_per_article.append([])
            continue

        triples_per_article.append(article_triples)
        for t in article_triples:
            text = f"{t.get('subject', '')} --{t.get('predicate', '')}--> {t.get('object', '')}"
            texts.append(text)

    # 告警: 三元组率为 0 或异常偏低
    if not texts:
        triple_sync_total.labels(status="skipped").inc()
        if empty_article_count == total_articles and total_articles > 0:
            logger.warning(
                f"⚠️ [ALERT] 所有 {total_articles} 篇文章的三元组均为空 — "
                "graph_extractor 可能失效或 LLM 返回异常"
            )
        else:
            logger.info(f"  (no triples to sync, {empty_article_count}/{total_articles} articles had triples)")
        return

    ratio = empty_article_count / total_articles if total_articles else 0
    if ratio > 0.5:
        logger.warning(
            f"⚠️ [ALERT] 三元组缺失率 {ratio:.0%} ({empty_article_count}/{total_articles}) — "
            "超过 50% 的文章缺少三元组，建议检查 graph_extractor"
        )

    all_embeddings = _batch_embed(texts)
    if not all_embeddings:
        triple_sync_total.labels(status="error").inc()
        logger.error("❌ [ALERT] 三元组嵌入失败，batch_embed 返回空")
        return

    written = store.upsert_triples(records, triples_per_article, all_embeddings)
    triple_sync_total.labels(status="synced").inc()
    logger.info(f"✅ Entity triples sync done: {written} triples from {total_articles} articles "
                f"({empty_article_count} articles had zero triples)")


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
