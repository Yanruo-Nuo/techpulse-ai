"""向量存储层 — 替代当前 Python cosine_similarity 暴力扫描

VectorStore 封装 Qdrant 客户端，提供：
  - upsert_batch: 批量写入向量 + payload
  - search: HNSW 近似语义检索（O(log n)）
  - count: 向量库中文档数
"""

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
import logging

logger = logging.getLogger(__name__)

COLLECTION_NAME = "tech_news"
VECTOR_DIM = 1536  # DashScope text-embedding-v2


class VectorStore:
    def __init__(self, host="qdrant", port=6333):
        self.client = QdrantClient(host=host, port=port)
        self._ensure_collection()

    def _ensure_collection(self):
        """幂等创建 collection，多次启动不报错"""
        collections = self.client.get_collections().collections
        names = [c.name for c in collections]
        if COLLECTION_NAME not in names:
            self.client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
            )
            logger.info(f"Created Qdrant collection: {COLLECTION_NAME}")

    def upsert_batch(self, records: list[dict], embeddings: list[list[float]]):
        """批量写入向量 + payload（title, category, summary, source, score, url）"""
        points = [
            PointStruct(
                id=abs(hash(rec.get("id", rec.get("title", "")))) % (2**63),
                vector=emb,
                payload={
                    "title": rec.get("title", ""),
                    "tech_category": rec.get("tech_category", "Others"),
                    "summary": rec.get("ai_summary", "")[:500],
                    "source": rec.get("source", ""),
                    "score": rec.get("score", 0),
                    "url": rec.get("url", ""),
                    "pub_date": rec.get("ds", ""),
                },
            )
            for rec, emb in zip(records, embeddings)
        ]
        self.client.upsert(collection_name=COLLECTION_NAME, wait=True, points=points)

    def search(
        self, query_embedding: list[float], top_k: int = 5,
        category_filter: str | None = None,
    ) -> list[dict]:
        """语义检索 — O(log n) HNSW，替代原 O(n·d) 全表扫描

        Args:
            query_embedding: 查询向量 (1536-dim)
            top_k: 返回 top-k 条结果
            category_filter: 可选，按 tech_category 过滤（如 "AI/ML"）

        Returns:
            list[dict]: 包含 title, score, category, summary, source, url
        """
        filter_cond = None
        if category_filter:
            filter_cond = Filter(
                must=[FieldCondition(key="tech_category", match=MatchValue(value=category_filter))]
            )

        hits = self.client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_embedding,
            limit=top_k,
            query_filter=filter_cond,
        )

        return [
            {
                "title": h.payload["title"],
                "score": h.score,
                "category": h.payload.get("tech_category", ""),
                "summary": h.payload.get("summary", ""),
                "source": h.payload.get("source", ""),
                "url": h.payload.get("url", ""),
            }
            for h in hits
        ]

    def upsert_chunks(self, article_records: list[dict], chunk_embeddings: list[list[float]]) -> int:
        """块级 upsert：一篇文章对应多个 chunk，每个 chunk 独立向量 + payload

        Args:
            article_records: 文章元数据列表（title, source, url, ai_chunks 等）
            chunk_embeddings: 展平后的所有 chunk embedding 列表

        Returns:
            int: 写入的向量总数
        """
        points = []
        idx = 0
        for article in article_records:
            chunks = article.get("ai_chunks", [])
            if not chunks:
                # 无分块时写入一条空向量（保持文章可检索）
                points.append(
                    PointStruct(
                        id=abs(hash(article.get("id", article.get("title", "")))) % (2**63),
                        vector=chunk_embeddings[idx] if idx < len(chunk_embeddings) else [0.0] * VECTOR_DIM,
                        payload={
                            "title": article.get("title", ""),
                            "source": article.get("source", ""),
                            "url": article.get("url", ""),
                            "block_index": -1,
                            "block_type": "article",
                            "block_preview": (article.get("ai_summary") or article.get("title", ""))[:500],
                            "type": "article",
                        },
                    )
                )
                idx += 1
                continue

            for chunk in chunks:
                emb = chunk_embeddings[idx] if idx < len(chunk_embeddings) else [0.0] * VECTOR_DIM
                points.append(
                    PointStruct(
                        id=abs(hash(f"{article.get('id', article.get('title', ''))}_{chunk['block_index']}")) % (2**63),
                        vector=emb,
                        payload={
                            "title": article.get("title", ""),
                            "source": article.get("source", ""),
                            "url": article.get("url", ""),
                            "block_index": chunk.get("block_index", -1),
                            "block_type": chunk.get("block_type", "paragraph"),
                            "block_preview": chunk.get("text", "")[:500],
                            "type": "block",
                        },
                    )
                )
                idx += 1

        if points:
            self.client.upsert(collection_name=COLLECTION_NAME, wait=True, points=points)
        return len(points)

    def search_blocks(
        self, query_embedding: list[float], top_k: int = 5,
    ) -> list[dict]:
        """块级语义检索 — 返回匹配的具体段落

        与 search() 不同：
          - 不按文章过滤，直接按块相似度排序
          - 返回结果包含 block_index 和 block_preview
          - 同一篇文章可能多次出现（多个匹配段落）
        """
        hits = self.client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_embedding,
            limit=top_k,
        )
        return [
            {
                "title": h.payload.get("title", ""),
                "score": h.score,
                "source": h.payload.get("source", ""),
                "url": h.payload.get("url", ""),
                "block_index": h.payload.get("block_index", -1),
                "block_type": h.payload.get("block_type", ""),
                "block_preview": h.payload.get("block_preview", ""),
            }
            for h in hits
        ]

    def count(self) -> int:
        """返回向量库中文档总数"""
        return self.client.get_collection(COLLECTION_NAME).points_count
