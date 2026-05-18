# AI 数据工程增强 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 Kafka 消息丢失 bug + 加 AI 输出质量校验 + 引入 Qdrant 向量检索

**Architecture:** P0 修 consumer commit 时序 + 死信队列 → P1 注入 AI 输出 5 维度校验 + Prometheus 告警 → P2 替换 O(n) Python cosine 为 Qdrant HNSW

**Tech Stack:** Python, Kafka, DashScope GLM-5.1, Qdrant, Prometheus, Grafana, Docker Compose

**设计文档:** [docs/superpowers/specs/2026-05-09-ai-data-engineering-enhancement-design.md](../specs/2026-05-09-ai-data-engineering-enhancement-design.md)

---

## P0: Kafka 消息丢失修复

### Task 1: 新增死信队列

**Files:**
- Create: `techpulse_intelligence/data_quality/__init__.py`
- Create: `techpulse_intelligence/data_quality/dead_letter.py`

- [ ] **Step 1: 创建 `__init__.py`**

```python
# techpulse_intelligence/data_quality/__init__.py
"""AI 数据质量模块 — 校验 + 死信队列"""
```

- [ ] **Step 2: 创建 `dead_letter.py`**

```python
# techpulse_intelligence/data_quality/dead_letter.py
"""死信队列：批量失败 3 次后写入本地 JSONL 日志"""
import json
import time
import os
import hashlib
import logging

logger = logging.getLogger(__name__)

DLQ_PATH = "logs/dead_letter.jsonl"
MAX_RETRIES = 3


class DeadLetterQueue:
    def __init__(self, max_retries=MAX_RETRIES):
        self.max_retries = max_retries
        self._attempts = {}  # batch_hash → retry count
        os.makedirs("logs", exist_ok=True)

    def _batch_hash(self, batch):
        titles = "|".join(r.get("title", "") for r in batch)
        return hashlib.md5(titles.encode()).hexdigest()

    def should_deadletter(self, batch) -> bool:
        key = self._batch_hash(batch)
        self._attempts[key] = self._attempts.get(key, 0) + 1
        return self._attempts[key] >= self.max_retries

    def write(self, batch, reason):
        with open(DLQ_PATH, "a") as f:
            f.write(
                json.dumps(
                    {
                        "ts": time.time(),
                        "reason": reason,
                        "records": [
                            {"title": r.get("title", "?"), "source": r.get("source", "?")}
                            for r in batch
                        ],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    def reset(self, batch):
        """成功后重置重试计数"""
        self._attempts.pop(self._batch_hash(batch), None)
```

- [ ] **Step 3: 验证文件语法**

Run: `python3 -c "import sys; sys.path.insert(0, 'techpulse_intelligence'); from data_quality.dead_letter import DeadLetterQueue; dlq = DeadLetterQueue(); print('OK:', dlq.max_retries)"`
Expected: `OK: 3`

---

### Task 2: 修复 kafka_consumer.py — commit 时序 + 死信

**Files:**
- Modify: `techpulse_intelligence/kafka_consumer.py:1-128`

- [ ] **Step 1: 修改 import 部分 — 新增死信队列和校验导入**

```python
# techpulse_intelligence/kafka_consumer.py — import 部分
"""Standalone Kafka consumer pipeline: raw_tech_feeds → OSS → MaxCompute"""

import os, sys
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(__file__))
from transformers.quixotic_illusion import transform_fetch
from transformers.billowing_hill import transform_ai
from data_exporters.insightful_resonance import UnifiedSink

from kafka import KafkaConsumer

from prometheus_client import start_http_server
from metrics import (
    oss_write_total, oss_write_bytes, oss_write_duration_seconds,
    kafka_consume_lag
)
from data_quality.dead_letter import DeadLetterQueue
from data_quality.validator import validate_batch, report_metrics

BATCH_SIZE = 10
POLL_TIMEOUT_MS = 30000
```

- [ ] **Step 2: 修改 `run()` — 指数退避**

```python
def run():
    start_http_server(8002)
    logger.info("Metrics HTTP server started on port 8002")

    consumer = KafkaConsumer(
        "raw_tech_feeds",
        bootstrap_servers="kafka:9092",
        group_id="techpulse-mage-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        max_poll_records=500,
        session_timeout_ms=180000,
        max_poll_interval_ms=360000,
        heartbeat_interval_ms=30000,
        consumer_timeout_ms=60000,
    )
    logger.info("Connected to kafka:9092, subscribed to raw_tech_feeds")

    sink = UnifiedSink()
    sink.init_client()
    logger.info("OSS sink initialized")

    retry_attempt = 0
    while True:
        try:
            _run_loop(consumer, sink)
            retry_attempt = 0
        except Exception as e:
            retry_attempt += 1
            backoff = min(30 * (2 ** retry_attempt), 300)
            logger.error(f"Consumer error: {e}, reconnecting in {backoff}s (attempt {retry_attempt})", exc_info=True)
            time.sleep(backoff)
```

- [ ] **Step 3: 修改 `_run_loop()` — commit 移入 try 块 + 死信**

Replace the ENTIRE `_run_loop` function:

```python
def _run_loop(consumer, sink):
    buffer = []
    dlq = DeadLetterQueue(max_retries=3)

    while True:
        msg_pack = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

        for tp, records in msg_pack.items():
            try:
                high = consumer.highwater(tp)
                pos = consumer.position(tp)
                if high is not None:
                    kafka_consume_lag.labels(partition=str(tp.partition)).set(high - pos)
            except Exception:
                pass

            for msg in records:
                # --- JSON 解析失败直接死信 ---
                try:
                    value = json.loads(msg.value.decode("utf-8"))
                    buffer.append(value)
                    logger.info(
                        f"Buffered [{len(buffer)}]: "
                        f"{value.get('source', '?')} / {value.get('title', '?')[:50]}"
                    )
                except json.JSONDecodeError as je:
                    logger.error(f"JSON parse error, dead-lettering: {je}")
                    dlq.write([{"title": "?"}], f"JSONDecodeError: {je}")
                    consumer.commit()
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error: {e}", exc_info=True)
                    continue

                # --- Pipeline 处理 ---
                if len(buffer) >= BATCH_SIZE:
                    batch = buffer
                    buffer = []
                    logger.info(f"Processing batch of {len(batch)} messages")

                    try:
                        fetch_result = transform_fetch(batch)
                        ai_result = transform_ai(fetch_result)

                        # AI 输出质量校验（P1 功能，此处可先跳过）
                        dq_checks = validate_batch(ai_result)
                        report_metrics(dq_checks)

                        _oss_start = time.time()
                        sink.batch_write(ai_result)
                        _oss_dur = time.time() - _oss_start
                        oss_write_duration_seconds.labels(target='hn_raw').observe(_oss_dur)
                        oss_write_total.labels(target='hn_raw', status='success').inc()
                        logger.info(f"Batch done: {len(ai_result)} records written")

                        # ✅ 成功后才 commit
                        consumer.commit()
                        dlq.reset(batch)

                    except Exception as e:
                        logger.error(f"Pipeline error: {e}", exc_info=True)
                        oss_write_total.labels(target='hn_raw', status='failure').inc()

                        # 死信判断：同批失败 3 次则跳过
                        if dlq.should_deadletter(batch):
                            dlq.write(batch, str(e))
                            consumer.commit()  # 跳过，避免无限重试阻塞
                            logger.warning(f"Dead-lettered {len(batch)} records after {dlq.max_retries} retries")
                        # else: 不 commit，下次 poll 重新消费

        # --- Flush 积压 ---
        if not msg_pack and buffer:
            logger.info(f"Flushing {len(buffer)} buffered messages")
            batch = buffer
            buffer = []

            try:
                fetch_result = transform_fetch(batch)
                ai_result = transform_ai(fetch_result)

                dq_checks = validate_batch(ai_result)
                report_metrics(dq_checks)

                _oss_start = time.time()
                sink.batch_write(ai_result)
                _oss_dur = time.time() - _oss_start
                oss_write_duration_seconds.labels(target='hn_raw').observe(_oss_dur)
                oss_write_total.labels(target='hn_raw', status='success').inc()
                logger.info(f"Flush done: {len(ai_result)} records written")

                consumer.commit()
                dlq.reset(batch)

            except Exception as e:
                logger.error(f"Flush error: {e}", exc_info=True)
                oss_write_total.labels(target='hn_raw', status='failure').inc()

                if dlq.should_deadletter(batch):
                    dlq.write(batch, str(e))
                    consumer.commit()
```

- [ ] **Step 4: 运行语法检查**

Run: `python3 -c "import py_compile; py_compile.compile('techpulse_intelligence/kafka_consumer.py', doraise=True); print('OK')"`
Expected: `OK`

---

### Task 3: 提交 P0

- [ ] **Step 1: Commit**

```bash
git add techpulse_intelligence/data_quality/ techpulse_intelligence/kafka_consumer.py
git commit -m "fix(pipeline): commit after success, add dead-letter queue and exponential backoff"
```

---

## P1: AI 输出质量校验

### Task 4: 新增数据质量校验器

**Files:**
- Create: `techpulse_intelligence/data_quality/validator.py`

- [ ] **Step 1: 创建 `validator.py`**

```python
# techpulse_intelligence/data_quality/validator.py
"""AI 输出 5 维度质量校验 + Prometheus Gauge 上报"""
from prometheus_client import Gauge

# ==== Prometheus Gauges ====
dq_summary_missing = Gauge("dq_ai_summary_missing_ratio", "AI summary missing ratio")
dq_category_missing = Gauge("dq_ai_category_missing_ratio", "AI category missing ratio")
dq_others_ratio = Gauge("dq_others_category_ratio", "Others category ratio")
dq_json_fail = Gauge("dq_json_parse_fail_ratio", "JSON parse fail ratio")
dq_hallucination = Gauge("dq_ai_hallucination_ratio", "AI hallucination ratio")

# ==== 允许的 7 个合法分类 ====
VALID_CATEGORIES = {"AI/ML", "Security", "CloudNative", "Programming", "Hardware", "DataEngineering", "Others"}

# ==== 幻觉拒绝语模式 ====
HALLUCINATION_PATTERNS = [
    "作为AI", "作为一个AI", "无法获取", "无法访问", "抱歉",
    "I cannot", "I'm sorry", "As an AI", "我不具备", "我没有能力",
]


def validate_batch(records: list[dict]) -> dict:
    """对一批 AI 处理后的 records 做 5 维度质量校验，返回各维度比率"""
    total = len(records) or 1

    # 维度 1：摘要缺失率
    summary_missing = sum(1 for r in records if not r.get("ai_summary"))

    # 维度 2：分类非法率
    category_missing = sum(1 for r in records if r.get("tech_category") not in VALID_CATEGORIES)

    # 维度 3：Others 占比（>40% 说明 AI 分类模型异常）
    others_ratio = sum(1 for r in records if r.get("tech_category") == "Others")

    # 维度 4：JSON 解析失败率
    json_fail = sum(1 for r in records if not r.get("_ai_parsed", True))

    # 维度 5：幻觉检测率
    def _is_hallucination(r):
        text = (r.get("ai_summary") or "") + (r.get("ai_insight") or "")
        return any(p in text for p in HALLUCINATION_PATTERNS)

    hallucination = sum(1 for r in records if _is_hallucination(r))

    return {
        "summary_missing": summary_missing / total,
        "category_missing": category_missing / total,
        "others_ratio": others_ratio / total,
        "json_fail": json_fail / total,
        "hallucination": hallucination / total,
    }


def report_metrics(checks: dict):
    """将校验结果推送到 Prometheus Gauges"""
    dq_summary_missing.set(checks["summary_missing"])
    dq_category_missing.set(checks["category_missing"])
    dq_others_ratio.set(checks["others_ratio"])
    dq_json_fail.set(checks["json_fail"])
    dq_hallucination.set(checks["hallucination"])
```

- [ ] **Step 2: 验证 validator 可导入**

Run: `python3 -c "import sys; sys.path.insert(0, 'techpulse_intelligence'); from data_quality.validator import validate_batch, report_metrics; print('OK:', validate_batch([{'ai_summary':'x','tech_category':'AI/ML','_ai_parsed':True}]))"`
Expected: `OK: {'summary_missing': 0.0, 'category_missing': 0.0, 'others_ratio': 0.0, 'json_fail': 0.0, 'hallucination': 0.0}`

---

### Task 5: 修改 billowing_hill.py — 标记解析状态

**Files:**
- Modify: `techpulse_intelligence/transformers/billowing_hill.py`

- [ ] **Step 1: 在 transform_ai() 中标记 _ai_parsed**

在 `transform_ai` 函数中，`parsed = True` 后面加一行，`not parsed` 的分支也加一行。以 `parsed = True` 处为例：

```python
# 在 billowing_hill.py 的 transform_ai() 中：
# 找到 parsed = True 的位置，改为：
parsed = True
row["_ai_parsed"] = True   # ← 新增：标记 AI 解析成功

# 找到 not parsed 分支末尾，加：
row["_ai_parsed"] = False  # ← 新增：标记 AI 解析失败
```

具体位置：`billowing_hill.py:92` (`parsed = True`) 后，和 `billowing_hill.py:113`（兜底分支末尾）。

---

### Task 6: 在 kafka_consumer.py 中插入校验（已在 P0 Task 2 完成）

已通过 `from data_quality.validator import validate_batch, report_metrics` 和
`validate_batch(ai_result)` + `report_metrics(dq_checks)` 完成集成。

- [ ] **Step 1: 确认导入和调用已存在**

Run: `grep -n "validate_batch\|report_metrics\|from data_quality.validator" techpulse_intelligence/kafka_consumer.py`
Expected: 至少有 3 处匹配

---

### Task 7: 新增 Grafana 告警规则

**Files:**
- Modify: `grafana/alerting/alert-rules.yml`

- [ ] **Step 1: 在现有 rules 列表末尾插入幻觉检测和 JSON 失败规则**

在最后一个 rule（`techpulse_kafka_lag_high`）之后添加：

```yaml
      - uid: techpulse_ai_hallucination
        title: "AI 幻觉率高"
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 600
              to: 0
            datasourceUid: PBFA97CFB590B2093
            model:
              expr: dq_ai_hallucination_ratio > 0.1
        noData: NoData
        execErr: Error
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "AI 幻觉率超过 10% — 可能有大量劣质数据入库"

      - uid: techpulse_json_parse_fail
        title: "AI JSON 解析失败率高"
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 600
              to: 0
            datasourceUid: PBFA97CFB590B2093
            model:
              expr: dq_json_parse_fail_ratio > 0.2
        noData: NoData
        execErr: Error
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "AI JSON 解析失败率超过 20% — GLM-5.1 输出可能异常"
```

---

### Task 8: 提交 P1

- [ ] **Step 1: Commit**

```bash
git add techpulse_intelligence/data_quality/validator.py \
        techpulse_intelligence/transformers/billowing_hill.py \
        grafana/alerting/alert-rules.yml
git commit -m "feat(dq): 5-dimension AI output quality validator with Prometheus gauges"
```

---

## P2: Qdrant 向量检索替换 Python cosine

### Task 9: docker-compose.yml 新增 Qdrant

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: 在 services 末尾加 qdrant 服务**

```yaml
  # Qdrant 向量数据库 — 替代 Python cosine_similarity O(n) 暴力扫描
  qdrant:
    image: qdrant/qdrant:latest
    container_name: qdrant
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage
    restart: always
```

- [ ] **Step 2: 在 volumes 末尾加 qdrant_data**

```yaml
volumes:
  kafka_data:
    driver: local
  prometheus_data:
    driver: local
  grafana_data:
    driver: local
  qdrant_data:
    driver: local
```

- [ ] **Step 3: 启动 Qdrant 验证**

Run: `docker compose up -d qdrant && docker ps --filter name=qdrant`
Expected: qdrant container is `Up`

---

### Task 10: 新增 VectorStore

**Files:**
- Create: `frontend/vector_store.py`

- [ ] **Step 1: 创建 vector_store.py**

```python
"""向量存储层 — 替代当前 Python cosine_similarity 暴力扫描

VectorStore 封装 Qdrant 客户端，提供：
  - upsert_batch: 批量写入向量 + payload
  - search: HNSW 近似语义检索（O(log n)）
  - count: 向量库文档数
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
        """幂等创建 collection"""
        collections = self.client.get_collections().collections
        names = [c.name for c in collections]
        if COLLECTION_NAME not in names:
            self.client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
            )
            logger.info(f"Created Qdrant collection: {COLLECTION_NAME}")

    def upsert_batch(self, records: list[dict], embeddings: list[list[float]]):
        """批量写入向量 + payload"""
        points = [
            PointStruct(
                id=hash(rec.get("id", rec.get("title", ""))),
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
        """语义检索 — O(log n) HNSW"""
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

    def count(self) -> int:
        return self.client.get_collection(COLLECTION_NAME).points_count
```

---

### Task 11: 新增向量同步脚本

**Files:**
- Create: `frontend/vector_sync.py`

- [ ] **Step 1: 创建 vector_sync.py**

```python
#!/usr/bin/env python3
"""向量同步脚本：MaxCompute → DashScope embedding → Qdrant

首次运行：全量同步所有文章向量到 Qdrant
后续运行：可加 --incremental 做增量（按 ds 过滤）

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
```

---

### Task 12: 改造 assistant.py — 替换 cosine_similarity 为 Qdrant

**Files:**
- Modify: `frontend/panels/assistant.py`

- [ ] **Step 1: 新增 import**

在文件顶部 import 区添加：

```python
from vector_store import VectorStore
```

- [ ] **Step 2: 全局单例**

在 `import` 区后、`cosine_similarity` 定义前添加：

```python
# 全局单例 — 首次导入时自动连接 Qdrant 并确保 collection 存在
vector_store = VectorStore()
```

- [ ] **Step 3: 修改 `get_rag_response()` 函数**

替换 `get_rag_response` 函数体中从 vectors 生成到 top_indices 的部分（约 50-90 行）：

```python
def get_rag_response(query, df, df_trend=None, messages=None):
    if df.empty:
        return "知识库暂无数据，请先同步数据。"

    # 1. 向量化用户问题
    q_emb_resp = TextEmbedding.call(model=TextEmbedding.Models.text_embedding_v2, input=query)
    if q_emb_resp.status_code != 200:
        return f"向量生成失败：{q_emb_resp.message}"
    q_embedding = q_emb_resp.output['embeddings'][0]['embedding']

    # 2. Qdrant 语义检索 — O(log n)，<5ms
    top_results = vector_store.search(q_embedding, top_k=5)
    context = "\n\n".join(
        f"【{r['category']}】{r['title']}\n{r['summary']}"
        for r in top_results
    ) if top_results else "暂无相关文章"

    # 3. 分类分布 + 热门排行（保留原有逻辑）
    cat_counts = df['tech_category'].value_counts()
    dist_text = "、".join(f"{k} {v}篇" for k, v in cat_counts.items())

    hot_text = "（无热度数据）"
    if 'score' in df.columns:
        hot_df = df.dropna(subset=['score']).copy()
        hot_df['score'] = pd.to_numeric(hot_df['score'], errors='coerce').fillna(0)
        hot_df = hot_df.nlargest(10, 'score')
        hot_list = [f"  🔥 {r['title'][:60]} | 热度 {int(r['score'])} | {r['tech_category']}" for _, r in hot_df.iterrows()]
        hot_text = "\n".join(hot_list)

    # 4. 趋势概览
    trend_overview = ""
    if df_trend is not None and not df_trend.empty:
        latest_date = df_trend['ds'].max()
        trend_overview = f"数据截止至 {latest_date}"

    # 5. 历史消息上下文
    history_text = ""
    if messages:
        history_text = "对话历史：\n" + "\n".join(
            f"{'用户' if m['role']=='user' else '助手'}：{m['content'][:200]}"
            for m in messages[-6:]
        ) + "\n\n"

    # 6. 构建 LLM prompt
    prompt = f"""你是一个技术新闻分析助手。根据以下由向量检索到的最相关文章来回答用户问题。

📊 数据库概况：{dist_text} | {trend_overview}

🔥 近期热门：
{hot_text}

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
        model="glm-5.1",
        messages=[{"role": "user", "content": prompt}],
        result_format='message'
    )
    return gen_resp.output.choices[0]['message']['content'] if gen_resp.status_code == 200 else f"大模型错误：{gen_resp.message}"
```

- [ ] **Step 4: 移除不再使用的 `cosine_similarity` 函数和 `compute_news_embeddings` 函数**

删除 `cosine_similarity` 和 `compute_news_embeddings` 两个函数定义。`st.cache_data` 也一并移除。

---

### Task 13: 首次向量同步 + 端到端验证

- [ ] **Step 1: 运行向量同步**

Run: `docker exec tech-frontend python /app/vector_sync.py`
Expected: 日志显示全量同步完成

- [ ] **Step 2: 验证 Qdrant 数据**

Run: `curl -s http://localhost:6333/collections/tech_news | python3 -m json.tool | head -10`
Expected: collection 存在且有 points_count > 0

- [ ] **Step 3: 访问前端验证 RAG 生效**

打开 `http://localhost:8501` → AI 助手页 → 提问。预期：回答引用具体文章标题，响应时间无明显退化。

---

### Task 14: 提交 P2

- [ ] **Step 1: Commit**

```bash
git add docker-compose.yml \
        frontend/vector_store.py \
        frontend/vector_sync.py \
        frontend/panels/assistant.py
git commit -m "feat(vector): replace Python O(n) cosine with Qdrant HNSW semantic search"
```

---

## 完成检查清单

- [ ] `docker-compose.yml` 包含 qdrant 服务
- [ ] `kafka_consumer.py` 的 commit() 在 try 块内
- [ ] `dead_letter.py` 可导入，`DeadLetterQueue` 类可用
- [ ] `validator.py` 可导入，`validate_batch` / `report_metrics` 可用
- [ ] Prometheus 端口 (9090) 可看到 `dq_ai_*` 指标
- [ ] Grafana (3000) AI Token 面板 + AI 数据质量面板正常
- [ ] `vector_store.py` + `vector_sync.py` 可运行
- [ ] 前端 AI 助手回答引用 Qdrant 检索结果
- [ ] 无回归错误
