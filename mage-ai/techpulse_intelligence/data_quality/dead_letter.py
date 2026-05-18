"""死信队列：批量失败 3 次后写入本地 JSONL 日志

指标：
  - dlq_records_total{reason="pipeline_error|json_decode"} — 死信累计条数
  - dlq_pending_total — 当前待重试 batch 数
"""
import json
import time
import os
import hashlib
import logging
from prometheus_client import Counter, Gauge

logger = logging.getLogger(__name__)

DLQ_PATH = "logs/dead_letter.jsonl"
MAX_RETRIES = 3

# ==== Prometheus 指标 ====
dlq_records_total = Counter(
    'dlq_records_total', 'Dead letter records written',
    ['reason']
)
dlq_pending_total = Gauge(
    'dlq_pending_total', 'Batches currently pending retry'
)

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
        dlq_pending_total.set(len(self._attempts))
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
        # 上报 Prometheus
        dlq_records_total.labels(reason=reason[:32]).inc(len(batch))

    def reset(self, batch):
        """成功后重置重试计数"""
        self._attempts.pop(self._batch_hash(batch), None)
        # 上报待重试数
        dlq_pending_total.set(len(self._attempts))
