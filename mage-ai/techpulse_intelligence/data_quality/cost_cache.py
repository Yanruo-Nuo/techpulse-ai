"""LLM 调用结果缓存 + 成本统计

用途:
  1. 缓存: 相同 prompt 不用重复调 LLM，减少延迟和费用
  2. 跟踪: 统计每个操作 (classify/score/extract/analyze) 的调用次数和费用
  3. 报告: 提供 cost_report() 用于监控和告警
"""

import hashlib
import threading
from collections import defaultdict
from typing import Any, Callable


class CostTracker:
    """全局 LLM 成本跟踪单例"""

    _instance: "CostTracker | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "CostTracker":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._cache: dict[str, Any] = {}
                    cls._instance._stats: dict[str, dict[str, float]] = defaultdict(
                        lambda: {"calls": 0, "tokens": 0, "cost_cny": 0.0}
                    )
        return cls._instance

    def cache_key(self, model: str, prompt: str) -> str:
        """基于 model + prompt 生成缓存键"""
        return hashlib.md5(f"{model}:{prompt}".encode()).hexdigest()

    def get_or_compute(
        self,
        model: str,
        prompt: str,
        compute_fn: Callable[[], Any],
    ) -> Any:
        """如果缓存命中，直接返回；否则调用 compute_fn 并缓存"""
        key = self.cache_key(model, prompt)
        if key in self._cache:
            return self._cache[key]
        result = compute_fn()
        self._cache[key] = result
        return result

    def track_call(self, operation: str, tokens: int, model: str = "qwen-plus"):
        """记录一次 LLM 调用"""
        self._stats[operation]["calls"] += 1
        self._stats[operation]["tokens"] += tokens
        # 参考价格 (2026): GLM-5.1 ≈ 0.005 元/1K tokens
        self._stats[operation]["cost_cny"] += tokens * 0.005 / 1000

    def report(self) -> dict[str, dict[str, Any]]:
        """返回成本报告"""
        return {
            op: {
                "calls": int(s["calls"]),
                "tokens": int(s["tokens"]),
                "cost_cny": round(s["cost_cny"], 4),
            }
            for op, s in self._stats.items()
        }

    def total_cost(self) -> float:
        """总费用（元）"""
        return round(sum(s["cost_cny"] for s in self._stats.values()), 4)

    def reset(self):
        """重置统计（不清缓存）"""
        self._stats.clear()


# 全局单例
cost_tracker = CostTracker()
