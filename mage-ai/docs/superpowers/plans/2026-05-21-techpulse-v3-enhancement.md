# TechPulse AI v3 — 全链路增强计划

> 创建: 2026-05-21 | 目标: 补 3 个能力缺口，每个借鉴自开源竞品的最佳实践

---

## 一、当前局限复盘

| # | 局限 | 影响 | 借鉴来源 |
|---|------|------|---------|
| ② | AI 提取无质量评估 | 不知道实体提取准确率 | Horizon LLM 打分 |
| ③ | 没有跨文章知识图谱 | 回答不了"A 和 B 的关系" | Microsoft GraphRAG / LightRAG |
| ⑥ | 去重缺失 | 125k 行 / 425 篇 = 同一篇存 296 次 | Horizon |
| ⑨ | AI 助手是薄壳 | 一次搜索→回答，无多步推理 | RAGFlow agentic RAG |
| ⑧ | LLM 成本不可控 | 三轮调用无缓存、无降级 | 自建成本监控 |

---

## 二、三道改造总览

```
Phase A: 知识图谱 ──→ 实体关系三元组 → Leiden 社区检测 → 概念回答
Phase B: 内容质量 ──→ LLM 打分(0-10) → 去重 → 成本缓存
Phase C: Agentic RAG ──→ 多步推理 → 工具调用 → 追问
```

---

## Phase A: 知识图谱管道

### A1: 实体关系抽取

**创建文件:** `mage-ai/techpulse_intelligence/transformers/graph_extractor.py`

```python
"""
从文章分块中提取 (实体, 关系, 实体) 三元组。
参考 Microsoft GraphRAG 的 extract_graph 逻辑，简化版。

输入: chunk text（≤800 字符）
输出: [(subject, predicate, object), ...]
"""

import json
from dashscope import Generation

ENTITY_PROMPT = """从以下技术新闻段落中，提取出所有的 (主体, 关系, 客体) 三元组。

主体和客体可以是:
- 技术实体: 编程语言(Rust,Go,Python)、框架(tokio,React)、工具(Kubernetes,Docker)
- 公司/组织: OpenAI, Google, Microsoft
- 概念/术语: 微服务, Serverless, WebAssembly

关系类型: 使用(uses), 替代(replaces), 依赖(depends_on), 对比(compared_to), 发布(releases), 收购(acquires)

输入段落:
{text}

输出 JSON 数组格式，不要任何额外文本:
[
  {{"subject": "实体A", "predicate": "关系", "object": "实体B"}},
  ...
]"""

def extract_entities(chunk_text: str) -> list[dict]:
    """对单个 chunk 提取实体关系"""
    resp = Generation.call(
        model="glm-5.1",
        messages=[{"role": "user", "content": ENTITY_PROMPT.format(text=chunk_text)}],
        result_format='message'
    )
    if resp.status_code != 200:
        return []
    try:
        return json.loads(resp.output.choices[0]['message']['content'])
    except json.JSONDecodeError:
        return []
```

### A2: 融入主管线

**修改文件:** `mage-ai/techpulse_intelligence/transformers/billowing_hill.py`

在 `transform_ai_v2` 中，对每个 chunk 提取实体关系：

```python
# 在 chunker 之后（第 278 行附近），插入:
from .graph_extractor import extract_entities

# 对每个 article 的所有 chunks 批量提取
all_triples = []
for chunk in chunks:
    triples = extract_entities(chunk["text"])
    all_triples.extend(triples)

article["ai_triples"] = all_triples   # 新增字段
```

### A3: 存储三元组

**修改文件:** `mage-ai/frontend/vector_sync.py`

在 `_sync_block_level` 中新增三元组处理：

```python
# 在 upsert_chunks 之后
_store_triples(store, records)

def _store_triples(store: VectorStore, records: list[dict]):
    """将三元组中的 subject/object 作为独立实体存入 Qdrant"""
    for r in records:
        for t in r.get("ai_triples", []):
            # 将实体名向量化存入新 collection (tech_entities)
            pass  # 简化实现: 跳过独立实体存储，直接在 prompt 中使用
```

### A4: AI 助手集成

**修改文件:** `mage-ai/frontend/panels/assistant.py`

在 `get_rag_response` 的 prompt 中新增实体关系上下文：

```python
# 在 prompt_template 中新增
entity_graph_lines = []
for article in df_news.to_dict('records')[:]:
    for t in article.get('ai_triples', []):
        entity_graph_lines.append(f"  {t['subject']} --{t['predicate']}--> {t['object']}")

entity_graph = "\n".join(entity_graph_lines[:30])  # 最多 30 条关系

# prompt 新增:
"""
🔗 知识图谱（实体间关系）：
{entity_graph}
"""
```

---

## Phase B: 内容质量管道

### B1: LLM 打分

**创建文件:** `mage-ai/techpulse_intelligence/data_quality/scorer.py`

```python
"""LLM 对文章质量打分 0-10，借鉴 Horizon 的评分机制"""

from dashscope import Generation

SCORING_PROMPT = """你是一个技术内容评审专家。根据以下文章信息，给出 0-10 的综合评分。

评分维度（各 0-2.5 分）:
- 技术深度: 是否深入探讨技术原理或实现细节
- 时效性: 是否涉及当前热门或新兴技术
- 信息密度: 是否有具体的代码、数据或引用
- 原创性: 是原创观点还是转载/汇总

文章信息:
标题: {title}
来源: {source}
摘要: {summary}

输出严格 JSON 格式:
{{"score": 8.5, "breakdown": {{"depth": 2.0, "timeliness": 2.5, "density": 2.0, "originality": 2.0}}, "summary": "一句话评价"}}"""

def score_article(title: str, source: str, summary: str) -> dict:
    resp = Generation.call(
        model="glm-5.1",
        messages=[{"role": "user", "content": SCORING_PROMPT.format(
            title=title, source=source, summary=summary
        )}],
        result_format='message'
    )
    if resp.status_code != 200:
        return {"score": 5.0, "breakdown": {}}
    try:
        import json
        return json.loads(resp.output.choices[0]['message']['content'])
    except:
        return {"score": 5.0, "breakdown": {}}
```

### B2: 融入主管线

**修改文件:** `mage-ai/techpulse_intelligence/transformers/billowing_hill.py`

在 AI 分类完成之后、写入 OSS 之前：

```python
from data_quality.scorer import score_article

scoring = score_article(
    title=article["title"],
    source=article["source"],
    summary=article.get("ai_summary", "")
)
article["quality_score"] = scoring["score"]
article["quality_breakdown"] = scoring.get("breakdown", {})
```

### B3: 去重引擎

**创建文件:** `mage-ai/techpulse_intelligence/data_quality/dedup.py`

```python
"""标题相似度去重 + URL 归一化"""

import hashlib
from urllib.parse import urlparse

def normalize_url(url: str) -> str:
    """去除 URL 中的 tracking 参数"""
    parsed = urlparse(url)
    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return hashlib.md5(clean.encode()).hexdigest()[:16]

def compute_similarity(title_a: str, title_b: str) -> float:
    """简化的 Jaccard 相似度"""
    set_a = set(title_a.lower().split())
    set_b = set(title_b.lower().split())
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)

def is_duplicate(new_title: str, new_url: str, existing: list[dict], threshold: float = 0.7) -> bool:
    """检查新文章是否与已有文章重复"""
    new_hash = normalize_url(new_url)
    for article in existing:
        if normalize_url(article.get("url", "")) == new_hash:
            return True
        if compute_similarity(new_title, article.get("title", "")) > threshold:
            return True
    return False
```

### B4: 成本缓存

**创建文件:** `mage-ai/techpulse_intelligence/data_quality/cost_cache.py`

```python
"""LLM 调用结果缓存 + 成本统计"""

import hashlib
import json
import time
from collections import defaultdict

class CostTracker:
    def __init__(self):
        self._cache = {}
        self._stats = defaultdict(lambda: {"calls": 0, "tokens": 0, "cost_cny": 0.0})

    def cache_key(self, model: str, prompt: str) -> str:
        return hashlib.md5(f"{model}:{prompt}".encode()).hexdigest()

    def get_or_compute(self, model: str, prompt: str, compute_fn):
        """如果缓存命中，直接返回；否则调用 compute_fn 并缓存"""
        key = self.cache_key(model, prompt)
        if key in self._cache:
            return self._cache[key]
        result = compute_fn()
        self._cache[key] = result
        return result

    def track_call(self, operation: str, tokens: int):
        self._stats[operation]["calls"] += 1
        self._stats[operation]["tokens"] += tokens
        # GLM-5.1 价格 (参考): 0.005 元 / 1K tokens
        self._stats[operation]["cost_cny"] += tokens * 0.005 / 1000

    def report(self) -> dict:
        return {op: dict(stats) for op, stats in self._stats.items()}

# 全局单例
cost_tracker = CostTracker()
```

### B5: MaxCompute 表 + dbt 字段

**修改文件:** `mage-ai/techpulse_dbt/models/staging/stg_tech_news.sql`

新增字段（如果上游有的话，空值默认）：

```sql
-- 新增字段（保持向后兼容）:
-- quality_score: DOUBLE
-- quality_breakdown: STRING (JSON)
```

### B6: Grafana 质量监控

**修改文件:** `mage-ai/grafana/dashboards/monitoring-dashboard.json`

新增 panel：

```json
{
  "title": "文章质量分布",
  "targets": [
    {
      "expr": "avg by (source) (article_quality_score)",
      "legendFormat": "{{source}}"
    }
  ]
}
```

### B7: 质量指标暴露

**修改文件:** `mage-ai/frontend/metrics_collector.py`（如果存在）或创建

```python
"""暴露文章质量指标到 Prometheus"""
import time
from prometheus_client import Gauge, Counter

article_quality_score = Gauge('article_quality_score', 'Article quality score', ['source'])
dedup_count = Counter('article_dedup_total', 'Deduplicated articles count', ['source'])
extraction_errors = Counter('extraction_errors_total', 'AI extraction errors', ['step'])
```

---

## Phase C: Agentic RAG 多步推理

### C1: 搜索工具封装

**创建文件:** `mage-ai/frontend/agent_tools.py`

```python
"""Agent 可调用的工具集"""

def search_knowledge_base(query: str, vector_store, top_k: int = 5) -> str:
    """工具 1: 在知识库中搜索相关段落"""
    # embedding → Qdrant search → 返回格式化结果
    pass

def get_trend_data(dt: str) -> str:
    """工具 2: 获取趋势数据"""
    pass

def get_entity_graph(topic: str) -> str:
    """工具 3: 获取某个主题的实体关系图"""
    pass
```

### C2: 多步推理循环

**修改文件:** `mage-ai/frontend/panels/assistant.py`

```python
def agentic_rag(query: str, vector_store, max_steps: int = 3) -> str:
    """
    多步推理 RAG:
    1. 首轮: 搜知识库 → 看结果
    2. 判断: 信息够不够？不够 → 换关键词再搜
    3. 终轮: 综合所有搜索结果 → 回答
    """
    collected_results = []
    for step in range(max_steps):
        if step == 0:
            search_query = query
        else:
            # LLM 判断需要补充什么信息
            follow_up = generate_follow_up_query(query, collected_results)
            search_query = follow_up

        results = vector_store.search_blocks(
            embed_query(search_query), top_k=3
        )
        collected_results.extend(results)

        if len(collected_results) >= 5:  # 收集到足够信息
            break

    return generate_final_answer(query, collected_results)
```

---

## 三、文件变更清单

| 文件 | 操作 | Phase |
|------|------|-------|
| `techpulse_intelligence/transformers/graph_extractor.py` | 新建 | A |
| `techpulse_intelligence/transformers/billowing_hill.py` | 修改 | A + B |
| `frontend/vector_sync.py` | 修改 | A |
| `frontend/panels/assistant.py` | 修改 | A + C |
| `techpulse_intelligence/data_quality/scorer.py` | 新建 | B |
| `techpulse_intelligence/data_quality/dedup.py` | 新建 | B |
| `techpulse_intelligence/data_quality/cost_cache.py` | 新建 | B |
| `techpulse_dbt/models/staging/stg_tech_news.sql` | 修改 | B |
| `grafana/dashboards/monitoring-dashboard.json` | 修改 | B |
| `frontend/metrics_collector.py` | 新建 | B |
| `frontend/agent_tools.py` | 新建 | C |

---

## 四、执行顺序

```
Step 1: A1+A2 实体关系提取 → 跑一次验证准确率
Step 2: A4 AI 助手集成 → 验证图谱回答效果
Step 3: B1+B2 LLM 打分 → 验证分数分布
Step 4: B3 去重 → 验证去重率
Step 5: B4 成本缓存 → 跑 100 篇文章测成本
Step 6: B5 dbt 字段 → 跑 dbt run 验证
Step 7: B6+B7 Grafana 监控 → 验证面板显示
Step 8: C1+C2 Agentic RAG → 验证多步推理
Step 9: 全链路集成测试
```

---

## 五、风险与缓解

| 风险 | 等级 | 缓解 |
|------|------|------|
| GLM-5.1 成本飙升（每篇文章多 3 次调用） | 高 | B4 成本缓存 + B5 降级开关 |
| 实体提取准确率低 | 中 | 先用简化 prompt 跑 50 篇测准确率，不达标就降级为关键词规则 |
| 去重误杀（相似但不同文章被删除） | 低 | 阈值可调，默认 0.7，可降到 0.85 |
| Agentic RAG 延迟高（3 轮搜索） | 中 | 加并行搜索 + 结果缓存 |
