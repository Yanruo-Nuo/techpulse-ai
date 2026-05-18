# TechPulse AI — AI 数据工程增强设计 v2

> 目标：将当前"单次 AI 摘要调用"升级为"多轮 AI 知识抽取引擎"
> 保留管道层不变，只改造 AI 处理 + 检索系统

---

## 一、现状问题

当前 AI 管线流程：

```
爬虫 → 取前 8000 字符 → GLM-5.1(单次调用) → {分类, 摘要, 洞察} → 存 parquet
```

三个根本问题：

| 问题 | 表现 | 后果 |
|------|------|------|
| **不分块** | 整篇文章一个 embedding | 用户搜"Kafka partition 策略"无法匹配到长文中的具体段落 |
| **单轮抽取** | 一次 GLM 调用输出所有内容 | 分类/摘要/洞察互相争抢 token 预算，深度不够 |
| **无结构化实体提取** | 只产出三段自由文本 | 没法回答"哪些文章提到了 RisingWave"这种具体问题 |

---

## 二、改动方案

### 2.1 整体链路对比

```
当前：
  article → truncate 8000 → GLM-5.1 → JSON dump → OSS

改后：
  article → 来源识别 → 文档解析 → 分块(512 tokens, overlap 64)
    ├─ 块 → LLM Round 1: 实体提取(技术名/工具/场景)
    │        → embedding → Qdrant(块级)
    ├─ 块 → LLM Round 2: 关联分析(技术关系/同类对比)
    │        → embedding → Qdrant(块级)
    └─ 全部块汇总 → LLM Round 3: 整合推荐(工具推荐/项目关联)
                     → 结构化输出 → OSS + Qdrant
```

### 2.2 三大改动

#### 改动 1：文档解析 + 分块

**`chunker.py`（新增）**

```python
def chunk_article(article: dict) -> list[dict]:
    """
    输入：文章 dict (title, content, source, url, ...)
    输出：chunk list，每个 chunk 包含：
      - text: 分块文本（512 tokens）
      - block_index: 块序号（0, 1, 2...）
      - metadata: 来源/标题/URL/块类型
    """
```

分块策略因来源而异：

| 来源 | 策略 | 理由 |
|------|------|------|
| HN / Lobsters / Dev.to | 按段落分割 (split("\n\n")) | 技术新闻每段独立，分段不影响阅读 |
| Reddit | 正文 + 高赞评论 | 评论中常有有价值的技术讨论 |
| GitHub Trending | README 前 3000 字 + 代码结构 | README 包含项目描述，代码结构告诉用户这个项目做什么 |
| RSS | 全文 fetch → 按段落分割 | 外站文章长短不一 |

#### 改动 2：多轮 AI 抽取

**`billowing_hill.py` 改造**

```
当前：classify_from_text() — 单次 GLM-5.1 调用单次返回

改后：三阶段管线

Round 1 (每个 chunk 独立调用):
  输入：[cls.{tech_entities, tool_mentions, topic, difficulty}]
  输出: {tech_entities: ["Kafka", "Qdrant"],
        tool_mentions: ["dbt", "prometheus"],
        topic: "分布式系统",
        difficulty: "intermediate"}
  prompt 控制: max_tokens=512, temperature=0.1（低温度，要求准确）

Round 2 (全文章汇总调用):
  输入: 所有 chunk 的 Round 1 结果 + 全文前 3000 字
  输出: {use_cases: ["实时流处理", "监控告警"],
        related_tech: ["Flink", "RisingWave"],
        actionability: "可以直接用"}
  prompt 控制: max_tokens=1024, temperature=0.3（中度温度，允许一定创造性）

Round 3 (整合推荐):
  输入: Round 1 + Round 2 结果
  输出: {summary (100字以内),
        tools_recommended: [{name: "RisingWave", scenario: "实时流计算"}],
        related_topics: ["流处理 vs 批处理"],
        my_project_relevance: "直接相关可实践"}
  prompt 控制: max_tokens=512, temperature=0.5（较高温度，鼓励联想）
```

**成本估算：**

| 阶段 | tokens/轮 | 轮次 | 每篇文章成本 | 日 500 篇文章成本 |
|------|----------|------|------------|----------------|
| 当前 | 4000 input + 500 output | 1 | ~$0.0036 | ~$1.8 |
| 改造后 | ~3000 input + 500 output | 3 | ~$0.0058 | ~$2.9 |

成本增加约 60%，但在可控范围内（日均不到 $3）。

#### 改动 3：块级 Qdrant 向量检索

**`vector_store.py` 改造**

```python
# 当前：文章级 embedding
payload = {title, summary, source}
points = [PointStruct(id=article_id, vector=article_embedding, payload=payload)]

# 改后：块级 embedding，每个块独立成 point
payload = {title, source, block_index, block_type, block_text_preview(50字)}
points = [PointStruct(id=f"{article_id}_{i}", vector=chunk_embedding, payload=payload)]
```

**`assistant.py` RAG prompt 改造**

```python
# 当前：prompt 包含整篇文章摘要（精度低）
context = "\n\n".join(f"【{r['category']}】{r['title']}\n{r['summary']}"

# 改后：prompt 只包含最相关的 2-3 个块（精度高）
context = "\n\n".join(f"【来源：{r['source']} 第{r['block_index']}块】\n{r['block_preview']}")
# → 用户搜"Kafka partition 设置"能直接看到讲 partition 的段落
```

---

## 三、改动文件清单

| 文件 | 改动类型 | 改动内容 |
|------|---------|---------|
| 新增 `chunker.py` | CREATE | 文档解析 + 分块策略，按来源类型路由 |
| `billowing_hill.py` | MODIFY | `classify_from_text()` → 3 轮管线 `round1_extract()`, `round2_analyze()`, `round3_integrate()` |
| `vector_store.py` | MODIFY | 支持块级 upsert（payload 加 `block_index`），支持块级 search |
| `vector_sync.py` | MODIFY | 处理逻辑从"文章级"改为"块级"：分块 → 逐块 embedding → 逐块 upsert |
| `assistant.py` | MODIFY | RAG prompt 从整篇改为块级：`{block_preview}` 替代 `{summary}` |
| `kafka_consumer.py` | MODIFY | `transform_ai` 调用链调整：处理后每条 record 变为多行 block records |

## 四、不改的部分

```
docker-compose.yml         — 不碰
producer/ (爬虫)           — 不碰
kafka 配置                — 不碰
OSS 路径结构              — 不碰
MaxCompute + dbt 建模      — 不碰
Prometheus / Grafana       — 不碰
Streamlit 页面结构         — 不碰
metrics.py (已有指标)      — 不碰
data_quality/validator.py  — 不碰
data_quality/dead_letter.py — 不碰
```

## 五、实施计划

| 步骤 | 内容 | 预计 |
|------|------|------|
| 1 | 新增 `chunker.py`：来源识别 + 分块策略 | 1 天 |
| 2 | 改造 `billowing_hill.py`：3 轮 pipeline，保留原函数做 fallback | 1.5 天 |
| 3 | 升级 `vector_store.py` + `vector_sync.py`：块级 embedding 和检索 | 1 天 |
| 4 | 调整 `assistant.py` RAG prompt + `kafka_consumer.py` 适配 | 0.5 天 |
| 5 | 端到端验证：分块 → AI 抽取 → Qdrant 写入 → 前端检索 | 1 天 |
| **总计** | | **5 天** |

---

## 六、效果预期

| 维度 | 当前 | 改造后 |
|------|------|--------|
| 检索粒度 | 文章级 | **块级**（段落级） |
| 实体提取 | 无 | **技术名/工具/场景/难度** |
| 关联分析 | 无 | **技术关系/同类对比/项目关联度** |
| 工具推荐 | 无 | **结构化推荐（场景搭配）** |
| 单篇文章成本 | ~$0.0036 | ~$0.0058（+60%）|
| 周成本（500篇/天）| ~$12.6 | ~$20.3 |
