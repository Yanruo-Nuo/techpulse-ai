import os
import re
import json
import time
import dashscope
from dashscope import MultiModalConversation as Generation

from metrics import (
    ai_token_usage_total, ai_token_cost_dollars,
    ai_processing_duration_seconds, ai_rate_limit_hits_total
)
from transformers.chunker import chunk_article

# GLM-5.1 pricing via DashScope ($0.573/M input, $2.58/M output)
AI_MODEL = "qwen3.6-plus"
INPUT_COST_PER_TOKEN = 0.573 / 1_000_000
OUTPUT_COST_PER_TOKEN = 2.58 / 1_000_000

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

dashscope.api_key = os.getenv("DASHSCOPE_KEY")

MAX_RETRIES = 2


def _extract_content(resp) -> str:
    """从 API 响应中提取文本内容，兼容 Generation 和 MultiModalConversation 格式"""
    raw = resp.output.choices[0]["message"]["content"]
    if isinstance(raw, list):
        # MultiModalConversation 格式: [{"text": "..."}, ...]
        return " ".join(item.get("text", "") for item in raw if isinstance(item, dict))
    return str(raw)

CLASSIFY_PROMPT_TEMPLATE = """你是一位资深技术趋势分析师。根据技术新闻标题和正文，生成全面的中文分析。

要求：
1. ai_summary：200-300字的技术摘要，清晰说明文章的核心内容、技术要点和主要结论
2. tech_category：只能选一个——AI/ML, Security, CloudNative, Programming, Hardware, DataEngineering, Others
3. ai_insight：300-500字的深度分析，包括：
   - 该技术的核心创新或突破在哪里
   - 对开发者/行业的影响和意义
   - 相比现有技术的改进或独特之处
   - 潜在的应用场景或未来发展方向
4. 只输出纯净JSON，不要任何其他文字。

{{
  "ai_summary": "详细技术摘要",
  "tech_category": "类别",
  "ai_insight": "深度技术分析"
}}

标题：{title}
正文：{content}"""


def classify_from_text(title: str, content: str):
    truncated = content[:8000] if content else title
    prompt_text = CLASSIFY_PROMPT_TEMPLATE.format(title=title, content=truncated)

    for attempt in range(MAX_RETRIES):
        try:
            _start = time.time()
            resp = Generation.call(
                model=AI_MODEL,
                messages=[{"role": "user", "content": [{"text": prompt_text}]}],
                result_format="message",
                temperature=0.3,
                max_tokens=2048,
            )
            _dur = time.time() - _start
            ai_processing_duration_seconds.labels(operation='classify').observe(_dur)

            if resp.status_code == 200:
                _usage = getattr(resp, 'usage', None)
                if _usage:
                    _input = getattr(_usage, 'input_tokens', 0) or 0
                    _output = getattr(_usage, 'output_tokens', 0) or 0
                    ai_token_usage_total.labels(model=AI_MODEL, operation='classify').inc(_input + _output)
                    _cost = _input * INPUT_COST_PER_TOKEN + _output * OUTPUT_COST_PER_TOKEN
                    ai_token_cost_dollars.labels(model=AI_MODEL).inc(_cost)
                return _extract_content(resp)
            elif resp.status_code == 429:
                ai_rate_limit_hits_total.labels(model=AI_MODEL).inc()
            print(f"⚠️ AI API 错误 (attempt {attempt+1}): {resp.message}")
        except Exception as e:
            print(f"⚠️ AI 调用异常 (attempt {attempt+1}): {type(e).__name__}: {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(1)
    return None


@transformer
def transform_ai(records, *args, **kwargs):
    output = []
    for row in records:
        title = row.get("title", "")
        content = row.get("content_excerpt", "")

        result = classify_from_text(title, content.strip() or title)

        parsed = False
        if result:
            try:
                json_str = re.sub(r'```(?:json)?\s*|\s*```', '', result).strip()
                ai_data = json.loads(json_str)
                row["ai_summary"] = ai_data.get("ai_summary", "").strip()
                row["ai_insight"] = ai_data.get("ai_insight", "").strip()
                row["tech_category"] = ai_data.get("tech_category", "Others").strip()
                parsed = True
                row["_ai_parsed"] = True
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON 解析失败: {e}, raw: {result[:200]}")

        if not parsed:
            print(f"⚠️ 使用规则兜底: {title[:40]}")
            row["ai_summary"] = row.get("ai_summary") or title
            row["ai_insight"] = row.get("ai_insight") or f"来自 Hacker News 的技术文章：{title}"
            row["_ai_parsed"] = False
            text = (title + content).lower()
            if any(x in text for x in ["vulnerability", "security", "漏洞", "安全", "exploit", "malware"]):
                row["tech_category"] = "Security"
            elif any(x in text for x in ["ai", "llm", "大模型", "machine learning", "deep learning", "gpt", "transformer", "neural"]):
                row["tech_category"] = "AI/ML"
            elif any(x in text for x in ["kubernetes", "cloud", "容器", "云原生", "docker", "k8s", "微服务"]):
                row["tech_category"] = "CloudNative"
            elif any(x in text for x in ["python", "rust", "programming", "代码", "开发语言", "compiler", "kernel", "linux"]):
                row["tech_category"] = "Programming"
            elif any(x in text for x in ["硬件", "芯片", "gpu", "cpu", "fpga", "soc"]):
                row["tech_category"] = "Hardware"
            elif any(x in text for x in ["数据", "data", "数据库", "database", "sql", "nosql", "大数据"]):
                row["tech_category"] = "DataEngineering"
            else:
                row["tech_category"] = "Others"

        output.append(row)
    return output


# ═══════════════════════════════════════════════════════
# 多轮 AI 抽取管线（v2）
# 保留上面的 classify_from_text / transform_ai 做 fallback
# ═══════════════════════════════════════════════════════

ROUND1_PROMPT = """你是技术内容分析助手。分析下面这段技术文章的片段，提取关键信息。

要求：只输出纯净JSON，不要任何其他文字。

字段说明：
- tech_entities: 这段文字中提到的具体技术名称列表（如Kafka, PyTorch, dbt）
- tool_mentions: 提到的工具/库/框架列表
- topic: 这段文字的核心主题（一句话，10字以内）
- difficulty: 技术难度（beginner/intermediate/advanced）

{{
  "tech_entities": [],
  "tool_mentions": [],
  "topic": "",
  "difficulty": "intermediate"
}}

文章标题：{title}
片段内容：
{chunk_text}"""

ROUND2_PROMPT = """你是技术内容分析助手。汇总整篇文章的技术要素，分析技术间关系。

要求：只输出纯净JSON，不要任何其他文字。

字段说明：
- use_cases: 这篇文章提到的应用场景列表
- related_tech: 与文章内容相关的其他技术
- key_insights: 文章的核心观点（2-3点）

{{
  "use_cases": [],
  "related_tech": [],
  "key_insights": []
}}

文章标题：{title}

汇总的各片段技术实体和工具：
{round1_summary}

全文范围（前2000字）：
{full_text_preview}"""

ROUND3_PROMPT = """你是技术内容分析助收。整合前两轮分析结果，生成工具推荐和项目关联评估。

要求：只输出纯净JSON，不要任何其他文字。

字段说明：
- summary: 100字以内的中文摘要
- tools_recommended: 推荐的工具列表，每项包含name和scenario
- related_topics: 相关的技术话题列表
- my_project_relevance: 和TechPulse AI项目的关联程度(high/medium/low/review)

{{
  "summary": "",
  "tools_recommended": [{"name": "", "scenario": ""}],
  "related_topics": [],
  "my_project_relevance": "medium"
}}

文章标题：{title}

第一轮（实体提取）结果：
{round1_summary}

第二轮（关联分析）结果：
{round2_result}"""


def _call_llm(prompt_text: str, max_tokens: int, temperature: float,
              operation: str = "extract") -> str | None:
    """通用的 LLM 调用函数，带重试和指标上报"""
    for attempt in range(MAX_RETRIES):
        try:
            _start = time.time()
            resp = Generation.call(
                model=AI_MODEL,
                messages=[{"role": "user", "content": [{"text": prompt_text}]}],
                result_format="message",
                temperature=temperature,
                max_tokens=max_tokens,
            )
            _dur = time.time() - _start
            ai_processing_duration_seconds.labels(operation=operation).observe(_dur)

            if resp.status_code == 200:
                _usage = getattr(resp, 'usage', None)
                if _usage:
                    _input = getattr(_usage, 'input_tokens', 0) or 0
                    _output = getattr(_usage, 'output_tokens', 0) or 0
                    ai_token_usage_total.labels(model=AI_MODEL, operation=operation).inc(
                        _input + _output
                    )
                    _cost = _input * INPUT_COST_PER_TOKEN + _output * OUTPUT_COST_PER_TOKEN
                    ai_token_cost_dollars.labels(model=AI_MODEL).inc(_cost)
                return _extract_content(resp)
            elif resp.status_code == 429:
                ai_rate_limit_hits_total.labels(model=AI_MODEL).inc()
            print(f"⚠️ LLM {operation} 错误 (attempt {attempt+1}): {resp.message}")
        except Exception as e:
            print(f"⚠️ LLM {operation} 异常 (attempt {attempt+1}): {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(1)
    return None


def _parse_json_or_fallback(raw: str | None, fallback: dict) -> dict:
    """安全解析 LLM 返回的 JSON，失败时返回 fallback"""
    if not raw:
        return fallback
    try:
        cleaned = re.sub(r'```(?:json)?\s*|\s*```', '', raw).strip()
        return json.loads(cleaned)
    except json.JSONDecodeError:
        print(f"⚠️ JSON解析失败: {raw[:100]}")
        return fallback


def round1_extract(title: str, chunks: list[dict]) -> list[dict]:
    """第一轮：每个 chunk 独立提取实体"""
    results = []
    for chunk in chunks:
        prompt = ROUND1_PROMPT.format(title=title, chunk_text=chunk["text"])
        raw = _call_llm(prompt, max_tokens=512, temperature=0.1, operation="round1_extract")
        parsed = _parse_json_or_fallback(raw, {
            "tech_entities": [],
            "tool_mentions": [],
            "topic": "",
            "difficulty": "intermediate",
        })
        results.append({
            "block_index": chunk["block_index"],
            **parsed,
        })
    return results


def round2_analyze(title: str, full_text: str, round1_results: list[dict]) -> dict:
    """第二轮：全文章汇总分析"""
    # 汇总 Round 1 结果
    round1_summary_lines = []
    for r in round1_results:
        entities = ", ".join(r.get("tech_entities", [])) or "无"
        tools = ", ".join(r.get("tool_mentions", [])) or "无"
        round1_summary_lines.append(f"  块[{r['block_index']}]: 技术={entities}, 工具={tools}")

    prompt = ROUND2_PROMPT.format(
        title=title,
        round1_summary="\n".join(round1_summary_lines),
        full_text_preview=full_text[:2000],
    )
    raw = _call_llm(prompt, max_tokens=1024, temperature=0.3, operation="round2_analyze")
    return _parse_json_or_fallback(raw, {
        "use_cases": [],
        "related_tech": [],
        "key_insights": [],
    })


def round3_integrate(title: str, round1_results: list[dict],
                     round2_result: dict) -> dict:
    """第三轮：整合推荐"""
    round1_summary_lines = []
    for r in round1_results:
        entities = ", ".join(r.get("tech_entities", [])) or "无"
        tools = ", ".join(r.get("tool_mentions", [])) or "无"
        topic = r.get("topic", "")
        round1_summary_lines.append(f"  块[{r['block_index']}] {topic}: {entities} | 工具: {tools}")

    round2_text = (f"应用场景: {'; '.join(round2_result.get('use_cases', []))}\n"
                   f"相关技术: {'; '.join(round2_result.get('related_tech', []))}\n"
                   f"核心观点: {'; '.join(round2_result.get('key_insights', []))}")

    prompt = ROUND3_PROMPT.format(
        title=title,
        round1_summary="\n".join(round1_summary_lines),
        round2_result=round2_text,
    )
    raw = _call_llm(prompt, max_tokens=512, temperature=0.5, operation="round3_integrate")
    return _parse_json_or_fallback(raw, {
        "summary": "",
        "tools_recommended": [],
        "related_topics": [],
        "my_project_relevance": "medium",
    })


def transform_ai_v2(article: dict) -> dict:
    """v2 增强版 AI 处理：分块 → 3 轮抽取 → 结构化输出

    保留 transform_ai() 作为 kafka_consumer 的 fallback。

    参数：
        article: 单篇文章 dict（需包含 title, content_excerpt, source）

    返回：
        原始 article 的增强版，新增字段：
        - ai_analysis_v2: {round1, round2, round3} 三轮结果
        - ai_chunks: [{text, block_index, metadata}] 分块结果
    """
    title = article.get("title", "")
    content = article.get("content_excerpt", "") or article.get("content", "")
    source = article.get("source", "")

    # Step 1: 分块
    chunks = chunk_article({
        "title": title,
        "content_excerpt": content,
        "source": source,
    })

    if not chunks or not content.strip():
        # 无内容时走原 fallback
        article["ai_analysis_v2"] = None
        article["ai_chunks"] = []
        return article

    # Step 2: Round 1 — 逐块实体提取
    round1_results = round1_extract(title, chunks)

    # Step 3: Round 2 — 全文章关联分析
    round2_result = round2_analyze(title, content, round1_results)

    # Step 4: Round 3 — 整合推荐
    round3_result = round3_integrate(title, round1_results, round2_result)

    # Step 5: 输出
    article["ai_analysis_v2"] = {
        "round1": round1_results,
        "round2": round2_result,
        "round3": round3_result,
    }
    article["ai_chunks"] = [
        {
            "text": c["text"],
            "block_index": c["block_index"],
            "block_type": c["metadata"]["block_type"],
        }
        for c in chunks
    ]

    # Step 6: 知识图谱实体关系提取
    try:
        from transformers.graph_extractor import extract_entities_batch
        article["ai_triples"] = extract_entities_batch(article["ai_chunks"])
    except ImportError:
        article["ai_triples"] = []

    return article
