import os
import re
import json
import time
import dashscope
from dashscope import Generation

from metrics import (
    ai_token_usage_total, ai_token_cost_dollars,
    ai_processing_duration_seconds, ai_rate_limit_hits_total
)

# GLM-5.1 pricing via DashScope ($0.573/M input, $2.58/M output)
AI_MODEL = "glm-5.1"
INPUT_COST_PER_TOKEN = 0.573 / 1_000_000
OUTPUT_COST_PER_TOKEN = 2.58 / 1_000_000

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

dashscope.api_key = os.getenv("DASHSCOPE_KEY")

MAX_RETRIES = 2

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
                messages=[{"role": "user", "content": prompt_text}],
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
                return resp.output.choices[0]["message"]["content"]
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
