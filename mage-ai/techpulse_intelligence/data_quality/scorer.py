"""LLM 对文章质量打分 0-10，借鉴 Horizon 的评分机制"""

import json


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

输出严格 JSON 格式（无额外文本）:
{{"score": 8.5, "breakdown": {{"depth": 2.0, "timeliness": 2.5, "density": 2.0, "originality": 2.0}}, "summary": "一句话评价"}}"""


def score_article(
    title: str,
    source: str,
    summary: str,
    max_tokens: int = 256,
) -> dict:
    """对文章质量打分

    Args:
        title: 文章标题
        source: 来源 (hackernews, reddit, ...)
        summary: AI 生成的摘要
        max_tokens: LLM 最大输出

    Returns:
        {"score": float, "breakdown": {...}, "summary": "..."}
        失败时返回默认评分 5.0
    """
    import dashscope
    resp = dashscope.Generation.call(
        model="qwen-plus",
        messages=[{
            "role": "user",
            "content": SCORING_PROMPT.format(
                title=title,
                source=source,
                summary=(summary or "")[:1500],
            ),
        }],
        max_tokens=max_tokens,
        temperature=0.1,
        result_format='message',
    )
    if resp.status_code != 200:
        return {"score": 5.0, "breakdown": {}, "summary": "评分调用失败"}
    try:
        content = resp.output.choices[0]['message']['content'].strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3]
        return json.loads(content)
    except (json.JSONDecodeError, KeyError):
        return {"score": 5.0, "breakdown": {}, "summary": "解析失败"}
