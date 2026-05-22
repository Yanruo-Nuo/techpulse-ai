"""从文章分块中提取 (实体, 关系, 实体) 三元组，构建知识图谱

参考 Microsoft GraphRAG 的 extract_graph 逻辑，简化为单轮 LLM 调用。
"""

import json
from typing import Any
from mage_ai.data_preparation.shared.secrets import get_secret_value

# 延迟导入，避免 Mage AI 上下文加载问题
_get_cache = None

def _get_llm():
    """懒初始化 LLM 调用函数"""
    global _get_cache
    if _get_cache is not None:
        return _get_cache
    import dashscope
    _get_cache = dashscope.Generation
    return _get_cache


ENTITY_PROMPT = """从以下技术新闻段落中，提取出所有的 (主体, 关系, 客体) 三元组。

主体和客体可以是:
- 技术实体: 编程语言(Rust,Go,Python)、框架(tokio,React)、工具(Kubernetes,Docker)
- 公司/组织: OpenAI, Google, Microsoft, Meta
- 概念/术语: 微服务, Serverless, WebAssembly, LLM, RAG

关系类型: 使用(uses), 替代(replaces), 依赖(depends_on), 对比(compared_to), 发布(releases), 收购(acquires), 集成(integrates_with)

输入段落:
{text}

输出严格 JSON 数组格式，每个元素格式为 {{"subject": "...", "predicate": "...", "object": "..."}}，不要任何额外文本或解释。
如果段落中没有明确的技术实体关系，返回空数组 []。"""


def extract_entities(chunk_text: str, max_tokens: int = 256) -> list[dict[str, str]]:
    """对单个 chunk 提取实体关系三元组

    Args:
        chunk_text: 段落文本（≤800 字符）
        max_tokens: LLM 最大输出 token 数

    Returns:
        [(subject, predicate, object), ...] 三元组列表，失败时返回 []
    """
    if not chunk_text or not chunk_text.strip():
        return []

    Gen = _get_llm()
    try:
        resp = Gen.call(
            model="glm-5.1",
            messages=[{
                "role": "user",
                "content": ENTITY_PROMPT.format(text=chunk_text[:1000])
            }],
            max_tokens=max_tokens,
            temperature=0.1,
            result_format='message',
        )
        if resp.status_code != 200:
            return []
        content = resp.output.choices[0]['message']['content']
        # 清理 LLM 可能输出的 markdown 代码块包裹
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3]
        return json.loads(content)
    except (json.JSONDecodeError, KeyError, Exception):
        return []


def extract_entities_batch(
    chunks: list[dict[str, Any]],
    skip_existing: bool = True,
) -> list[dict[str, str]]:
    """对多个 chunk 批量提取，去重合并

    Args:
        chunks: chunk 列表，每项需有 'text' 字段
        skip_existing: 是否跳过 'ai_entities' 字段已有的 chunk

    Returns:
        去重后的三元组列表
    """
    all_triples: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    for chunk in chunks:
        if skip_existing and chunk.get("ai_entities"):
            for t in chunk["ai_entities"]:
                key = (t["subject"], t["predicate"], t["object"])
                if key not in seen:
                    seen.add(key)
                    all_triples.append(t)
            continue

        text = chunk.get("text", "")
        triples = extract_entities(text)
        chunk["ai_entities"] = triples  # 回写，下次可复用
        for t in triples:
            key = (t["subject"], t["predicate"], t["object"])
            if key not in seen:
                seen.add(key)
                all_triples.append(t)

    return all_triples
