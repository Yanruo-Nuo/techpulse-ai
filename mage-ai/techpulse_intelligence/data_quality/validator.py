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
