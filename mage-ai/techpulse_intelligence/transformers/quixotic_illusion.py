import os
import json
import time
import hashlib
import requests
import oss2
import trafilatura
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Any, Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

OSS_ENDPOINT = "https://oss-cn-hongkong.aliyuncs.com"
OSS_BUCKET = "techpulse-data-lake-hk-unique"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TechPulseAI/1.0; "
        "+https://example.com/bot)"
    )
}

def parse_kafka_message(msg: Any) -> Dict:
    if msg is None:
        return {}
    if isinstance(msg, dict):
        return msg
    if isinstance(msg, bytes):
        return json.loads(msg.decode("utf-8"))
    if isinstance(msg, str):
        return json.loads(msg)
    if hasattr(msg, "value"):
        value = msg.value
        if callable(value):
            value = value()
        return parse_kafka_message(value)
    return {}

def get_oss_bucket():
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    if not access_key_id or not access_key_secret:
        raise ValueError("缺少 ALIBABA_CLOUD_ACCESS_KEY_ID / SECRET")
    auth = oss2.Auth(access_key_id, access_key_secret)
    return oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)

def safe_id(url: str, item_id: Any) -> str:
    if item_id:
        return str(item_id)
    return hashlib.md5(url.encode("utf-8")).hexdigest()

def fetch_html(url: str, timeout: int = 10, max_bytes: int = 2_000_000):
    if not url:
        return None, "missing_url"
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=timeout, allow_redirects=True
        )
        if resp.status_code != 200:
            return None, f"http_{resp.status_code}"
        content = resp.content[:max_bytes]
        encoding = resp.encoding or "utf-8"
        try:
            html = content.decode(encoding, errors="ignore")
        except Exception:
            html = content.decode("utf-8", errors="ignore")
        return html, "ok"
    except requests.exceptions.Timeout:
        return None, "timeout"
    except Exception as e:
        return None, f"error_{type(e).__name__}"

def extract_text_from_html(html: str) -> str:
    if not html:
        return ""
    text = trafilatura.extract(
        html, include_comments=False, include_tables=False, favor_precision=True
    )
    if text and len(text.strip()) > 100:
        return text.strip()
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()
    fallback_text = soup.get_text(separator="\n")
    fallback_text = "\n".join(
        line.strip() for line in fallback_text.splitlines() if line.strip()
    )
    return fallback_text[:20000]

def upload_text(bucket, object_name: str, content: str):
    bucket.put_object(
        object_name, content.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"}
    )

def upload_html(bucket, object_name: str, content: str):
    bucket.put_object(
        object_name, content.encode("utf-8"),
        headers={"Content-Type": "text/html; charset=utf-8"}
    )

@transformer
def transform_fetch(messages, *args, **kwargs):
    bucket = get_oss_bucket()
    output = []
    ds = datetime.utcnow().strftime("%Y%m%d")  # 统一日期格式

    for raw_msg in messages:
        msg = parse_kafka_message(raw_msg)
        source = msg.get("source", "hackernews")  # 新增 source 字段
        item_id = msg.get("source_id") or msg.get("id")  # 支持新旧格式
        title = msg.get("title")
        url = msg.get("url")
        author = msg.get("author") or msg.get("by")  # 新格式优先
        ingest_time = msg.get("published_at") or msg.get("time")  # 新格式优先
        score = msg.get("score")
        item_type = msg.get("type")

        record_id = safe_id(url or title or "", item_id)
        html = None
        article_text = ""
        fetch_status = "not_fetched"
        html_oss_path = None
        text_oss_path = None

        if url:
            html, fetch_status = fetch_html(url)
            if html:
                article_text = extract_text_from_html(html)
                html_oss_path = f"raw_html/{source}/ds={ds}/{record_id}.html"
                text_oss_path = f"article_text/{source}/ds={ds}/{record_id}.txt"
                upload_html(bucket, html_oss_path, html)
                upload_text(bucket, text_oss_path, article_text)
                print(f"✅ 抓取成功: {record_id}")
            else:
                print(f"⚠️ 抓取失败: {record_id}, {fetch_status}")
        else:
            print(f"⚠️ 无URL: {record_id}")

        output.append({
            "id": str(item_id) if item_id else record_id,
            "source": source,  # 新增
            "title": title,
            "url": url,
            "author": author,
            "score": score,
            "type": item_type,
            "ingest_time": ingest_time,
            "ds": ds,
            "fetch_status": fetch_status,
            "html_oss_path": html_oss_path,
            "content_oss_path": text_oss_path,
            "content_excerpt": article_text[:8000] if article_text else "",
            "ai_summary": None,
            "ai_insight": None,
            "tech_category": None
        })
        time.sleep(0.2)
    return output