# 多源爬虫扩展 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将单源 HN 爬虫重构为插件式多源系统（6 scraper + 合规客户端 + 数据流适配）

**Architecture:** `producer/scrapers/base.py` 定义 BaseScraper 抽象类 + ScraperClient 合规 HTTP 客户端。各 scraper 继承 BaseScraper 实现 `fetch()`。`main.py` 通过 `importlib` 反射自动发现所有 scraper，调度循环统一执行、验证、推送 Kafka。Mage transformer 侧按 `source` 字段区分解析逻辑和 OSS 存储路径。

**Tech Stack:** Python 3.9, confluent-kafka, requests, feedparser, beautifulsoup4, lxml

---

## 文件结构

```
producer/
  main.py                        # 调度器（重构）：自动发现 → 循环调度 → 验证 → 推送
  scrapers/
    __init__.py
    base.py                      # Article dataclass, BaseScraper 抽象类, ScraperClient, validate_article, discover_scrapers
    hackernews.py                # HN API scraper（从 main.py 抽取）
    github_trending.py           # GitHub Trending 页面解析
    reddit.py                    # Reddit API（多 subreddit）
    rss_tech.py                  # RSS 聚合（Ars / TechCrunch / The Verge / Wired）
    lobsters.py                  # Lobsters API
    devto.py                     # Dev.to API
  Dockerfile                     # 新增 feedparser 依赖
  requirements.txt               # 新增 feedparser, beautifulsoup4, lxml

techpulse_intelligence/
  transformers/
    quixotic_illusion.py         # 修改：按 source 解析 + OSS 路径加 source 前缀

techpulse_dbt/
  models/
    staging/
      stg_tech_news.sql          # 修改：加 source 字段
    sources.yml                  # 无变更（hn_raw 表兼容新增列）
```

### Task 1: 基础框架 — base.py

**Files:**
- Create: `producer/scrapers/__init__.py`
- Create: `producer/scrapers/base.py`

- [ ] **Step 1: 创建 scrapers 包和 base.py**

```python
# scrapers/__init__.py
from .base import BaseScraper, ScraperClient, Article, validate_article, discover_scrapers
```

```python
# scrapers/base.py
import random
import time
import json
import importlib
import pkgutil
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


@dataclass
class Article:
    source: str
    source_id: str
    title: str
    url: str
    author: Optional[str] = None
    score: Optional[int] = None
    summary: Optional[str] = None
    content_html: Optional[str] = None
    published_at: Optional[str] = None
    external_metadata: dict = field(default_factory=dict)


class ScraperClient:
    """合规 HTTP 客户端：限流、UA 伪装、重试、robots.txt 遵守"""

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = requests.Session()
        self._robots_cache = {}
        self._last_request_time = 0.0
        self._request_count = 0

    def _rotate_ua(self) -> str:
        return random.choice(USER_AGENTS)

    def _rate_limit(self):
        """请求间随机延时 1-5 秒"""
        elapsed = time.time() - self._last_request_time
        min_delay = random.uniform(1, 3)
        if elapsed < min_delay:
            time.sleep(min_delay - elapsed)
        self._last_request_time = time.time()

    def _check_robots(self, url: str) -> bool:
        """缓存并检查 robots.txt"""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base not in self._robots_cache:
            rp = RobotFileParser()
            rp.set_url(f"{base}/robots.txt")
            try:
                rp.read()
                self._robots_cache[base] = rp
            except Exception:
                self._robots_cache[base] = None
                return True
        rp = self._robots_cache[base]
        return rp.can_fetch("*", url) if rp else True

    def get(self, url: str, timeout: int = 10) -> Optional[requests.Response]:
        if not self._check_robots(url):
            logger.warning(f"[{self.source_name}] robots.txt 禁止: {url}")
            return None
        self._rate_limit()
        headers = {"User-Agent": self._rotate_ua()}
        for attempt in range(3):
            try:
                resp = self.session.get(url, headers=headers, timeout=timeout)
                self._request_count += 1
                return resp
            except requests.exceptions.Timeout:
                wait = 2 ** attempt
                logger.warning(f"[{self.source_name}] 超时 (尝试 {attempt + 1}/3): {url}, 等待 {wait}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                logger.error(f"[{self.source_name}] 请求失败: {url}, {e}")
                time.sleep(2)
        return None

    def get_json(self, url: str) -> Optional[dict]:
        resp = self.get(url)
        if resp and resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                return None
        return None

    def get_text(self, url: str) -> Optional[str]:
        resp = self.get(url)
        return resp.text if resp and resp.status_code == 200 else None


class BaseScraper(ABC):
    """所有爬虫的基类"""

    def __init__(self):
        self.client = ScraperClient(self.name)

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def interval(self) -> int:
        ...

    @abstractmethod
    def fetch(self) -> List[Article]:
        ...

    @property
    def max_per_run(self) -> int:
        return 30

    @property
    def enabled(self) -> bool:
        return True


def validate_article(article: Article) -> Optional[Article]:
    """校验文章有效性，剔除无效数据"""
    if not article.title or len(article.title.strip()) < 5:
        return None
    has_url = bool(article.url and article.url.startswith("http"))
    has_content = bool(
        article.content_html
        or (article.summary and len(article.summary.strip()) > 20)
    )
    if not has_url and not has_content:
        return None
    if article.url and not article.url.startswith("http"):
        return None
    return article


def discover_scrapers():
    """自动发现 scrapers 包下所有 BaseScraper 子类"""
    import scrapers as scrapers_pkg
    scrapers_list = []
    for importer, mod_name, is_pkg in pkgutil.iter_modules(scrapers_pkg.__path__):
        if mod_name.startswith("_"):
            continue
        module = importlib.import_module(f"scrapers.{mod_name}")
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, BaseScraper) and attr is not BaseScraper:
                scrapers_list.append(attr())
    return scrapers_list
```

- [ ] **Step 2: 验证语法**

```bash
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/__init__.py
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/base.py
```

---

### Task 2: 各 Scraper 实现

**Files:**
- Create: `producer/scrapers/hackernews.py`
- Create: `producer/scrapers/github_trending.py`
- Create: `producer/scrapers/reddit.py`
- Create: `producer/scrapers/rss_tech.py`
- Create: `producer/scrapers/lobsters.py`
- Create: `producer/scrapers/devto.py`

- [ ] **Step 2a: 创建 hackernews.py**

```python
"""Hacker News API scraper"""
from typing import List
from .base import BaseScraper, Article


class HackerNewsScraper(BaseScraper):
    @property
    def name(self) -> str:
        return "hackernews"

    @property
    def interval(self) -> int:
        return 120  # 2 分钟

    def fetch(self) -> List[Article]:
        top_ids = self.client.get_json("https://hacker-news.firebaseio.com/v0/topstories.json")
        if not top_ids:
            return []
        articles = []
        for sid in top_ids[:self.max_per_run]:
            item = self.client.get_json(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json")
            if item and item.get("title"):
                articles.append(Article(
                    source=self.name,
                    source_id=str(sid),
                    title=item.get("title", ""),
                    url=item.get("url") or f"https://news.ycombinator.com/item?id={sid}",
                    author=item.get("by"),
                    score=item.get("score"),
                    published_at=str(item.get("time")) if item.get("time") else None,
                ))
        return articles
```

- [ ] **Step 2b: 创建 github_trending.py**

```python
"""GitHub Trending scraper（页面解析）"""
from typing import List
from bs4 import BeautifulSoup
from .base import BaseScraper, Article


class GithubTrendingScraper(BaseScraper):
    @property
    def name(self) -> str:
        return "github_trending"

    @property
    def interval(self) -> int:
        return 86400  # 每天 1 次

    @property
    def max_per_run(self) -> int:
        return 25

    def fetch(self) -> List[Article]:
        html = self.client.get_text("https://github.com/trending")
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        articles = []
        for repo in soup.select("article.Box-row"):
            h2 = repo.select_one("h2 a")
            if not h2:
                continue
            full_name = h2.get("href", "").strip("/")
            desc_el = repo.select_one("p")
            stars_el = repo.select_one(".d-inline-block.float-sm-right")
            url = f"https://github.com/{full_name}"
            title = full_name
            summary = desc_el.get_text(strip=True) if desc_el else ""
            score_text = stars_el.get_text(strip=True) if stars_el else "0"
            try:
                score = int(score_text.replace(",", ""))
            except ValueError:
                score = 0
            articles.append(Article(
                source=self.name,
                source_id=full_name,
                title=title,
                url=url,
                summary=summary,
                score=score,
            ))
            if len(articles) >= self.max_per_run:
                break
        return articles
```

- [ ] **Step 2c: 创建 reddit.py**

```python
"""Reddit API scraper（多 subreddit）"""
from typing import List
from .base import BaseScraper, Article

SUBREDDITS = ["programming", "MachineLearning", "rust", "devops", "cybersecurity"]


class RedditScraper(BaseScraper):
    @property
    def name(self) -> str:
        return "reddit"

    @property
    def interval(self) -> int:
        return 180  # 3 分钟

    @property
    def max_per_run(self) -> int:
        return 50

    def fetch(self) -> List[Article]:
        articles = []
        for sub in SUBREDDITS:
            data = self.client.get_json(f"https://www.reddit.com/r/{sub}/hot.json?limit=10")
            if not data:
                continue
            for child in data.get("data", {}).get("children", []):
                d = child.get("data", {})
                title = d.get("title", "")
                url = d.get("url") or f"https://www.reddit.com{d.get('permalink', '')}"
                summary = d.get("selftext", "")[:500]
                articles.append(Article(
                    source=self.name,
                    source_id=f"reddit_{d.get('id', '')}",
                    title=title,
                    url=url,
                    author=d.get("author"),
                    score=d.get("score"),
                    summary=summary,
                    published_at=str(d.get("created_utc")),
                ))
                if len(articles) >= self.max_per_run:
                    return articles
        return articles
```

- [ ] **Step 2d: 创建 rss_tech.py**

```python
"""科技媒体 RSS scraper"""
from typing import List
import feedparser
from .base import BaseScraper, Article

RSS_FEEDS = {
    "ars": "https://feeds.arstechnica.com/arstechnica/index",
    "techcrunch": "https://techcrunch.com/feed/",
    "theverge": "https://www.theverge.com/rss/index.xml",
    "wired": "https://www.wired.com/feed/rss",
}


class RsstechScraper(BaseScraper):
    @property
    def name(self) -> str:
        return "rss_tech"

    @property
    def interval(self) -> int:
        return 600  # 10 分钟

    @property
    def max_per_run(self) -> int:
        return 40

    def fetch(self) -> List[Article]:
        articles = []
        for feed_key, feed_url in RSS_FEEDS.items():
            resp = self.client.get(feed_url)
            if not resp or resp.status_code != 200:
                continue
            feed = feedparser.parse(resp.text)
            for entry in feed.entries[:10]:
                title = entry.get("title", "")
                url = entry.get("link", "")
                summary = entry.get("summary", "")[:500] if entry.get("summary") else ""
                author = None
                if entry.get("author"):
                    author = entry["author"]
                elif entry.get("authors"):
                    author = entry["authors"][0].get("name")
                articles.append(Article(
                    source=f"{self.name}_{feed_key}",
                    source_id=entry.get("id", url),
                    title=title,
                    url=url,
                    author=author,
                    summary=summary,
                    published_at=entry.get("published", ""),
                ))
                if len(articles) >= self.max_per_run:
                    return articles
        return articles
```

- [ ] **Step 2e: 创建 lobsters.py**

```python
"""Lobsters API scraper"""
from typing import List
from .base import BaseScraper, Article


class LobstersScraper(BaseScraper):
    @property
    def name(self) -> str:
        return "lobsters"

    @property
    def interval(self) -> int:
        return 180  # 3 分钟

    def fetch(self) -> List[Article]:
        data = self.client.get_json("https://lobste.rs/hottest.json")
        if not data:
            return []
        articles = []
        for item in data[:self.max_per_run]:
            articles.append(Article(
                source=self.name,
                source_id=str(item.get("short_id", "")),
                title=item.get("title", ""),
                url=item.get("url") or f"https://lobste.rs/s/{item.get('short_id', '')}",
                author=item.get("submitter_user", {}).get("username"),
                score=item.get("score"),
                summary=item.get("description", ""),
            ))
        return articles
```

- [ ] **Step 2f: 创建 devto.py**

```python
"""Dev.to API scraper"""
from typing import List
from .base import BaseScraper, Article


class DevtoScraper(BaseScraper):
    @property
    def name(self) -> str:
        return "devto"

    @property
    def interval(self) -> int:
        return 600  # 10 分钟

    def fetch(self) -> List[Article]:
        data = self.client.get_json("https://dev.to/api/articles?top=1&per_page=30")
        if not data:
            return []
        articles = []
        for item in data[:self.max_per_run]:
            articles.append(Article(
                source=self.name,
                source_id=str(item.get("id", "")),
                title=item.get("title", ""),
                url=item.get("url", ""),
                author=item.get("user", {}).get("name") if item.get("user") else None,
                score=item.get("positive_reactions_count"),
                summary=item.get("description", ""),
                published_at=item.get("published_at", ""),
                external_metadata={"tags": item.get("tag_list", [])},
            ))
        return articles
```

- [ ] **Step 2g: 验证所有 scraper 语法**

```bash
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/hackernews.py
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/github_trending.py
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/reddit.py
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/rss_tech.py
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/lobsters.py
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/scrapers/devto.py
```

---

### Task 3: Producer 调度器 — main.py

**Files:**
- Modify: `producer/main.py`

- [ ] **Step 3: 重写 main.py 为调度器**

```python
"""多源爬虫调度器：自动发现 scrapers → 循环调度 → 验证 → 推送 Kafka"""

import json
import time
import logging
from confluent_kafka import Producer
from scrapers import discover_scrapers, validate_article
from dataclasses import asdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC = "raw_tech_feeds"

KAFKA_CONF = {
    "bootstrap.servers": "kafka:9092",
    "client.id": "techpulse-crawler-hk",
}


def delivery_report(err, msg):
    if err:
        logger.error(f"推送失败: {err}")
    else:
        logger.info(f"成功推送 {msg.key().decode('utf-8')}")


def run():
    p = None
    while p is None:
        try:
            p = Producer(KAFKA_CONF)
            logger.info("Kafka Producer 初始化成功")
        except Exception as e:
            logger.warning(f"等待 Kafka 就绪... {e}")
            time.sleep(5)

    scrapers = discover_scrapers()
    logger.info(f"发现 {len(scrapers)} 个 scraper: {[s.name for s in scrapers]}")

    failure_counts = {s.name: 0 for s in scrapers}
    last_run = {s.name: 0 for s in scrapers}

    while True:
        now = time.time()
        for scraper in scrapers:
            if not scraper.enabled:
                continue
            if now - last_run[scraper.name] < scraper.interval:
                continue

            try:
                articles = scraper.fetch()
                validated = [a for a in (validate_article(a) for a in articles) if a]
                for article in validated:
                    p.produce(
                        TOPIC,
                        key=article.source_id,
                        value=json.dumps(asdict(article)),
                        callback=delivery_report,
                    )
                p.flush()
                logger.info(f"[{scraper.name}] 推送 {len(validated)}/{len(articles)} 篇")
                failure_counts[scraper.name] = 0
            except Exception as e:
                logger.error(f"[{scraper.name}] 错误: {e}")
                failure_counts[scraper.name] += 1
                if failure_counts[scraper.name] >= 5:
                    logger.warning(f"[{scraper.name}] 连续失败 5 次，暂停 1 小时")
                    last_run[scraper.name] = now + 3600
                    failure_counts[scraper.name] = 0
                    continue

            last_run[scraper.name] = now

        time.sleep(30)


if __name__ == "__main__":
    run()
```

- [ ] **Step 3b: 验证语法**

```bash
python3 -m py_compile /root/techpulse-ai/mage-ai/producer/main.py
```

---

### Task 4: Dockerfile + requirements

**Files:**
- Modify: `producer/requirements.txt`
- Modify: `producer/Dockerfile`

- [ ] **Step 4a: 更新 requirements.txt**

```
requests
confluent-kafka
feedparser
beautifulsoup4
lxml
```

- [ ] **Step 4b: 更新 Dockerfile（无需变更，build-essential + librdkafka-dev 已安装，pip 安装新增依赖即可）**

```bash
# Dockerfile 不变，requirements.txt 新增依赖后自动 pip install
```

---

### Task 5: Transformer 适配多源

**Files:**
- Modify: `techpulse_intelligence/transformers/quixotic_illusion.py`

- [ ] **Step 5: 修改 quixotic_illusion.py**

核心变更点：
1. 解析 `msg.source` 字段区分来源
2. OSS 路径从 `raw_html/hn/...` 改为 `raw_html/{source}/...`
3. `source` 字段加入输出 DataFrame

```python
# 修改 upload_text / upload_html 调用处的 OSS 路径
# 变化：从消息中获取 source 字段，替换 hn 前缀

# 在 parse_kafka_message 后增加 source 提取
source = msg.get("source", "unknown")

# OSS 路径改为
html_oss_path = f"raw_html/{source}/ds={ds}/{record_id}.html"
text_oss_path = f"article_text/{source}/ds={ds}/{record_id}.txt"

# 输出增加 source 字段
output.append({
    "source": source,
    "id": ...,
    ...
})
```

精确变更：
- 第 132-133 行 OSS 路径：`f"raw_html/hn/..."` → `f"raw_html/{source}/..."`
- 第 142 行输出：增加 `"source": source` 字段

---

### Task 6: dbt 模型适配

**Files:**
- Modify: `techpulse_dbt/models/staging/stg_tech_news.sql`

- [ ] **Step 6: stg_tech_news.sql 加 source 字段**

```sql
SELECT
    id,
    source,          -- 新增
    title,
    url,
    ...
FROM {{ source('techpulse_dw', 'hn_raw') }}
WHERE ds = '{{ var("ds") }}'
```

---

### Task 7: 前端适配

**Files:**
- Modify: `frontend/pages/timeline.py`
- Modify: `frontend/maxcompute.py`

- [ ] **Step 7a: maxcompute.py load_news_data() 加 source 字段**

```sql
-- SELECT 中增加 source 列
SELECT id, title, url, score, ai_summary, ai_insight,
       tech_category, source, ingest_time, ds
FROM (
    SELECT id, title, url, score, ai_summary, ai_insight,
           tech_category, source, ingest_time, ds,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingest_time DESC) AS rn
    FROM hn_raw
    WHERE ds IS NOT NULL
) t
WHERE rn = 1
ORDER BY ingest_time DESC
```

- [ ] **Step 7b: timeline.py 增加来源标签 + 来源过滤**

```python
# 在 render_news_card 的 HTML 标签行旁边加一个来源标签
# 在 news-card 的分类 tag 后面加：
<span class="tag" style="background:#E2E8F0;color:#475569;">{source}</span>

# 在过滤栏增加来源多选
sources = filtered_df['source'].unique().tolist()
selected_sources = st.multiselect("按来源过滤", options=["全部"] + sources, default=["全部"])
if "全部" not in selected_sources and selected_sources:
    filtered_df = filtered_df[filtered_df['source'].isin(selected_sources)]
```

---

### Task 8: 构建部署与验证

- [ ] **Step 8a: 构建新 producer 镜像**

```bash
cd /root/techpulse-ai/mage-ai && docker compose up -d --build news-crawler
```

- [ ] **Step 8b: 验证 producer 启动**

```bash
docker logs tech-crawler --tail 20
# 应看到: "发现 6 个 scraper: [hackernews, github_trending, reddit, rss_tech, lobsters, devto]"
```

- [ ] **Step 8c: 构建前端（含 source 字段变更）**

```bash
docker compose up -d --build tech-frontend
```

- [ ] **Step 8d: 验证 Kafka 消息含 source 字段**

```bash
docker exec kafka-kraft kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_tech_feeds --max-messages 3 | python3 -m json.tool
# 每条消息应包含 "source": "hackernews" 等
```
