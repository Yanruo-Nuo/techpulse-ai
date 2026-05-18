# TechPulse AI — 多源爬虫扩展设计

> **Goal:** 将单源（Hacker News）爬虫扩展为插件式多源爬虫系统，新增 5 个英文技术源，使用统一合规爬虫策略，数据标注来源，文章必须真实可读可跳转。

---

## 1. 架构总览

```
producer/
  main.py                 # 调度器：自动发现 scrapers，循环执行
  scrapers/
    __init__.py
    base.py               # BaseScraper 抽象类 + ScraperClient 合规 HTTP 客户端
    hackernews.py          # Hacker News API
    github_trending.py     # GitHub Trending (页面解析)
    reddit.py              # Reddit r/programming, r/MachineLearning 等
    rss_tech.py            # Ars Technica / TechCrunch / The Verge / Wired RSS
    lobsters.py            # Lobsters API
    devto.py               # Dev.to API
```

所有 scraper 通过 `importlib` 反射自动发现。新增源 = 在 `scrapers/` 下加一个文件，零配置注册。

调度器循环：遍历所有 scraper → 检查是否到达 `interval` → 调用 `fetch()` → 统一验证 → 推送 Kafka。

---

## 2. 核心接口

### Article 数据类

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Article:
    source: str                    # "hackernews"
    source_id: str                 # 来源侧唯一 ID
    title: str
    url: str                       # 原文跳转链接
    author: Optional[str] = None
    score: Optional[int] = None
    summary: Optional[str] = None  # 摘要/描述
    content_html: Optional[str] = None
    published_at: Optional[str] = None  # ISO 格式
    external_metadata: dict = field(default_factory=dict)
```

### BaseScraper 抽象类

```python
from abc import ABC, abstractmethod
import time
import random
from typing import List

class BaseScraper(ABC):
    """所有爬虫的基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """源标识符，如 'hackernews'"""
        ...

    @property
    @abstractmethod
    def interval(self) -> int:
        """轮询间隔（秒），每个 scraper 独立"""
        ...

    @abstractmethod
    def fetch(self) -> List[Article]:
        """抓取并返回文章列表"""
        ...

    @property
    def max_per_run(self) -> int:
        """每轮最多返回文章数"""
        return 30

    @property
    def enabled(self) -> bool:
        """是否启用"""
        return True
```

### 自动发现机制

```python
import importlib
import pkgutil
import scrapers

def discover_scrapers():
    """自动发现 scrapers 包下所有 BaseScraper 子类"""
    scrapers_list = []
    for importer, mod_name, is_pkg in pkgutil.iter_modules(scrapers.__path__):
        if mod_name.startswith('_'):
            continue
        module = importlib.import_module(f"scrapers.{mod_name}")
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, BaseScraper) and attr is not BaseScraper:
                scrapers_list.append(attr())
    return scrapers_list
```

---

## 3. 合规爬虫策略 (ScraperClient)

所有 scraper 共用一个 HTTP 客户端，内置合规策略：

### User-Agent 轮换池

```python
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]
```

### 限流策略

| 措施 | 具体实现 |
|------|----------|
| 随机延时 | 每个请求后 sleep `random.uniform(1, 5)` 秒 |
| 单线程 | 所有请求串行，禁止并发 |
| 超时 | `timeout=10`（连接）+ `timeout=10`（读取） |
| 重试 | 最多 2 次，指数退避（2s → 4s） |
| 单源上限 | 每轮最多 50 次请求 |
| UA 伪装 | 每次请求从池中随机选一个 |
| robots.txt | requests 库 + `robotparser` 缓存 1 小时 |

### ScraperClient 接口

```python
class ScraperClient:
    def get(self, url: str) -> Optional[requests.Response]:
        """GET 请求（限流 + 重试 + UA 伪装）"""
    def get_json(self, url: str) -> Optional[dict]:
        """GET JSON 响应"""
    def get_text(self, url: str) -> Optional[str]:
        """GET 文本响应"""
    def is_allowed(self, url: str) -> bool:
        """检查 robots.txt 是否允许"""
```

---

## 4. 各 Scraper 实现要点

### 4.1 Hacker News（已有，重构）

| 项目 | 说明 |
|------|------|
| API | `https://hacker-news.firebaseio.com/v0/topstories.json` |
| 策略 | 取 top 30，逐个 `/v0/item/{id}.json`，每 2 分钟 1 次 |
| 字段映射 | `by` → `author`, `score` → `score`, `url` → `url`, `title` → `title` |
| 限流 | API 无限制，但 Client 仍加 1-2s 延时 |

### 4.2 GitHub Trending

| 项目 | 说明 |
|------|------|
| 数据源 | 页面解析 `https://github.com/trending`（无官方 API） |
| 策略 | 每天 1 次，解析 HTML 获取仓库列表 |
| 字段映射 | 仓库名 → `title`，描述 → `summary`，链接 → `url`，star 数 → `score` |
| 限制 | 每轮最多 25 个仓库 |

### 4.3 Reddit

| 项目 | 说明 |
|------|------|
| API | `https://www.reddit.com/r/programming/hot.json`, `/r/MachineLearning/hot.json`, `/r/rust/hot.json`, `/r/devops/hot.json`, `/r/cybersecurity/hot.json` |
| 策略 | 每个 subreddit 取 top 10，每 3 分钟 1 次 |
| UA 必填 | Reddit API 要求 User-Agent |
| 字段映射 | `title` → `title`, `url` → `url`, `author` → `author`, `score` → `score`, `selftext` → `summary` |

### 4.4 RSS 科技媒体

| 媒体 | RSS URL | 间隔 |
|------|---------|------|
| Ars Technica | `https://feeds.arstechnica.com/arstechnica/index` | 10 分钟 |
| TechCrunch | `https://techcrunch.com/feed/` | 10 分钟 |
| The Verge | `https://www.theverge.com/rss/index.xml` | 10 分钟 |
| Wired | `https://www.wired.com/feed/rss` | 10 分钟 |

RSS 解析使用 `feedparser` 库。每源取最新 10 条。

### 4.5 Lobsters

| 项目 | 说明 |
|------|------|
| API | `https://lobste.rs/hottest.json` |
| 策略 | 取 top 30，每 3 分钟 1 次 |
| 字段映射 | `title` → `title`, `url` → `url`, `author` → `author`, `score` → `score`, `description` → `summary` |

### 4.6 Dev.to

| 项目 | 说明 |
|------|------|
| API | `https://dev.to/api/articles?top=1&per_page=30` |
| 策略 | 热门文章，取 top 30，每 10 分钟 1 次 |
| 字段映射 | `title` → `title`, `url` → `url`, `user.name` → `author`, `positive_reactions_count` → `score`, `description` → `summary`, `tags` → `external_metadata` |

---

## 5. 数据验证

所有 scraper 产出后，统一校验：

```python
def validate_article(article: Article) -> Optional[Article]:
    """校验文章有效性，剔除无效数据"""
    # 必须有标题
    if not article.title or len(article.title.strip()) < 5:
        return None
    # 必须有原文链接或可展示内容
    has_url = bool(article.url and article.url.startswith("http"))
    has_content = bool(article.content_html or (article.summary and len(article.summary.strip()) > 20))
    if not has_url and not has_content:
        return None
    # URL 必须安全
    if article.url and not article.url.startswith("http"):
        return None
    return article
```

---

## 6. 数据流变更

### 6.1 Kafka

- 当前：topic `raw_tech_feeds`，所有源共用
- 后续扩容：topic 命名规范 `raw_{source}`（如 `raw_github_trending`）
- 消息新增 `"source"` 字段标识来源

### 6.2 Transformer（quixotic_illusion.py）

| 变更项 | 说明 |
|--------|------|
| 解析逻辑 | 按 `source` 字段分发到不同解析器 |
| OSS 路径 | `raw_html/{source}/ds={ds}/{id}.html`（原为 `raw_html/hn/...`） |
| 新增字段 | `source` 列写入输出 |
| 字段兜底 | 非 HN 源可能缺 `by`/`score`，用 `author`/`0` 兜底 |

### 6.3 Sink（insightful_resonance.py）

- 无变更，`source` 字段自动进入 DataFrame 一并写入 OSS Parquet

### 6.4 MaxCompute + dbt

| 变更项 | 说明 |
|--------|------|
| hn_raw 表 | 加 `source` 列（STRING，默认 'hackernews'） |
| stg_tech_news.sql | 新增 `source` 字段透传 |
| mart_trend_analysis | 可以按 `source` 分组分析 |

### 6.5 前端

- 资讯卡片显示来源标签（如 `Hacker News`、`GitHub Trending`）
- 时间线页面过滤增加来源筛选

---

## 7. Producer 调度器

```python
def run():
    scrapers = discover_scrapers()
    last_run = {s.name: 0 for s in scrapers}

    while True:
        now = time.time()
        for scraper in scrapers:
            if now - last_run[scraper.name] >= scraper.interval:
                try:
                    articles = scraper.fetch()
                    validated = [a for a in (validate_article(a) for a in articles) if a]
                    for article in validated:
                        p.produce(TOPIC, key=article.source_id,
                                  value=json.dumps(asdict(article)))
                    p.flush()
                    print(f"[{scraper.name}] 推送 {len(validated)}/{len(articles)} 篇")
                except Exception as e:
                    print(f"[{scraper.name}] 错误: {e}")
                last_run[scraper.name] = now
        time.sleep(30)  # 主循环 30s 一次
```

---

## 8. Producer 依赖更新

`requirements.txt` 新增：
```
requests
confluent-kafka
feedparser       # RSS 解析
beautifulsoup4   # HTML 解析（GitHub Trending 需要）
lxml             # HTML 解析
```

---

## 9. 错误处理策略

| 场景 | 处理方式 |
|------|----------|
| 某 scraper 抛异常 | 日志记录，不影响其他 scraper |
| 网络超时 | 重试 2 次，仍失败则跳过本轮 |
| 空数据返回 | 记录日志，正常跳过 |
| Kafka 不可用 | 等待重连，不丢数据 |
| 某源持续失败 | 累计 5 轮失败后自动禁用，隔 1 小时再试 |
