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
from metrics import crawler_http_status_codes, crawler_scrape_duration_seconds

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

    def __init__(self, source_name: str, check_robots: bool = True):
        self.source_name = source_name
        self.session = requests.Session()
        self._check_robots_enabled = check_robots
        self._robots_cache = {}
        self._last_request_time = 0.0
        self._request_count = 0

    def _rotate_ua(self) -> str:
        return random.choice(USER_AGENTS)

    def _rate_limit(self):
        """请求间随机延时 1-3 秒"""
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
        if self._check_robots_enabled and not self._check_robots(url):
            logger.warning(f"[{self.source_name}] robots.txt 禁止: {url}")
            return None
        self._rate_limit()
        headers = {"User-Agent": self._rotate_ua()}
        for attempt in range(3):
            try:
                resp = self.session.get(url, headers=headers, timeout=timeout)
                self._request_count += 1
                _dur = time.time() - self._last_request_time
                crawler_scrape_duration_seconds.labels(source=self.source_name).observe(_dur)
                crawler_http_status_codes.labels(
                    source=self.source_name, code=str(resp.status_code)
                ).inc()
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
        self.client = ScraperClient(self.name, check_robots=self.check_robots)

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
        return 60

    @property
    def enabled(self) -> bool:
        return True

    @property
    def check_robots(self) -> bool:
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
