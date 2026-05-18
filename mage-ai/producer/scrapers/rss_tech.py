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
