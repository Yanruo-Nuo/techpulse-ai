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

    @property
    def check_robots(self) -> bool:
        return False

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
