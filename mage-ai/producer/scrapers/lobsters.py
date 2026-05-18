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

    @property
    def check_robots(self) -> bool:
        return False

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
