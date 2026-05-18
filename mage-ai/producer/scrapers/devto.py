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

    @property
    def check_robots(self) -> bool:
        return False

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
