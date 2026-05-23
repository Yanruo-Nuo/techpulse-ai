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
        return 100

    @property
    def check_robots(self) -> bool:
        return False

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
