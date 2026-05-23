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
        return 50

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
