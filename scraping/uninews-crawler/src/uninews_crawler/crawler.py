from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


DEFAULT_KEYWORDS = [
    "合作", "校企", "企业合作", "产学研", "战略合作", "签约", "校企合作", "产业合作",
    "企业捐赠", "合作办学", "联合实验室", "cooperation", "partnership", "collaboration",
]

DEFAULT_SITES = {
    "清华大学": "https://www.tsinghua.edu.cn/",
    "北京大学": "https://www.pku.edu.cn/",
    "浙江大学": "https://www.zju.edu.cn/",
    "复旦大学": "https://www.fudan.edu.cn/",
    "上海交通大学": "https://www.sjtu.edu.cn/",
}

NEWS_LIST_PATTERNS = ["news/", "article/", "info/", "content/", "xxgg/", "xwdt/"]
NEWS_CONTENT_SELECTORS = {
    "title": ["h1", ".article-title", ".news-title", "title"],
    "time":  [".publish-time", ".article-time", ".news-time", "time"],
    "body":  [".article-content", ".news-content", ".content", "article"],
}
NEWS_LINK_SELECTORS = [
    'a[href*="news"]',
    'a[href*="article"]',
    'a[href*="info"]',
    'a[href*="content"]',
    ".news-list a", ".article-list a", ".news-item a", ".list-item a",
]


@dataclass
class CrawlConfig:
    timeout: int = 10
    delay_min: float = 1.0
    delay_max: float = 3.0
    max_per_site: int = 10
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )


@dataclass
class UniversityNewsCrawler:
    keywords: list[str] = field(default_factory=lambda: list(DEFAULT_KEYWORDS))
    sites: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_SITES))
    cfg: CrawlConfig = field(default_factory=CrawlConfig)

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.cfg.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        })
        self.rows: list[dict] = []

    # ---------------- helpers ----------------
    @staticmethod
    def _is_valid_url(url: str) -> bool:
        try:
            res = urlparse(url)
            return bool(res.scheme and res.netloc)
        except Exception:
            return False

    def _get(self, url: str) -> str | None:
        try:
            resp = self.session.get(url, timeout=self.cfg.timeout)
            resp.encoding = resp.encoding or "utf-8"
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.warning("GET failed %s: %s", url, e)
            return None

    def _contains_kw(self, text: str | None) -> bool:
        if not text:
            return False
        tl = text.lower()
        return any(kw.lower() in tl for kw in self.keywords)

    def _extract_links(self, html: str, base_url: str) -> list[tuple[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        links: list[tuple[str, str]] = []
        for sel in NEWS_LINK_SELECTORS:
            for a in soup.select(sel):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(base_url, href)
                if not self._is_valid_url(full):
                    continue
                text = a.get_text(strip=True)
                if text:
                    links.append((full, text))
        return list({(u, t) for (u, t) in links})  # dedupe

    def _extract_content(self, html: str, url: str) -> dict:
        soup = BeautifulSoup(html, "lxml")

        def pick(selectors: list[str]) -> str:
            for s in selectors:
                el = soup.select_one(s)
                if el:
                    txt = el.get_text(strip=True)
                    if txt:
                        return txt
            return ""

        title = pick(NEWS_CONTENT_SELECTORS["title"])
        publish_time = pick(NEWS_CONTENT_SELECTORS["time"])
        content = pick(NEWS_CONTENT_SELECTORS["body"]) or soup.get_text(strip=True)
        if len(content) > 500:
            content = content[:500] + "..."

        return {"title": title, "publish_time": publish_time, "content": content, "url": url}

    # ---------------- core ----------------
    def crawl_site(self, name: str, base_url: str) -> None:
        logger.info("Crawling %s", name)
        candidates: list[tuple[str, str]] = []

        # try common news-list paths
        for pat in NEWS_LIST_PATTERNS:
            html = self._get(urljoin(base_url, pat))
            if not html:
                continue
            if self._contains_kw(html):
                candidates += self._extract_links(html, base_url)

        # also scan homepage
        home = self._get(base_url)
        if home:
            candidates += self._extract_links(home, base_url)

        # filter to links whose anchor contains keywords
        uniq = list({(u, t) for (u, t) in candidates})
        filtered = [(u, t) for (u, t) in uniq if self._contains_kw(t)]
        logger.info("[%s] candidate links: %d", name, len(filtered))

        for i, (url, link_text) in enumerate(filtered[: self.cfg.max_per_site], start=1):
            html = self._get(url)
            if not html:
                continue
            data = self._extract_content(html, url)
            self.rows.append({
                "university": name,
                "title": data["title"],
                "publish_time": data["publish_time"],
                "content": data["content"],
                "url": data["url"],
                "link_text": link_text,
                "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            time.sleep(random.uniform(self.cfg.delay_min, self.cfg.delay_max))

    def crawl(self, sites: dict[str, str] | None = None) -> list[dict]:
        target = sites or self.sites
        for name, url in target.items():
            try:
                self.crawl_site(name, url)
            except Exception as e:
                logger.error("Error on %s: %s", name, e)
        return self.rows

    # ---------------- output ----------------
    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)

    def save(self, path: str) -> str:
        df = self.to_dataframe()
        if path.lower().endswith(".xlsx"):
            df.to_excel(path, index=False, engine="openpyxl")
        elif path.lower().endswith(".csv"):
            df.to_csv(path, index=False)
        else:
            raise ValueError("Output must end with .xlsx or .csv")
        return path

    @staticmethod
    def default_output(ext: str = "xlsx") -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"university_cooperation_news_{ts}.{ext}"