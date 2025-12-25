#!/usr/bin/env python3
"""
crawl_to_sheet.py (CRAWL-FIRST + MOVIE FILTER + SITEMAP DAILY + METRICS)

Sheets:
- Movies  (input): movie_title, release_year, aliases, enabled
- Sources (input): publication_id, publication, author, source_url, source_url_type, enabled
- Feed    (output): appends matched reviews (for enabled movies)

Outputs to Feed columns (will set headers if needed):
  movie_title, review_url, headline, publication, source_url, author,
  first_seen_at, match_confidence, matched_by

Sensible target defaults:
- run every 20 minutes (YAML)
- MIN_DOMAIN_DELAY=2.5s
- pagination: review=10, author=6, home=1
- sitemap sweep TTL=24h, only last 14 days

Metrics:
- requests total, bytes downloaded, status histogram, 304 rate, avg latency
- per-domain request counts
- new reviews appended
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import random
import re
import sqlite3
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import requests

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except Exception:
    HAS_BS4 = False

import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Config (defaults tuned to "sensible target")
# ----------------------------

SHEET_ID = os.environ.get("SHEET_ID", "").strip()
GOOGLE_SA_JSON_B64 = os.environ.get("GOOGLE_SA_JSON_B64", "").strip()

WORKSHEET_MOVIES = os.environ.get("WORKSHEET_MOVIES", "Movies").strip()
WORKSHEET_SOURCES = os.environ.get("WORKSHEET_SOURCES", "Sources").strip()
WORKSHEET_FEED = os.environ.get("WORKSHEET_FEED", "Feed").strip()

MIN_DOMAIN_DELAY = float(os.environ.get("MIN_DOMAIN_DELAY", "2.5"))

MAX_NEXT_PAGES_REVIEW = int(os.environ.get("MAX_NEXT_PAGES_REVIEW", "10"))
MAX_NEXT_PAGES_AUTHOR = int(os.environ.get("MAX_NEXT_PAGES_AUTHOR", "6"))
MAX_NEXT_PAGES_HOME = int(os.environ.get("MAX_NEXT_PAGES_HOME", "1"))

SITEMAP_TTL_HOURS = int(os.environ.get("SITEMAP_TTL_HOURS", "24"))
SITEMAP_RECENT_DAYS = int(os.environ.get("SITEMAP_RECENT_DAYS", "14"))

MAX_NEW_PER_RUN = int(os.environ.get("MAX_NEW_PER_RUN", "2000"))

USER_AGENT = os.environ.get("USER_AGENT", "RTReviewFeedCrawler/2.1 (polite; contact: you@example.com)").strip()

FEED_HEADERS = [
    "movie_title",
    "review_url",
    "headline",
    "publication",
    "source_url",
    "author",
    "first_seen_at",
    "match_confidence",
    "matched_by",
]


# ----------------------------
# Metrics
# ----------------------------

@dataclass
class Metrics:
    started_ts: float = time.time()
    requests_total: int = 0
    bytes_total: int = 0
    latency_total_s: float = 0.0
    status_counts: Dict[int, int] = None
    domain_counts: Dict[str, int] = None

    # logical counters
    source_pages_fetched: int = 0
    source_pages_304: int = 0
    article_pages_fetched: int = 0
    sitemap_pages_fetched: int = 0
    candidates_seen: int = 0
    candidates_deduped_seen: int = 0
    candidates_fetched_articles: int = 0
    candidates_confirmed_reviews: int = 0
    candidates_matched_movies: int = 0
    appended_rows: int = 0

    def __post_init__(self):
        if self.status_counts is None:
            self.status_counts = {}
        if self.domain_counts is None:
            self.domain_counts = {}

    def add_request(self, url: str, status: int, bytes_len: int, latency_s: float):
        self.requests_total += 1
        self.bytes_total += max(0, int(bytes_len))
        self.latency_total_s += max(0.0, float(latency_s))
        self.status_counts[status] = self.status_counts.get(status, 0) + 1
        d = urllib.parse.urlparse(url).netloc.lower()
        self.domain_counts[d] = self.domain_counts.get(d, 0) + 1

    def summary_text(self) -> str:
        elapsed = max(0.001, time.time() - self.started_ts)
        avg_latency = (self.latency_total_s / self.requests_total) if self.requests_total else 0.0
        mb = self.bytes_total / (1024 * 1024)
        rps = self.requests_total / elapsed
        s304 = self.status_counts.get(304, 0)
        pct304 = (100.0 * s304 / self.requests_total) if self.requests_total else 0.0

        top_domains = sorted(self.domain_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
        top_domains_str = ", ".join([f"{d}:{c}" for d, c in top_domains])

        status_str = ", ".join([f"{k}:{v}" for k, v in sorted(self.status_counts.items(), key=lambda kv: kv[0])])

        return (
            f"Metrics:\n"
            f"- elapsed_s: {elapsed:.1f}\n"
            f"- requests_total: {self.requests_total} (rps={rps:.2f})\n"
            f"- bytes_total_mb: {mb:.2f}\n"
            f"- avg_latency_s: {avg_latency:.2f}\n"
            f"- status_counts: {status_str}\n"
            f"- 304_rate_pct: {pct304:.1f}\n"
            f"- source_pages_fetched: {self.source_pages_fetched} (304={self.source_pages_304})\n"
            f"- article_pages_fetched: {self.article_pages_fetched}\n"
            f"- sitemap_pages_fetched: {self.sitemap_pages_fetched}\n"
            f"- candidates_seen: {self.candidates_seen}\n"
            f"- candidates_deduped_seen: {self.candidates_deduped_seen}\n"
            f"- candidates_fetched_articles: {self.candidates_fetched_articles}\n"
            f"- candidates_confirmed_reviews: {self.candidates_confirmed_reviews}\n"
            f"- candidates_matched_movies: {self.candidates_matched_movies}\n"
            f"- appended_rows: {self.appended_rows}\n"
            f"- top_domains: {top_domains_str}\n"
        )


# ----------------------------
# Small utils
# ----------------------------

def truthy(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"1", "true", "t", "yes", "y", "on"}


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def norm_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return normalize_ws(s)


def canonicalize_url(url: str, base: Optional[str] = None) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    if url.startswith("#") or url.lower().startswith("mailto:") or url.lower().startswith("javascript:"):
        return None
    if base:
        url = urllib.parse.urljoin(base, url)

    p = urllib.parse.urlparse(url)
    if p.scheme not in ("http", "https"):
        return None

    q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
    q2 = [(k, v) for (k, v) in q if not re.match(r"^(utm_|fbclid|gclid|mc_cid|mc_eid)", k, re.I)]
    new_query = urllib.parse.urlencode(q2)
    p2 = p._replace(query=new_query, fragment="")
    return p2.geturl()


def domain(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.lower()


def same_domain(a: str, b: str) -> bool:
    return domain(a) == domain(b)


# ----------------------------
# DB
# ----------------------------

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_reviews (
            review_url TEXT PRIMARY KEY,
            first_seen_ts INTEGER,
            headline TEXT,
            movie_title TEXT,
            publication TEXT,
            source_url TEXT,
            author TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_state (
            source_url TEXT PRIMARY KEY,
            etag TEXT,
            last_modified TEXT,
            last_fetch_ts INTEGER,
            ok_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            last_ok_ts INTEGER,
            last_error TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sitemap_state (
            domain TEXT PRIMARY KEY,
            last_sitemap_ts INTEGER,
            last_error TEXT
        )
    """)
    conn.commit()
    return conn


def already_seen(conn: sqlite3.Connection, review_url: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen_reviews WHERE review_url = ?", (review_url,))
    return cur.fetchone() is not None


def mark_seen(conn: sqlite3.Connection, row: Dict[str, str]) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO seen_reviews
           (review_url, first_seen_ts, headline, movie_title, publication, source_url, author)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            row["review_url"],
            int(time.time()),
            row.get("headline", ""),
            row.get("movie_title", ""),
            row.get("publication", ""),
            row.get("source_url", ""),
            row.get("author", ""),
        )
    )


def get_source_headers(conn: sqlite3.Connection, source_url: str) -> Tuple[Optional[str], Optional[str]]:
    cur = conn.execute("SELECT etag, last_modified FROM source_state WHERE source_url = ?", (source_url,))
    row = cur.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def update_source_result(conn: sqlite3.Connection, source_url: str, ok: bool, err: str = "") -> None:
    now = int(time.time())
    conn.execute("""
        INSERT INTO source_state(source_url, last_fetch_ts, ok_count, fail_count, last_ok_ts, last_error)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_url) DO UPDATE SET
          last_fetch_ts=excluded.last_fetch_ts,
          ok_count=source_state.ok_count + ?,
          fail_count=source_state.fail_count + ?,
          last_ok_ts=CASE WHEN ?=1 THEN excluded.last_ok_ts ELSE source_state.last_ok_ts END,
          last_error=CASE WHEN ?=1 THEN '' ELSE excluded.last_error END
    """, (
        source_url,
        now,
        1 if ok else 0,
        0 if ok else 1,
        now if ok else None,
        err[:500],
        1 if ok else 0,
        0 if ok else 1,
        1 if ok else 0,
        1 if ok else 0,
    ))


def set_source_headers(conn: sqlite3.Connection, source_url: str, etag: Optional[str], last_modified: Optional[str]) -> None:
    conn.execute("""
        INSERT INTO source_state(source_url, etag, last_modified, last_fetch_ts)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(source_url) DO UPDATE SET
          etag=excluded.etag,
          last_modified=excluded.last_modified,
          last_fetch_ts=excluded.last_fetch_ts
    """, (source_url, etag, last_modified, int(time.time())))


def get_health(conn: sqlite3.Connection, source_url: str) -> Tuple[float, Optional[int], str]:
    cur = conn.execute("""
        SELECT ok_count, fail_count, last_ok_ts, last_error
        FROM source_state WHERE source_url=?
    """, (source_url,))
    row = cur.fetchone()
    if not row:
        return 0.0, None, ""
    okc, fc, last_ok_ts, last_err = row
    total = (okc or 0) + (fc or 0)
    health = (okc or 0) / total if total else 0.0
    return float(health), last_ok_ts, (last_err or "")


def sitemap_due(conn: sqlite3.Connection, d: str) -> bool:
    cur = conn.execute("SELECT last_sitemap_ts FROM sitemap_state WHERE domain=?", (d,))
    row = cur.fetchone()
    if not row or not row[0]:
        return True
    age_hours = (time.time() - row[0]) / 3600.0
    return age_hours >= SITEMAP_TTL_HOURS


def set_sitemap_state(conn: sqlite3.Connection, d: str, ok: bool, err: str = "") -> None:
    now = int(time.time())
    conn.execute("""
        INSERT INTO sitemap_state(domain, last_sitemap_ts, last_error)
        VALUES(?, ?, ?)
        ON CONFLICT(domain) DO UPDATE SET
          last_sitemap_ts=excluded.last_sitemap_ts,
          last_error=excluded.last_error
    """, (d, now, "" if ok else err[:500]))


# ----------------------------
# Rate limiting + fetching (instrumented)
# ----------------------------

class DomainLimiter:
    def __init__(self, min_delay: float):
        self.min_delay = min_delay
        self.next_ok: Dict[str, float] = {}

    def wait(self, d: str) -> None:
        now = time.time()
        t = self.next_ok.get(d, 0.0)
        if now < t:
            time.sleep(t - now)
        self.next_ok[d] = time.time() + self.min_delay + random.random() * 0.8


def _http_get(session: requests.Session, limiter: DomainLimiter, metrics: Metrics,
              url: str, headers: Dict[str, str]) -> requests.Response:
    limiter.wait(domain(url))
    t0 = time.time()
    resp = session.get(url, headers=headers, timeout=25, allow_redirects=True)
    latency = time.time() - t0
    # bytes: use content length after download
    b = len(resp.content or b"")
    metrics.add_request(resp.url or url, resp.status_code, b, latency)
    return resp


def fetch_source_page(session: requests.Session, conn: sqlite3.Connection, limiter: DomainLimiter,
                      metrics: Metrics, url: str) -> Tuple[int, Optional[str], str]:
    """
    Fetch index/source pages with conditional headers.
    Returns: status, html (or None), final_url
    """
    etag, last_mod = get_source_headers(conn, url)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_mod:
        headers["If-Modified-Since"] = last_mod

    resp = _http_get(session, limiter, metrics, url, headers=headers)
    metrics.source_pages_fetched += 1

    set_source_headers(conn, url, resp.headers.get("ETag"), resp.headers.get("Last-Modified"))
    final_url = resp.url or url

    if resp.status_code == 304:
        metrics.source_pages_304 += 1
        update_source_result(conn, url, ok=True)
        return 304, None, final_url
    if resp.status_code >= 400:
        update_source_result(conn, url, ok=False, err=f"HTTP {resp.status_code}")
        return resp.status_code, None, final_url

    resp.encoding = resp.encoding or "utf-8"
    update_source_result(conn, url, ok=True)
    return resp.status_code, resp.text, final_url


def fetch_article_page(session: requests.Session, limiter: DomainLimiter,
                       metrics: Metrics, url: str) -> Tuple[int, Optional[str]]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
    }
    resp = _http_get(session, limiter, metrics, url, headers=headers)
    metrics.article_pages_fetched += 1
    if resp.status_code >= 400:
        return resp.status_code, None
    resp.encoding = resp.encoding or "utf-8"
    return resp.status_code, resp.text


def fetch_text(session: requests.Session, limiter: DomainLimiter,
               metrics: Metrics, url: str, kind: str) -> Tuple[int, Optional[str]]:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/plain,text/xml,text/html,*/*"}
    resp = _http_get(session, limiter, metrics, url, headers=headers)
    if kind == "sitemap":
        metrics.sitemap_pages_fetched += 1
    if resp.status_code >= 400:
        return resp.status_code, None
    resp.encoding = resp.encoding or "utf-8"
    return resp.status_code, resp.text


# ----------------------------
# Parsing + pagination
# ----------------------------

@dataclass
class LinkCandidate:
    url: str
    anchor_text: str


def extract_links(html: str, base_url: str) -> List[LinkCandidate]:
    out: List[LinkCandidate] = []
    if not html:
        return out

    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            cu = canonicalize_url(a["href"], base=base_url)
            if not cu:
                continue
            txt = a.get_text(" ", strip=True) or ""
            out.append(LinkCandidate(cu, txt))
    else:
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
            cu = canonicalize_url(m.group(1), base=base_url)
            if cu:
                out.append(LinkCandidate(cu, ""))

    seen = set()
    dedup: List[LinkCandidate] = []
    for c in out:
        if c.url not in seen:
            seen.add(c.url)
            dedup.append(c)
    return dedup


def find_next_page(html: str, base_url: str) -> Optional[str]:
    if not html:
        return None
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        a = soup.find("a", attrs={"rel": re.compile(r"\bnext\b", re.I)})
        if a and a.get("href"):
            return canonicalize_url(a["href"], base=base_url)
        for a in soup.find_all("a", href=True):
            text = (a.get_text(" ", strip=True) or "").lower()
            if text in ("next", "next >", "older", "older posts", "more", "load more"):
                return canonicalize_url(a["href"], base=base_url)
    else:
        m = re.search(r'rel=["\']next["\']\s+href=["\']([^"\']+)["\']', html, re.I)
        if m:
            return canonicalize_url(m.group(1), base=base_url)
    return None


# ----------------------------
# Review confirmation + headline
# ----------------------------

REVIEW_URL_HINTS = [
    "/review", "/reviews",
    "movie-review", "film-review",
    "/cinema/", "/film/", "/movies/",
    "/entertainment/"
]

BAD_URL_HINTS = [
    "/tag/", "/tags/", "/category/", "/topics/",
    "/about", "/privacy", "/terms", "/contact",
    "/newsletter", "/subscribe", "/donate",
    "/cart", "/login", "/account"
]


def looks_like_review_url(u: str) -> bool:
    ul = (u or "").lower()
    if any(b in ul for b in BAD_URL_HINTS):
        return False
    return any(h in ul for h in REVIEW_URL_HINTS)


def extract_headline(html: str, fallback: str = "") -> str:
    if not html:
        return normalize_ws(fallback)

    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")

        def meta(name: str, attr: str) -> Optional[str]:
            tag = soup.find("meta", attrs={attr: name})
            if tag and tag.get("content"):
                return tag["content"].strip()
            return None

        og = meta("og:title", "property") or meta("og:title", "name")
        if og:
            return normalize_ws(og)
        tw = meta("twitter:title", "name") or meta("twitter:title", "property")
        if tw:
            return normalize_ws(tw)
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t:
                return normalize_ws(t)
        if soup.title and soup.title.get_text(strip=True):
            return normalize_ws(soup.title.get_text(strip=True))

    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        t = re.sub(r"<.*?>", "", m.group(1))
        return normalize_ws(t)

    return normalize_ws(fallback)


def contains_review_type(obj) -> bool:
    if isinstance(obj, dict):
        t = obj.get("@type") or obj.get("type")
        if isinstance(t, str) and t.lower() == "review":
            return True
        if isinstance(t, list) and any(isinstance(x, str) and x.lower() == "review" for x in t):
            return True
        g = obj.get("@graph")
        if g and contains_review_type(g):
            return True
        for v in obj.values():
            if contains_review_type(v):
                return True
    elif isinstance(obj, list):
        for it in obj:
            if contains_review_type(it):
                return True
    return False


def looks_like_review_article(html: str, url: str, headline: str) -> bool:
    hl = (headline or "").lower()
    if "review" in hl:
        return True

    if HAS_BS4 and html:
        soup = BeautifulSoup(html, "html.parser")
        for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
            try:
                txt = script.get_text(strip=True)
                if not txt:
                    continue
                data = json.loads(txt)
                if contains_review_type(data):
                    return True
            except Exception:
                continue
        kw = soup.find("meta", attrs={"name": "keywords"})
        if kw and kw.get("content") and "review" in kw["content"].lower():
            return True

    if looks_like_review_url(url):
        return True

    if html and re.search(r"\breview\b", html, re.I):
        return True

    return False


def extract_body_text_for_matching(html: str, max_chars: int = 6000) -> str:
    if not html:
        return ""
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()
        text = soup.get_text(" ", strip=True)
        return text[:max_chars]
    text = re.sub(r"<[^>]+>", " ", html)
    return normalize_ws(text)[:max_chars]


# ----------------------------
# Movie matching
# ----------------------------

@dataclass
class Movie:
    title: str
    year: Optional[int]
    aliases: List[str]


def parse_aliases(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    if "|" in s:
        parts = [normalize_ws(x) for x in s.split("|")]
    elif ";" in s:
        parts = [normalize_ws(x) for x in s.split(";")]
    else:
        parts = [normalize_ws(s)]
    return [p for p in parts if p]


def movie_keys(m: Movie) -> List[str]:
    base = [m.title] + (m.aliases or [])
    return [norm_text(x) for x in base if x]


def match_movie(movies: List[Movie], headline: str, body_text: str) -> Tuple[Optional[str], float]:
    h = norm_text(headline)
    b = norm_text(body_text)

    best_title = None
    best_score = 0.0

    for m in movies:
        keys = movie_keys(m)
        for k in keys:
            if k and k in h:
                return m.title, 1.0
        for k in keys:
            if k and k in b:
                best_title = m.title
                best_score = max(best_score, 0.7)

        title_tokens = [t for t in norm_text(m.title).split() if len(t) >= 4]
        if title_tokens:
            overlap = sum(1 for t in title_tokens if t in h)
            frac = overlap / max(1, len(title_tokens))
            if frac >= 0.75:
                best_title = m.title
                best_score = max(best_score, 0.5)

    return best_title, best_score


# ----------------------------
# Sitemap fallback
# ----------------------------

SITEMAP_CANDIDATES = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/sitemap-news.xml",
    "/sitemap_posts.xml",
]


def discover_sitemaps_from_robots(robots_txt: str) -> List[str]:
    out = []
    for line in (robots_txt or "").splitlines():
        if line.lower().startswith("sitemap:"):
            u = line.split(":", 1)[1].strip()
            if u:
                cu = canonicalize_url(u)
                if cu:
                    out.append(cu)
    seen = set()
    res = []
    for u in out:
        if u not in seen:
            seen.add(u)
            res.append(u)
    return res


def parse_sitemap_urls(xml_text: str) -> List[Tuple[str, Optional[str]]]:
    if not xml_text:
        return []
    locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml_text, flags=re.I)
    lastmods = re.findall(r"<lastmod>\s*([^<\s]+)\s*</lastmod>", xml_text, flags=re.I)
    out: List[Tuple[str, Optional[str]]] = []
    if len(lastmods) == len(locs):
        for u, lm in zip(locs, lastmods):
            out.append((u.strip(), lm.strip()))
    else:
        for u in locs:
            out.append((u.strip(), None))
    return out


def lastmod_is_recent(lastmod: Optional[str], days: int) -> bool:
    if not lastmod:
        return True
    try:
        d = lastmod.strip()
        if len(d) >= 10:
            d = d[:10]
        lm_date = dt.datetime.strptime(d, "%Y-%m-%d").date()
        return (dt.date.today() - lm_date).days <= days
    except Exception:
        return True


def get_recent_urls_from_sitemaps(session: requests.Session, conn: sqlite3.Connection,
                                 limiter: DomainLimiter, metrics: Metrics, dom: str) -> List[str]:
    if not sitemap_due(conn, dom):
        return []

    base = "https://" + dom
    urls_to_try = [canonicalize_url(base + p) for p in SITEMAP_CANDIDATES if canonicalize_url(base + p)]

    # robots first
    st, robots = fetch_text(session, limiter, metrics, base + "/robots.txt", kind="sitemap")
    sitemap_urls = []
    if robots and st < 400:
        sitemap_urls.extend(discover_sitemaps_from_robots(robots))
    sitemap_urls.extend([u for u in urls_to_try if u])

    seen = set()
    sitemap_urls_dedup = []
    for u in sitemap_urls:
        if u and u not in seen:
            seen.add(u)
            sitemap_urls_dedup.append(u)

    collected: List[str] = []
    errors = []

    for sm in sitemap_urls_dedup[:8]:
        st, txt = fetch_text(session, limiter, metrics, sm, kind="sitemap")
        if not txt:
            errors.append(f"{sm} -> HTTP {st}")
            continue

        pairs = parse_sitemap_urls(txt)
        xml_locs = [loc for (loc, _) in pairs if loc.lower().endswith(".xml")]

        # sitemap index heuristic
        if xml_locs and len(xml_locs) >= min(10, max(1, len(pairs) // 2)):
            for child in xml_locs[:10]:
                child = canonicalize_url(child)
                if not child:
                    continue
                st2, txt2 = fetch_text(session, limiter, metrics, child, kind="sitemap")
                if not txt2:
                    continue
                child_pairs = parse_sitemap_urls(txt2)
                for loc, lastmod in child_pairs:
                    if not lastmod_is_recent(lastmod, SITEMAP_RECENT_DAYS):
                        continue
                    cu = canonicalize_url(loc)
                    if cu and domain(cu) == dom:
                        collected.append(cu)
        else:
            for loc, lastmod in pairs:
                if not lastmod_is_recent(lastmod, SITEMAP_RECENT_DAYS):
                    continue
                cu = canonicalize_url(loc)
                if cu and domain(cu) == dom:
                    collected.append(cu)

    out = []
    seen2 = set()
    for u in collected:
        if u not in seen2:
            seen2.add(u)
            out.append(u)

    set_sitemap_state(conn, dom, ok=bool(out), err="; ".join(errors)[:500])
    return out


# ----------------------------
# Google Sheets I/O
# ----------------------------

def get_gspread_client() -> gspread.Client:
    if not SHEET_ID:
        raise RuntimeError("Missing SHEET_ID")
    if not GOOGLE_SA_JSON_B64:
        raise RuntimeError("Missing GOOGLE_SA_JSON_B64")
    sa_json = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def ensure_feed_headers(ws: gspread.Worksheet) -> None:
    vals = ws.row_values(1)
    vals = [v.strip() for v in vals] if vals else []
    if vals == FEED_HEADERS:
        return
    ws.update("A1", [FEED_HEADERS])


def read_sheet_as_dicts(ws: gspread.Worksheet) -> List[Dict[str, str]]:
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return []
    header = [h.strip() for h in rows[0]]
    out = []
    for r in rows[1:]:
        rec = {}
        for i, h in enumerate(header):
            rec[h] = r[i].strip() if i < len(r) else ""
        out.append(rec)
    return out


def append_rows(ws: gspread.Worksheet, rows: List[List[str]]) -> None:
    if rows:
        ws.append_rows(rows, value_input_option="RAW")


def maybe_update_sources_health(ws_sources: gspread.Worksheet, sources: List["SourceRow"],
                               conn: sqlite3.Connection) -> None:
    header = [h.strip() for h in ws_sources.row_values(1)]
    if not header:
        return

    col_idx = {h: i for i, h in enumerate(header)}
    needed = {"source_url", "crawl_health", "last_ok", "last_error"}
    if not needed.issubset(col_idx.keys()):
        return

    all_rows = ws_sources.get_all_values()
    url_to_rownum: Dict[str, int] = {}
    for i, row in enumerate(all_rows[1:], start=2):
        if col_idx["source_url"] < len(row):
            u = row[col_idx["source_url"]].strip()
            if u:
                url_to_rownum[u] = i

    for s in sources[:500]:
        u = s.source_url
        if u not in url_to_rownum:
            continue
        health, last_ok_ts, last_err = get_health(conn, u)
        last_ok = ""
        if last_ok_ts:
            last_ok = dt.datetime.fromtimestamp(last_ok_ts).isoformat(sep=" ", timespec="minutes")

        rownum = url_to_rownum[u]
        ws_sources.update_cell(rownum, col_idx["crawl_health"] + 1, f"{health:.3f}")
        ws_sources.update_cell(rownum, col_idx["last_ok"] + 1, last_ok)
        ws_sources.update_cell(rownum, col_idx["last_error"] + 1, last_err)


# ----------------------------
# Input models + loaders
# ----------------------------

@dataclass
class SourceRow:
    publication: str
    author: str
    source_url: str
    source_url_type: str
    enabled: bool


def max_pages_for_type(source_url_type: str) -> int:
    t = (source_url_type or "").strip().lower()
    if t == "publication_review":
        return MAX_NEXT_PAGES_REVIEW
    if t == "publication_with_author":
        return MAX_NEXT_PAGES_AUTHOR
    if t == "publication_home":
        return MAX_NEXT_PAGES_HOME
    return 2


def parse_aliases(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    if "|" in s:
        parts = [normalize_ws(x) for x in s.split("|")]
    elif ";" in s:
        parts = [normalize_ws(x) for x in s.split(";")]
    else:
        parts = [normalize_ws(s)]
    return [p for p in parts if p]


def load_movies(ws_movies: gspread.Worksheet) -> List[Movie]:
    rows = read_sheet_as_dicts(ws_movies)
    movies: List[Movie] = []
    for r in rows:
        if not truthy(r.get("enabled", "")):
            continue
        title = normalize_ws(r.get("movie_title", ""))
        if not title:
            continue
        year_s = normalize_ws(r.get("release_year", ""))
        year = int(year_s) if year_s.isdigit() else None
        aliases = parse_aliases(r.get("aliases", ""))
        movies.append(Movie(title=title, year=year, aliases=aliases))
    return movies


def load_sources(ws_sources: gspread.Worksheet) -> List[SourceRow]:
    rows = read_sheet_as_dicts(ws_sources)
    sources: List[SourceRow] = []
    for r in rows:
        if not truthy(r.get("enabled", "")):
            continue
        publication = normalize_ws(r.get("publication", ""))
        author = normalize_ws(r.get("author", ""))
        source_url = normalize_ws(r.get("source_url", ""))
        source_url_type = normalize_ws(r.get("source_url_type", ""))
        if publication and source_url:
            sources.append(SourceRow(publication, author, source_url, source_url_type, True))
    return sources


# ----------------------------
# Crawl loops
# ----------------------------

def crawl_source(session: requests.Session, conn: sqlite3.Connection, limiter: DomainLimiter,
                 metrics: Metrics, s: SourceRow) -> List[LinkCandidate]:
    max_next = max_pages_for_type(s.source_url_type)
    candidates: List[LinkCandidate] = []
    url = s.source_url
    seen_pages: Set[str] = set()

    for _ in range(max_next + 1):
        if not url or url in seen_pages:
            break
        seen_pages.add(url)

        status, html, final_url = fetch_source_page(session, conn, limiter, metrics, url)
        if status == 304:
            break
        if not html:
            break

        base_for_links = final_url or url
        links = extract_links(html, base_for_links)

        for c in links:
            if not same_domain(c.url, s.source_url):
                continue
            if looks_like_review_url(c.url):
                candidates.append(c)

        nxt = find_next_page(html, base_for_links)
        if nxt and same_domain(nxt, s.source_url):
            url = nxt
        else:
            break

    # dedup candidates
    out: List[LinkCandidate] = []
    seen = set()
    for c in candidates:
        if c.url not in seen:
            seen.add(c.url)
            out.append(c)
    metrics.candidates_seen += len(candidates)
    metrics.candidates_deduped_seen += len(out)
    return out


def process_candidate_article(session: requests.Session, limiter: DomainLimiter, metrics: Metrics,
                              movies: List[Movie], cand: LinkCandidate, source: SourceRow) -> Optional[Dict[str, str]]:
    metrics.candidates_fetched_articles += 1
    status, html = fetch_article_page(session, limiter, metrics, cand.url)
    if not html:
        return None

    headline = extract_headline(html, fallback=cand.anchor_text)
    if not headline:
        return None

    if not looks_like_review_article(html, cand.url, headline):
        return None

    metrics.candidates_confirmed_reviews += 1

    body = extract_body_text_for_matching(html)
    matched_title, conf = match_movie(movies, headline, body)
    if not matched_title or conf < 0.5:
        return None

    metrics.candidates_matched_movies += 1

    return {
        "movie_title": matched_title,
        "review_url": cand.url,
        "headline": headline,
        "publication": source.publication,
        "source_url": source.source_url,
        "author": source.author,
        "first_seen_at": dt.datetime.now().isoformat(sep=" ", timespec="seconds"),
        "match_confidence": f"{conf:.2f}",
        "matched_by": "crawl_index",
    }


def process_sitemap_url(session: requests.Session, limiter: DomainLimiter, metrics: Metrics,
                        movies: List[Movie], url: str, rep_source: SourceRow) -> Optional[Dict[str, str]]:
    if any(b in url.lower() for b in BAD_URL_HINTS):
        return None
    cand = LinkCandidate(url=url, anchor_text="")
    row = process_candidate_article(session, limiter, metrics, movies, cand, rep_source)
    if row:
        row["matched_by"] = "sitemap"
    return row


# ----------------------------
# Main
# ----------------------------

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="crawler_state.sqlite", help="SQLite db path (cache this in Actions)")
    ap.add_argument("--max-new-per-run", type=int, default=MAX_NEW_PER_RUN)
    args = ap.parse_args()

    if not SHEET_ID:
        raise SystemExit("Missing SHEET_ID")
    if not GOOGLE_SA_JSON_B64:
        raise SystemExit("Missing GOOGLE_SA_JSON_B64")

    metrics = Metrics()
    conn = init_db(args.db)
    limiter = DomainLimiter(MIN_DOMAIN_DELAY)
    session = requests.Session()

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    ws_movies = sh.worksheet(WORKSHEET_MOVIES)
    ws_sources = sh.worksheet(WORKSHEET_SOURCES)
    ws_feed = sh.worksheet(WORKSHEET_FEED)

    ensure_feed_headers(ws_feed)

    movies = load_movies(ws_movies)
    if not movies:
        raise SystemExit("No enabled movies found in Movies sheet.")

    sources = load_sources(ws_sources)
    if not sources:
        raise SystemExit("No enabled sources found in Sources sheet.")

    # Representative source per domain for sitemap attribution
    dom_to_source: Dict[str, SourceRow] = {}
    for s in sources:
        dom_to_source.setdefault(domain(s.source_url), s)

    rows_to_append: List[List[str]] = []
    new_count = 0

    # 1) Crawl source pages (primary)
    for s in sources:
        if new_count >= args.max_new_per_run:
            break

        try:
            candidates = crawl_source(session, conn, limiter, metrics, s)
        except Exception as e:
            update_source_result(conn, s.source_url, ok=False, err=str(e))
            continue

        for cand in candidates:
            if new_count >= args.max_new_per_run:
                break
            if already_seen(conn, cand.url):
                continue

            row = process_candidate_article(session, limiter, metrics, movies, cand, s)
            if not row:
                continue

            mark_seen(conn, row)
            new_count += 1
            rows_to_append.append([row[h] for h in FEED_HEADERS])

            if len(rows_to_append) >= 100:
                append_rows(ws_feed, rows_to_append)
                metrics.appended_rows += len(rows_to_append)
                rows_to_append.clear()
                conn.commit()

    # 2) Sitemap fallback (once/day per domain)
    for dom, rep_source in dom_to_source.items():
        if new_count >= args.max_new_per_run:
            break

        try:
            sitemap_urls = get_recent_urls_from_sitemaps(session, conn, limiter, metrics, dom)
        except Exception as e:
            set_sitemap_state(conn, dom, ok=False, err=str(e))
            continue

        # Cheap filter before fetch
        sitemap_urls = [u for u in sitemap_urls if looks_like_review_url(u) or "/review" in u.lower() or "/reviews" in u.lower()]

        for u in sitemap_urls[:300]:  # safety cap per domain/run
            if new_count >= args.max_new_per_run:
                break
            if already_seen(conn, u):
                continue

            row = process_sitemap_url(session, limiter, metrics, movies, u, rep_source)
            if not row:
                continue

            mark_seen(conn, row)
            new_count += 1
            rows_to_append.append([row[h] for h in FEED_HEADERS])

            if len(rows_to_append) >= 100:
                append_rows(ws_feed, rows_to_append)
                metrics.appended_rows += len(rows_to_append)
                rows_to_append.clear()
                conn.commit()

    if rows_to_append:
        append_rows(ws_feed, rows_to_append)
        metrics.appended_rows += len(rows_to_append)
        rows_to_append.clear()

    conn.commit()

    # Optional: write health columns back if they exist
    try:
        maybe_update_sources_health(ws_sources, sources, conn)
    except Exception:
        pass

    print(f"Done. New matched reviews appended: {new_count}\n")
    print(metrics.summary_text())


if __name__ == "__main__":
    main()
