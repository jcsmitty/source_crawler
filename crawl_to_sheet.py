#!/usr/bin/env python3
"""
crawl_to_sheet.py — RT review feed crawler (FAST + POLITE + LIVE STATUS)

Key upgrades:
- Never crashes on network errors (Errno 101, DNS, timeouts, etc.)
- Per-page candidate selection uses movie names/aliases BEFORE fetching articles
- Workaround for bad anchors: uses nearby container text (card headline/teaser) for matching
- Caps to avoid one site eating the whole run:
  - per-page candidates cap
  - per-source candidates cap
  - per-run article fetch cap
  - per-run wall-clock time limit
- Domain cooldowns for 403/429/network failures
- Live status updates every N seconds

Google Sheet tabs:
- Movies:  movie_title | release_year | aliases | enabled
- Sources: publication | author | source_url | source_url_type | enabled
- Feed:    appended rows

Feed columns:
movie_title, review_url, headline, publication, source_url, author, first_seen_at, match_confidence, matched_by
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except Exception:
    HAS_BS4 = False

import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# ENV / Config
# ----------------------------

SHEET_ID = os.environ.get("SHEET_ID", "").strip()
GOOGLE_SA_JSON_B64 = os.environ.get("GOOGLE_SA_JSON_B64", "").strip()

WORKSHEET_MOVIES = os.environ.get("WORKSHEET_MOVIES", "Movies").strip()
WORKSHEET_SOURCES = os.environ.get("WORKSHEET_SOURCES", "Sources").strip()
WORKSHEET_FEED = os.environ.get("WORKSHEET_FEED", "Feed").strip()

USER_AGENT = os.environ.get("USER_AGENT", "RTReviewFeedCrawler/2.3 (polite; contact: you@example.com)").strip()

# Politeness
MIN_DOMAIN_DELAY = float(os.environ.get("MIN_DOMAIN_DELAY", "2.0"))
JITTER_S = float(os.environ.get("JITTER_S", "0.8"))

# Pagination
MAX_NEXT_PAGES_REVIEW = int(os.environ.get("MAX_NEXT_PAGES_REVIEW", "8"))
MAX_NEXT_PAGES_AUTHOR = int(os.environ.get("MAX_NEXT_PAGES_AUTHOR", "5"))
MAX_NEXT_PAGES_HOME = int(os.environ.get("MAX_NEXT_PAGES_HOME", "1"))

# Candidate caps (per page) — applied DURING extraction using movie names
MAX_CANDIDATES_PER_PAGE_HOME = int(os.environ.get("MAX_CANDIDATES_PER_PAGE_HOME", "6"))
MAX_CANDIDATES_PER_PAGE_REVIEW = int(os.environ.get("MAX_CANDIDATES_PER_PAGE_REVIEW", "12"))
MAX_CANDIDATES_PER_PAGE_AUTHOR = int(os.environ.get("MAX_CANDIDATES_PER_PAGE_AUTHOR", "10"))

# Candidate caps (per source total)
MAX_CANDIDATES_PER_SOURCE_HOME = int(os.environ.get("MAX_CANDIDATES_PER_SOURCE_HOME", "25"))
MAX_CANDIDATES_PER_SOURCE_REVIEW = int(os.environ.get("MAX_CANDIDATES_PER_SOURCE_REVIEW", "60"))
MAX_CANDIDATES_PER_SOURCE_AUTHOR = int(os.environ.get("MAX_CANDIDATES_PER_SOURCE_AUTHOR", "45"))

# Article fetching limits
ARTICLE_WORKERS = int(os.environ.get("ARTICLE_WORKERS", "4"))
MAX_ARTICLE_FETCHES_PER_RUN = int(os.environ.get("MAX_ARTICLE_FETCHES_PER_RUN", "350"))
MAX_ARTICLES_PER_SOURCE = int(os.environ.get("MAX_ARTICLES_PER_SOURCE", "12"))

# Runtime safety: stop before Actions kills you / schedule overlap
MAX_RUN_SECONDS = int(os.environ.get("MAX_RUN_SECONDS", str(18 * 60)))

# Domain cooldowns
COOLDOWN_403_SECONDS = int(os.environ.get("COOLDOWN_403_SECONDS", str(60 * 60)))   # 1h
COOLDOWN_429_SECONDS = int(os.environ.get("COOLDOWN_429_SECONDS", str(30 * 60)))   # 30m
COOLDOWN_NETFAIL_SECONDS = int(os.environ.get("COOLDOWN_NETFAIL_SECONDS", str(60 * 30)))  # 30m

# Live status
STATUS_EVERY_S = float(os.environ.get("STATUS_EVERY_S", "15"))

# Optional: for noisy homepages, require a movie mention in card text
REQUIRE_MOVIE_ON_HOME = os.environ.get("REQUIRE_MOVIE_ON_HOME", "true").lower() in {"1", "true", "yes", "y", "on"}
# For review indexes/author pages you can keep it looser (still uses movie mention to cap per-page)
REQUIRE_MOVIE_ON_REVIEW = os.environ.get("REQUIRE_MOVIE_ON_REVIEW", "false").lower() in {"1", "true", "yes", "y", "on"}
REQUIRE_MOVIE_ON_AUTHOR = os.environ.get("REQUIRE_MOVIE_ON_AUTHOR", "false").lower() in {"1", "true", "yes", "y", "on"}

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
# Metrics + Status
# ----------------------------

@dataclass
class Metrics:
    started_ts: float
    last_print_ts: float = 0.0

    # counts
    sources_total: int = 0
    sources_done: int = 0

    requests_total: int = 0
    bytes_total: int = 0
    latency_total_s: float = 0.0
    status_counts: Dict[int, int] = None
    domain_counts: Dict[str, int] = None

    source_pages_fetched: int = 0
    source_pages_304: int = 0
    article_pages_fetched: int = 0

    candidates_seen: int = 0
    candidates_deduped_seen: int = 0
    candidates_queued_articles: int = 0
    candidates_confirmed_reviews: int = 0
    candidates_matched_movies: int = 0
    appended_rows: int = 0

    last_domain: str = ""
    last_status: int = 0

    def __post_init__(self):
        self.status_counts = self.status_counts or {}
        self.domain_counts = self.domain_counts or {}

    def add_request(self, url: str, status: int, bytes_len: int, latency_s: float):
        self.requests_total += 1
        self.bytes_total += max(0, int(bytes_len))
        self.latency_total_s += max(0.0, float(latency_s))
        self.status_counts[status] = self.status_counts.get(status, 0) + 1
        d = urllib.parse.urlparse(url).netloc.lower()
        self.domain_counts[d] = self.domain_counts.get(d, 0) + 1
        self.last_domain = d
        self.last_status = status

    def maybe_print(self, force: bool = False):
        now = time.time()
        if not force and (now - self.last_print_ts) < STATUS_EVERY_S:
            return
        self.last_print_ts = now

        elapsed = max(0.001, now - self.started_ts)
        avg_latency = (self.latency_total_s / self.requests_total) if self.requests_total else 0.0
        mb = self.bytes_total / (1024 * 1024)
        s304 = self.status_counts.get(304, 0)
        pct304 = (100.0 * s304 / self.requests_total) if self.requests_total else 0.0

        eta_s = None
        if self.sources_done > 0 and self.sources_total > 0:
            per_source = elapsed / self.sources_done
            remaining = self.sources_total - self.sources_done
            eta_s = remaining * per_source
        eta_txt = f"{eta_s/60:.1f}m" if eta_s is not None else "n/a"

        print(
            f"[{dt.datetime.now().strftime('%H:%M:%S')}] "
            f"sources {self.sources_done}/{self.sources_total} | "
            f"cand {self.candidates_deduped_seen} | "
            f"queued {self.candidates_queued_articles} | "
            f"articles {self.article_pages_fetched} | "
            f"reviews {self.candidates_confirmed_reviews} | "
            f"matches {self.candidates_matched_movies} | "
            f"appended {self.appended_rows} | "
            f"req {self.requests_total} | {mb:.1f}MB | "
            f"avg {avg_latency:.2f}s | 304 {pct304:.0f}% | "
            f"last {self.last_domain} {self.last_status} | ETA {eta_txt}"
        )


# ----------------------------
# Utils
# ----------------------------

def truthy(s: str) -> bool:
    return (s or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}

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
    p2 = p._replace(query=urllib.parse.urlencode(q2), fragment="")
    return p2.geturl()

def domain(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.lower()

def same_domain(a: str, b: str) -> bool:
    return domain(a) == domain(b)


# ----------------------------
# DB (dedupe + conditional GET cache)
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
            last_fetch_ts INTEGER
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

def set_source_headers(conn: sqlite3.Connection, source_url: str, etag: Optional[str], last_modified: Optional[str]) -> None:
    conn.execute("""
        INSERT INTO source_state(source_url, etag, last_modified, last_fetch_ts)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(source_url) DO UPDATE SET
          etag=excluded.etag,
          last_modified=excluded.last_modified,
          last_fetch_ts=excluded.last_fetch_ts
    """, (source_url, etag, last_modified, int(time.time())))


# ----------------------------
# HTTP session + retries
# ----------------------------

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# ----------------------------
# Rate limiting + domain cooldowns
# ----------------------------

class DomainLimiter:
    def __init__(self, min_delay: float, jitter: float):
        self.min_delay = min_delay
        self.jitter = jitter
        self.next_ok: Dict[str, float] = {}

    def wait(self, d: str) -> None:
        now = time.time()
        t = self.next_ok.get(d, 0.0)
        if now < t:
            time.sleep(t - now)
        self.next_ok[d] = time.time() + self.min_delay + random.random() * self.jitter


class DomainCooldowns:
    def __init__(self):
        self.until: Dict[str, float] = {}

    def is_cooled(self, d: str) -> bool:
        return time.time() < self.until.get(d, 0.0)

    def set(self, d: str, seconds: int):
        self.until[d] = max(self.until.get(d, 0.0), time.time() + seconds)


def safe_http_get(session: requests.Session, limiter: DomainLimiter, cooldowns: DomainCooldowns,
                  metrics: Metrics, url: str, headers: Dict[str, str]) -> Optional[requests.Response]:
    """
    Never throws. Returns Response or None on failure or if domain is cooled down.
    Records status=0 for network failures.
    """
    d = domain(url)
    if cooldowns.is_cooled(d):
        return None

    limiter.wait(d)
    t0 = time.time()
    try:
        resp = session.get(url, headers=headers, timeout=25, allow_redirects=True)
        metrics.add_request(resp.url or url, resp.status_code, len(resp.content or b""), time.time() - t0)

        # cooldown policy
        if resp.status_code == 429:
            cooldowns.set(d, COOLDOWN_429_SECONDS)
        elif resp.status_code == 403:
            cooldowns.set(d, COOLDOWN_403_SECONDS)

        return resp
    except requests.exceptions.RequestException:
        metrics.add_request(url, 0, 0, time.time() - t0)
        cooldowns.set(d, COOLDOWN_NETFAIL_SECONDS)
        return None


# ----------------------------
# Parsing candidates + "workaround" text extraction
# ----------------------------

@dataclass
class LinkCandidate:
    url: str
    anchor_text: str  # may include nearby card/snippet text

REVIEW_URL_HINTS = [
    "/review", "/reviews",
    "movie-review", "film-review",
    "/cinema/", "/film/", "/movies/",
    "/entertainment/",
]
BAD_URL_HINTS = [
    "/tag/", "/tags/", "/category/", "/topics/",
    "/about", "/privacy", "/terms", "/contact",
    "/newsletter", "/subscribe", "/donate",
    "/cart", "/login", "/account",
]

def looks_like_review_url(u: str) -> bool:
    ul = (u or "").lower()
    if any(b in ul for b in BAD_URL_HINTS):
        return False
    return any(h in ul for h in REVIEW_URL_HINTS)

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

def extract_nearby_text_for_link(a_tag) -> str:
    """
    Workaround for bad anchors:
    - If anchor text is short ("Read more"), grab parent container text (headline/teaser).
    """
    if not HAS_BS4:
        return ""

    anchor = a_tag.get_text(" ", strip=True) or ""
    anchor_norm = normalize_ws(anchor)

    # If anchor already informative, use it
    if len(anchor_norm) >= 18 and not re.fullmatch(r"(read more|review|more|click here)", anchor_norm.lower()):
        return anchor_norm

    # Walk up a few levels to find a "card-ish" container
    cur = a_tag
    for _ in range(4):
        if not cur:
            break
        cur = cur.parent
        if not cur:
            break
        txt = normalize_ws(cur.get_text(" ", strip=True) or "")
        # Heuristic: if container has enough text, it's likely a card with headline/teaser
        if len(txt) >= 30:
            # keep it reasonably small (avoid nav/footer text)
            return txt[:240]

    return anchor_norm

def extract_links_with_workarounds(html: str, base_url: str) -> List[LinkCandidate]:
    if not html:
        return []

    out: List[LinkCandidate] = []

    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            cu = canonicalize_url(a["href"], base=base_url)
            if not cu:
                continue
            txt = extract_nearby_text_for_link(a)
            out.append(LinkCandidate(cu, txt))
    else:
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
            cu = canonicalize_url(m.group(1), base=base_url)
            if cu:
                out.append(LinkCandidate(cu, ""))

    # Dedup by URL
    seen = set()
    dedup: List[LinkCandidate] = []
    for c in out:
        if c.url not in seen:
            seen.add(c.url)
            dedup.append(c)
    return dedup


# ----------------------------
# Headline + "is review" confirmation
# ----------------------------

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

def extract_body_text_for_matching(html: str, max_chars: int = 4500) -> str:
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
# Movies: fast phrase set + matching
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

def build_movie_phrase_set(movies: List[Movie]) -> Set[str]:
    phrases: Set[str] = set()
    for m in movies:
        for k in movie_keys(m):
            if k and len(k) >= 4:
                phrases.add(k)
    return phrases

def text_mentions_any_movie(text: str, movie_phrases: Set[str]) -> bool:
    t = norm_text(text)
    if not t:
        return False
    for p in movie_phrases:
        if p in t:
            return True
    return False

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


# ----------------------------
# Inputs
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

def per_page_cap_for_type(source_url_type: str) -> int:
    t = (source_url_type or "").strip().lower()
    if t == "publication_home":
        return MAX_CANDIDATES_PER_PAGE_HOME
    if t == "publication_review":
        return MAX_CANDIDATES_PER_PAGE_REVIEW
    if t == "publication_with_author":
        return MAX_CANDIDATES_PER_PAGE_AUTHOR
    return 10

def per_source_cap_for_type(source_url_type: str) -> int:
    t = (source_url_type or "").strip().lower()
    if t == "publication_home":
        return MAX_CANDIDATES_PER_SOURCE_HOME
    if t == "publication_review":
        return MAX_CANDIDATES_PER_SOURCE_REVIEW
    if t == "publication_with_author":
        return MAX_CANDIDATES_PER_SOURCE_AUTHOR
    return 40

def require_movie_for_type(source_url_type: str) -> bool:
    t = (source_url_type or "").strip().lower()
    if t == "publication_home":
        return REQUIRE_MOVIE_ON_HOME
    if t == "publication_review":
        return REQUIRE_MOVIE_ON_REVIEW
    if t == "publication_with_author":
        return REQUIRE_MOVIE_ON_AUTHOR
    return False

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
# Crawl source pages with per-page movie-filter caps
# ----------------------------

def crawl_source(session: requests.Session, conn: sqlite3.Connection, limiter: DomainLimiter,
                 cooldowns: DomainCooldowns, metrics: Metrics,
                 s: SourceRow, movie_phrases: Set[str]) -> List[LinkCandidate]:
    max_next = max_pages_for_type(s.source_url_type)
    per_page_cap = per_page_cap_for_type(s.source_url_type)
    per_source_cap = per_source_cap_for_type(s.source_url_type)
    require_movie = require_movie_for_type(s.source_url_type)

    candidates: List[LinkCandidate] = []
    url = s.source_url
    seen_pages: Set[str] = set()

    for _ in range(max_next + 1):
        if not url or url in seen_pages:
            break
        if len(candidates) >= per_source_cap:
            break
        seen_pages.add(url)

        etag, last_mod = get_source_headers(conn, url)
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
        if etag:
            headers["If-None-Match"] = etag
        if last_mod:
            headers["If-Modified-Since"] = last_mod

        resp = safe_http_get(session, limiter, cooldowns, metrics, url, headers=headers)
        metrics.source_pages_fetched += 1
        metrics.maybe_print()

        if resp is None:
            break

        set_source_headers(conn, url, resp.headers.get("ETag"), resp.headers.get("Last-Modified"))
        final_url = resp.url or url

        if resp.status_code == 304:
            metrics.source_pages_304 += 1
            break
        if resp.status_code >= 400:
            break

        resp.encoding = resp.encoding or "utf-8"
        html = resp.text or ""
        links = extract_links_with_workarounds(html, final_url)

        page_hits: List[LinkCandidate] = []
        for c in links:
            if not same_domain(c.url, s.source_url):
                continue
            if not looks_like_review_url(c.url):
                continue

            # IMPORTANT: Use movie names DURING selection (per-page cap)
            # Workaround text already includes nearby card text when anchor is useless.
            if require_movie:
                if not text_mentions_any_movie(c.anchor_text, movie_phrases):
                    continue

            page_hits.append(c)

        # If not requiring movie for this type, we still prefer movie mentions first
        if not require_movie:
            page_hits.sort(key=lambda x: 1 if text_mentions_any_movie(x.anchor_text, movie_phrases) else 0, reverse=True)

        # Per-page cap
        if len(page_hits) > per_page_cap:
            page_hits = page_hits[:per_page_cap]

        candidates.extend(page_hits)
        if len(candidates) >= per_source_cap:
            break

        nxt = find_next_page(html, final_url)
        if nxt and same_domain(nxt, s.source_url):
            url = nxt
        else:
            break

    # Dedup candidates (within this source)
    seen = set()
    out: List[LinkCandidate] = []
    for c in candidates:
        if c.url not in seen:
            seen.add(c.url)
            out.append(c)

    metrics.candidates_seen += len(candidates)
    metrics.candidates_deduped_seen += len(out)
    return out


# ----------------------------
# Article fetch + match
# ----------------------------

def process_candidate_article(session: requests.Session, limiter: DomainLimiter, cooldowns: DomainCooldowns,
                              metrics: Metrics, movies: List[Movie],
                              cand: LinkCandidate, source: SourceRow) -> Optional[Dict[str, str]]:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    resp = safe_http_get(session, limiter, cooldowns, metrics, cand.url, headers=headers)
    metrics.article_pages_fetched += 1
    metrics.maybe_print()

    if resp is None or resp.status_code >= 400:
        return None

    resp.encoding = resp.encoding or "utf-8"
    html = resp.text or ""
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


# ----------------------------
# Main
# ----------------------------

def main():
    import argparse
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="crawler_state.sqlite")
    ap.add_argument("--max-new-per-run", type=int, default=2000)
    args = ap.parse_args()

    if not SHEET_ID or not GOOGLE_SA_JSON_B64:
        raise SystemExit("Missing SHEET_ID or GOOGLE_SA_JSON_B64")

    started = time.time()
    deadline = started + MAX_RUN_SECONDS

    metrics = Metrics(started_ts=started)
    conn = init_db(args.db)
    limiter = DomainLimiter(MIN_DOMAIN_DELAY, JITTER_S)
    cooldowns = DomainCooldowns()
    session = build_session()

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws_movies = sh.worksheet(WORKSHEET_MOVIES)
    ws_sources = sh.worksheet(WORKSHEET_SOURCES)
    ws_feed = sh.worksheet(WORKSHEET_FEED)

    ensure_feed_headers(ws_feed)

    movies = load_movies(ws_movies)
    if not movies:
        raise SystemExit("No enabled movies found in Movies sheet.")
    movie_phrases = build_movie_phrase_set(movies)

    sources = load_sources(ws_sources)
    if not sources:
        raise SystemExit("No enabled sources found in Sources sheet.")

    metrics.sources_total = len(sources)

    rows_to_append: List[List[str]] = []
    new_count = 0

    article_budget = MAX_ARTICLE_FETCHES_PER_RUN

    for s in sources:
        if time.time() > deadline:
            break
        if new_count >= args.max_new_per_run:
            break
        if article_budget <= 0:
            break

        metrics.sources_done += 1
        metrics.maybe_print()

        # Crawl index/source pages, selecting candidates with per-page caps
        candidates = crawl_source(session, conn, limiter, cooldowns, metrics, s, movie_phrases)

        # Remove already seen
        candidates = [c for c in candidates if not already_seen(conn, c.url)]

        # Per-source article cap and per-run budget
        if not candidates:
            continue

        # Prefer candidates whose anchor/snippet mentions a movie (workaround text)
        candidates.sort(key=lambda c: 1 if text_mentions_any_movie(c.anchor_text, movie_phrases) else 0, reverse=True)

        candidates = candidates[:min(MAX_ARTICLES_PER_SOURCE, article_budget)]
        article_budget -= len(candidates)
        metrics.candidates_queued_articles += len(candidates)

        # Fetch articles with small concurrency (still domain-throttled)
        with ThreadPoolExecutor(max_workers=max(1, ARTICLE_WORKERS)) as ex:
            futs = [ex.submit(process_candidate_article, session, limiter, cooldowns, metrics, movies, c, s) for c in candidates]
            for fut in as_completed(futs):
                if time.time() > deadline or new_count >= args.max_new_per_run:
                    break
                row = fut.result()
                if not row:
                    continue
                if already_seen(conn, row["review_url"]):
                    continue

                mark_seen(conn, row)
                new_count += 1
                rows_to_append.append([row[h] for h in FEED_HEADERS])

                if len(rows_to_append) >= 100:
                    append_rows(ws_feed, rows_to_append)
                    metrics.appended_rows += len(rows_to_append)
                    rows_to_append.clear()
                    conn.commit()

        if time.time() > deadline:
            break

    if rows_to_append:
        append_rows(ws_feed, rows_to_append)
        metrics.appended_rows += len(rows_to_append)
        rows_to_append.clear()

    conn.commit()
    metrics.maybe_print(force=True)

    print("\nRun complete.")
    print(f"New matched reviews appended: {new_count}")
    elapsed = time.time() - started
    print(f"Elapsed: {elapsed:.1f}s (limit {MAX_RUN_SECONDS}s)")


if __name__ == "__main__":
    main()
