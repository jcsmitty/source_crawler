#!/usr/bin/env python3
"""
crawl_to_sheet.py (LIVE STATUS + SPEED KNOBS)

New:
- Live status line (prints every STATUS_EVERY_S seconds)
- Faster early filtering to reduce article fetches
- Optional small concurrency for ARTICLE fetches only (safe-ish)
- Caps and backoff hooks

Defaults keep block-risk reasonable.
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
# Config
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

# --- Live status
STATUS_EVERY_S = float(os.environ.get("STATUS_EVERY_S", "15"))
# --- Speed knobs
ARTICLE_WORKERS = int(os.environ.get("ARTICLE_WORKERS", "4"))  # concurrency for article fetches only
MAX_ARTICLE_FETCHES_PER_RUN = int(os.environ.get("MAX_ARTICLE_FETCHES_PER_RUN", "600"))
# If true, only fetch an article if headline/anchor already seems to mention a movie (big speed win)
REQUIRE_MOVIE_IN_ANCHOR_BEFORE_FETCH = os.environ.get("REQUIRE_MOVIE_IN_ANCHOR_BEFORE_FETCH", "true").lower() in {"1","true","yes","y","on"}

USER_AGENT = os.environ.get("USER_AGENT", "RTReviewFeedCrawler/2.2 (polite; contact: you@example.com)").strip()

FEED_HEADERS = [
    "movie_title","review_url","headline","publication","source_url","author",
    "first_seen_at","match_confidence","matched_by"
]


# ----------------------------
# Metrics + live status
# ----------------------------

@dataclass
class Metrics:
    started_ts: float
    requests_total: int = 0
    bytes_total: int = 0
    latency_total_s: float = 0.0
    status_counts: Dict[int, int] = None
    domain_counts: Dict[str, int] = None

    # logical counters
    sources_total: int = 0
    sources_done: int = 0
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

    last_print_ts: float = 0.0
    last_domain: str = ""
    last_status: int = 0

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

        # ETA based on sources progress
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
            f"articles {self.article_pages_fetched} | "
            f"reviews {self.candidates_confirmed_reviews} | "
            f"matches {self.candidates_matched_movies} | "
            f"appended {self.appended_rows} | "
            f"req {self.requests_total} | {mb:.1f}MB | "
            f"avg {avg_latency:.2f}s | 304 {pct304:.0f}% | "
            f"last {self.last_domain} {self.last_status} | "
            f"ETA {eta_txt}"
        )


# ----------------------------
# Utils
# ----------------------------

def truthy(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"1","true","t","yes","y","on"}

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
    if p.scheme not in ("http","https"):
        return None
    q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
    q2 = [(k,v) for (k,v) in q if not re.match(r"^(utm_|fbclid|gclid|mc_cid|mc_eid)", k, re.I)]
    p2 = p._replace(query=urllib.parse.urlencode(q2), fragment="")
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
            last_fetch_ts INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sitemap_state (
            domain TEXT PRIMARY KEY,
            last_sitemap_ts INTEGER
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
            row.get("headline",""),
            row.get("movie_title",""),
            row.get("publication",""),
            row.get("source_url",""),
            row.get("author",""),
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

def sitemap_due(conn: sqlite3.Connection, d: str, ttl_hours: int) -> bool:
    cur = conn.execute("SELECT last_sitemap_ts FROM sitemap_state WHERE domain=?", (d,))
    row = cur.fetchone()
    if not row or not row[0]:
        return True
    age_hours = (time.time() - row[0]) / 3600.0
    return age_hours >= ttl_hours

def set_sitemap_ts(conn: sqlite3.Connection, d: str) -> None:
    conn.execute("""
        INSERT INTO sitemap_state(domain, last_sitemap_ts) VALUES(?, ?)
        ON CONFLICT(domain) DO UPDATE SET last_sitemap_ts=excluded.last_sitemap_ts
    """, (d, int(time.time())))


# ----------------------------
# Rate limit + HTTP (instrumented)
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


def http_get(session: requests.Session, limiter: DomainLimiter, metrics: Metrics,
             url: str, headers: Dict[str,str]) -> requests.Response:
    limiter.wait(domain(url))
    t0 = time.time()
    resp = session.get(url, headers=headers, timeout=25, allow_redirects=True)
    latency = time.time() - t0
    metrics.add_request(resp.url or url, resp.status_code, len(resp.content or b""), latency)
    return resp


# ----------------------------
# Parsing + review logic
# ----------------------------

@dataclass
class LinkCandidate:
    url: str
    anchor_text: str

REVIEW_URL_HINTS = ["/review","/reviews","movie-review","film-review","/cinema/","/film/","/movies/","/entertainment/"]
BAD_URL_HINTS = ["/tag/","/tags/","/category/","/topics/","/about","/privacy","/terms","/contact",
                 "/newsletter","/subscribe","/donate","/cart","/login","/account"]

def looks_like_review_url(u: str) -> bool:
    ul = (u or "").lower()
    if any(b in ul for b in BAD_URL_HINTS):
        return False
    return any(h in ul for h in REVIEW_URL_HINTS)

def extract_links(html: str, base_url: str) -> List[LinkCandidate]:
    if not html:
        return []
    out: List[LinkCandidate] = []
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            cu = canonicalize_url(a["href"], base=base_url)
            if not cu:
                continue
            out.append(LinkCandidate(cu, a.get_text(" ", strip=True) or ""))
    else:
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
            cu = canonicalize_url(m.group(1), base=base_url)
            if cu:
                out.append(LinkCandidate(cu, ""))
    seen=set(); ded=[]
    for c in out:
        if c.url not in seen:
            seen.add(c.url); ded.append(c)
    return ded

def find_next_page(html: str, base_url: str) -> Optional[str]:
    if not html:
        return None
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        a = soup.find("a", attrs={"rel": re.compile(r"\bnext\b", re.I)})
        if a and a.get("href"):
            return canonicalize_url(a["href"], base=base_url)
        for a in soup.find_all("a", href=True):
            t = (a.get_text(" ", strip=True) or "").lower()
            if t in ("next","next >", "older","older posts","more","load more"):
                return canonicalize_url(a["href"], base=base_url)
    else:
        m = re.search(r'rel=["\']next["\']\s+href=["\']([^"\']+)["\']', html, re.I)
        if m:
            return canonicalize_url(m.group(1), base=base_url)
    return None

def extract_headline(html: str, fallback: str="") -> str:
    if not html:
        return normalize_ws(fallback)
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        def meta(name: str, attr: str) -> Optional[str]:
            tag = soup.find("meta", attrs={attr: name})
            if tag and tag.get("content"):
                return tag["content"].strip()
            return None
        og = meta("og:title","property") or meta("og:title","name")
        if og: return normalize_ws(og)
        tw = meta("twitter:title","name") or meta("twitter:title","property")
        if tw: return normalize_ws(tw)
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t: return normalize_ws(t)
        if soup.title and soup.title.get_text(strip=True):
            return normalize_ws(soup.title.get_text(strip=True))
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I|re.S)
    if m:
        return normalize_ws(re.sub(r"<.*?>","",m.group(1)))
    return normalize_ws(fallback)

def contains_review_type(obj) -> bool:
    if isinstance(obj, dict):
        t = obj.get("@type") or obj.get("type")
        if isinstance(t, str) and t.lower()=="review":
            return True
        if isinstance(t, list) and any(isinstance(x,str) and x.lower()=="review" for x in t):
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
                if not txt: continue
                data = json.loads(txt)
                if contains_review_type(data):
                    return True
            except Exception:
                continue
        kw = soup.find("meta", attrs={"name":"keywords"})
        if kw and kw.get("content") and "review" in kw["content"].lower():
            return True
    if looks_like_review_url(url):
        return True
    if html and re.search(r"\breview\b", html, re.I):
        return True
    return False

def extract_body_text(html: str, max_chars: int=4000) -> str:
    if not html:
        return ""
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script","style","noscript"]):
            tag.extract()
        return soup.get_text(" ", strip=True)[:max_chars]
    return normalize_ws(re.sub(r"<[^>]+>"," ",html))[:max_chars]


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

def match_movie(movies: List[Movie], headline: str, body: str) -> Tuple[Optional[str], float]:
    h = norm_text(headline)
    b = norm_text(body)
    best_title=None; best=0.0
    for m in movies:
        keys = movie_keys(m)
        for k in keys:
            if k and k in h:
                return m.title, 1.0
        for k in keys:
            if k and k in b:
                best_title=m.title; best=max(best,0.7)
        toks=[t for t in norm_text(m.title).split() if len(t)>=4]
        if toks:
            overlap=sum(1 for t in toks if t in h)
            if overlap/max(1,len(toks))>=0.75:
                best_title=m.title; best=max(best,0.5)
    return best_title, best

def any_movie_in_text(movies: List[Movie], text: str) -> bool:
    t = norm_text(text)
    for m in movies:
        for k in movie_keys(m):
            if k and k in t:
                return True
    return False


# ----------------------------
# Google Sheets
# ----------------------------

def get_gspread_client() -> gspread.Client:
    sa_json = base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8")
    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def ensure_feed_headers(ws: gspread.Worksheet) -> None:
    vals = [v.strip() for v in (ws.row_values(1) or [])]
    if vals == FEED_HEADERS:
        return
    ws.update("A1", [FEED_HEADERS])

def read_sheet_as_dicts(ws: gspread.Worksheet) -> List[Dict[str, str]]:
    rows = ws.get_all_values()
    if not rows or len(rows)<2:
        return []
    header = [h.strip() for h in rows[0]]
    out=[]
    for r in rows[1:]:
        rec={}
        for i,h in enumerate(header):
            rec[h]=r[i].strip() if i < len(r) else ""
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

def max_pages_for_type(t: str) -> int:
    t=(t or "").strip().lower()
    if t=="publication_review": return MAX_NEXT_PAGES_REVIEW
    if t=="publication_with_author": return MAX_NEXT_PAGES_AUTHOR
    if t=="publication_home": return MAX_NEXT_PAGES_HOME
    return 2

def load_movies(ws_movies) -> List[Movie]:
    rows = read_sheet_as_dicts(ws_movies)
    out=[]
    for r in rows:
        if not truthy(r.get("enabled","")): continue
        title=normalize_ws(r.get("movie_title",""))
        if not title: continue
        year_s=normalize_ws(r.get("release_year",""))
        year=int(year_s) if year_s.isdigit() else None
        aliases=parse_aliases(r.get("aliases",""))
        out.append(Movie(title, year, aliases))
    return out

def load_sources(ws_sources) -> List[SourceRow]:
    rows = read_sheet_as_dicts(ws_sources)
    out=[]
    for r in rows:
        if not truthy(r.get("enabled","")): continue
        pub=normalize_ws(r.get("publication",""))
        src=normalize_ws(r.get("source_url",""))
        if not pub or not src: continue
        out.append(SourceRow(pub, normalize_ws(r.get("author","")), src, normalize_ws(r.get("source_url_type",""))))
    return out


# ----------------------------
# Crawl
# ----------------------------

def crawl_source(session, conn, limiter, metrics, s: SourceRow) -> List[LinkCandidate]:
    max_next = max_pages_for_type(s.source_url_type)
    candidates=[]
    url=s.source_url
    seen_pages=set()

    for _ in range(max_next+1):
        if not url or url in seen_pages:
            break
        seen_pages.add(url)

        etag, last_mod = get_source_headers(conn, url)
        headers={"User-Agent":USER_AGENT,"Accept":"text/html,application/xhtml+xml"}
        if etag: headers["If-None-Match"]=etag
        if last_mod: headers["If-Modified-Since"]=last_mod

        resp = http_get(session, limiter, metrics, url, headers=headers)
        metrics.source_pages_fetched += 1
        metrics.maybe_print()

        set_source_headers(conn, url, resp.headers.get("ETag"), resp.headers.get("Last-Modified"))
        final_url = resp.url or url

        if resp.status_code == 304:
            metrics.source_pages_304 += 1
            break
        if resp.status_code >= 400:
            break

        resp.encoding = resp.encoding or "utf-8"
        html = resp.text

        links=extract_links(html, final_url)
        for c in links:
            if not same_domain(c.url, s.source_url): continue
            if looks_like_review_url(c.url):
                candidates.append(c)

        nxt = find_next_page(html, final_url)
        if nxt and same_domain(nxt, s.source_url):
            url = nxt
        else:
            break

    # dedup
    out=[]; seen=set()
    for c in candidates:
        if c.url not in seen:
            seen.add(c.url); out.append(c)
    metrics.candidates_seen += len(candidates)
    metrics.candidates_deduped_seen += len(out)
    return out


# ----------------------------
# Article worker pool (safe-ish concurrency)
# ----------------------------

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_and_match_article(session: requests.Session, limiter: DomainLimiter, metrics: Metrics,
                            movies: List[Movie], source: SourceRow, cand: LinkCandidate) -> Optional[Dict[str,str]]:
    # NOTE: limiter is shared; this keeps per-domain delays even with threads (good).
    metrics.candidates_fetched_articles += 1
    headers={"User-Agent":USER_AGENT,"Accept":"text/html,application/xhtml+xml"}
    resp = http_get(session, limiter, metrics, cand.url, headers=headers)
    metrics.article_pages_fetched += 1
    metrics.maybe_print()
    if resp.status_code >= 400:
        return None
    resp.encoding = resp.encoding or "utf-8"
    html = resp.text

    headline = extract_headline(html, fallback=cand.anchor_text)
    if not headline:
        return None
    if not looks_like_review_article(html, cand.url, headline):
        return None
    metrics.candidates_confirmed_reviews += 1

    body = extract_body_text(html)
    matched, conf = match_movie(movies, headline, body)
    if not matched or conf < 0.5:
        return None
    metrics.candidates_matched_movies += 1

    return {
        "movie_title": matched,
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="crawler_state.sqlite")
    ap.add_argument("--max-new-per-run", type=int, default=MAX_NEW_PER_RUN)
    args = ap.parse_args()

    if not SHEET_ID or not GOOGLE_SA_JSON_B64:
        raise SystemExit("Missing SHEET_ID or GOOGLE_SA_JSON_B64")

    metrics = Metrics(started_ts=time.time())
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
        raise SystemExit("No enabled movies in Movies sheet")

    sources = load_sources(ws_sources)
    if not sources:
        raise SystemExit("No enabled sources in Sources sheet")

    metrics.sources_total = len(sources)

    rows_to_append=[]
    new_count=0
    article_budget = MAX_ARTICLE_FETCHES_PER_RUN

    # Crawl sources sequentially (polite), but fetch articles in a small pool
    for s in sources:
        metrics.sources_done += 1
        metrics.maybe_print()

        candidates = crawl_source(session, conn, limiter, metrics, s)

        # pre-filter: remove already seen
        cand2=[]
        for c in candidates:
            if already_seen(conn, c.url):
                continue
            # optional: only fetch article if anchor already mentions a movie (major speed win)
            if REQUIRE_MOVIE_IN_ANCHOR_BEFORE_FETCH and c.anchor_text:
                if not any_movie_in_text(movies, c.anchor_text):
                    continue
            cand2.append(c)

        # apply budget
        if article_budget <= 0:
            continue
        cand2 = cand2[:article_budget]
        article_budget -= len(cand2)

        # small pool for articles
        if not cand2:
            continue

        with ThreadPoolExecutor(max_workers=max(1, ARTICLE_WORKERS)) as ex:
            futs = [ex.submit(fetch_and_match_article, session, limiter, metrics, movies, s, c) for c in cand2]
            for fut in as_completed(futs):
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

                if new_count >= args.max_new_per_run:
                    break

        if new_count >= args.max_new_per_run:
            break

    if rows_to_append:
        append_rows(ws_feed, rows_to_append)
        metrics.appended_rows += len(rows_to_append)
        rows_to_append.clear()

    conn.commit()

    metrics.maybe_print(force=True)
    print("\nRun complete.")
    print(f"New matched reviews appended: {new_count}")

if __name__ == "__main__":
    main()
