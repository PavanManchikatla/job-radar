from __future__ import annotations

import argparse
import hashlib
import html
import ipaddress
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import math
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Tuple, Dict, Any, Set
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import tz


# =========================
# CONFIG
# =========================
TIMEZONE = "America/Chicago"

'''ROLE_KEYWORDS = [
    # Core health informatics roles
    "health informatics",
    "healthcare informatics",
    "clinical informatics",
    "medical informatics",
    "biomedical informatics",
    
    # Healthcare data & analytics roles
    "healthcare data analyst",
    "healthcare data scientist",
    "clinical data analyst",
    "clinical data scientist",
    "healthcare analyst",
    "clinical analyst",
    "population health analyst",
    "public health analyst",
    "health analytics",
    
    # Healthcare data engineering
    "healthcare data engineer",
    "clinical data engineer",
    "health data engineer",
    
    # Clinical systems & IT
    "clinical systems analyst",
    "health it analyst",
    "healthcare it specialist",
    "ehr analyst",
    "ehr developer",
    "epic analyst",
    "cerner analyst",
    "clinical applications analyst",
    
    # Research informatics
    "clinical research informatics",
    "research informatics",
    "translational informatics",
    "clinical research analyst",
    "clinical research coordinator",  # Often has data components
    
    # BI & reporting in healthcare
    "healthcare business intelligence",
    "clinical bi developer",
    "healthcare bi analyst",
    
    # Specialized domains
    "bioinformatics",  # Genomics/computational biology focused
    "pharmacy informatics",
    "nursing informatics",
    "dental informatics",
    "radiology informatics",
    "pathology informatics",
    
    # Manager/Lead roles
    "health informatics manager",
    "clinical informatics director",
    "healthcare analytics manager",
    "director of health informatics",
    
    # Adjacent/broader roles with healthcare focus
    "data scientist - healthcare",
    "data analyst - healthcare",
    "ml engineer - healthcare",
    "software engineer - healthcare",
]

EXCLUDE_KEYWORDS = [
    "intern", "internship", "co-op", "contract", "temporary", "part-time"
]
'''
ROLE_KEYWORDS = [
    # Core data roles
    "data scientist",
    "data analyst",
    "data engineer",
    "analytics engineer",
    "business intelligence",
    "bi developer",
    
    # ML/AI roles (flexible matching)
    "machine learning",  # Catches: "ML Engineer", "Software Engineer - Machine Learning", etc.
    "ml engineer",
    "ai engineer",
    "artificial intelligence",
    
    # Research roles
    "research scientist",
    "applied scientist",
    "research engineer",
    "machine learning researcher",
    "ai researcher",
    
    # Quant roles
    "quantitative analyst",
    "quantitative researcher",
    "quant developer",
    
    # Manager/Lead roles (if you want them)
    "data science manager",
    "analytics manager",
    "ml manager",
    
    # Broader software engineering roles with data/ML focus
    "software engineer, ml",
    "software engineer - machine learning",
    "software engineer - ai",
    "backend engineer - data",
]

EXCLUDE_KEYWORDS = [
    "intern", "internship", "co-op", "contract", "temporary", "part-time"
]

ALLOW_US = True
ALLOW_REMOTE = True

COLLECT_EVERY_MINUTES = 20

DB_PATH = "jobs.db"
EXPORT_DIR = Path("exports")
VALID_DIR = Path("validated_sources")
STATE_DIR = Path("state")
PIPELINE_STATE_PATH = STATE_DIR / "pipeline_state.json"

DEFAULT_MASTER_LIST_LOCAL = Path("sources/companies.txt")
EXTRA_MASTER_LIST_LOCAL = Path("sources/companies_extra.txt")
EXTRA_MASTER_LISTS_DIR = Path("sources/company_lists")

VALIDATOR_WORKERS = 25
VALIDATOR_TIMEOUT_SEC = 20

# Max days to consider a job posting as "fresh"
MAX_POSTING_AGE_DAYS = 90

USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY", "")
USAJOBS_USER_AGENT_EMAIL = os.getenv("USAJOBS_USER_AGENT_EMAIL", "")

DEFAULT_COLLECT_WORKERS = 16
DEFAULT_MIN_REQUEST_INTERVAL_SEC = 0.5

SESSION_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "job-radar/2.1 (+respectful-rate-limits)"
}
_THREAD_LOCAL = threading.local()
_HOST_SAFETY_CACHE: Dict[str, bool] = {}
_HOST_SAFETY_LOCK = threading.Lock()

BLOCKED_NETWORKS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
]


class RequestThrottler:
    """Simple host-level throttler shared across worker threads."""

    def __init__(self, default_min_interval_sec: float = 0.5):
        self.default_min_interval_sec = max(0.0, float(default_min_interval_sec))
        self._next_allowed: Dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, key: str, min_interval_sec: Optional[float] = None) -> None:
        interval = self.default_min_interval_sec if min_interval_sec is None else max(0.0, float(min_interval_sec))
        if interval <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                next_allowed = self._next_allowed.get(key, 0.0)
                if now >= next_allowed:
                    self._next_allowed[key] = now + interval
                    return
                sleep_for = next_allowed - now
            time.sleep(min(max(sleep_for, 0.001), 1.0))

    def defer(self, key: str, delay_sec: float) -> None:
        delay = max(0.0, float(delay_sec))
        if delay <= 0:
            return
        with self._lock:
            target = time.monotonic() + delay
            current = self._next_allowed.get(key, 0.0)
            if target > current:
                self._next_allowed[key] = target


def _ip_is_blocked(ip_str: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip_str)
    except ValueError:
        return True
    if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_multicast:
        return True
    if ip_obj.is_reserved or ip_obj.is_unspecified:
        return True
    for network in BLOCKED_NETWORKS:
        if ip_obj in network:
            return True
    return False


def _hostname_is_safe(hostname: str) -> bool:
    host = (hostname or "").strip().lower()
    if not host:
        return False

    with _HOST_SAFETY_LOCK:
        cached = _HOST_SAFETY_CACHE.get(host)
    if cached is not None:
        return cached

    try:
        infos = socket.getaddrinfo(host, None)
        if not infos:
            return False
        blocked = False
        for info in infos:
            sockaddr = info[4]
            ip_str = sockaddr[0] if sockaddr else ""
            if _ip_is_blocked(ip_str):
                blocked = True
                break
    except Exception:
        # DNS/network failure: treat as unsafe for this request, but do not poison cache.
        return False

    safe = not blocked

    with _HOST_SAFETY_LOCK:
        _HOST_SAFETY_CACHE[host] = safe
    return safe


def _validate_request_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Blocked non-http(s) URL: {url}")
    if not parsed.hostname:
        raise ValueError(f"Blocked URL with no hostname: {url}")
    if not _hostname_is_safe(parsed.hostname):
        raise ValueError(f"Blocked unsafe hostname: {parsed.hostname}")
    return parsed.netloc or "unknown-host"


class RobotsChecker:
    """Cache and evaluate robots.txt permissions for polite web scraping."""

    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self._cache: Dict[str, Tuple[robotparser.RobotFileParser, Optional[float]]] = {}
        self._lock = threading.Lock()

    def can_fetch(
        self,
        url: str,
        *,
        throttler: Optional[RequestThrottler] = None,
        min_request_interval_sec: Optional[float] = None,
    ) -> Tuple[bool, Optional[float]]:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return False, None

        base = f"{parsed.scheme}://{parsed.netloc}"
        with self._lock:
            cached = self._cache.get(base)
        if cached is None:
            cached = self._load_robots(base, throttler=throttler, min_request_interval_sec=min_request_interval_sec)
            with self._lock:
                self._cache[base] = cached

        rp, crawl_delay = cached
        return rp.can_fetch(self.user_agent, url), crawl_delay

    def _load_robots(
        self,
        base: str,
        *,
        throttler: Optional[RequestThrottler],
        min_request_interval_sec: Optional[float],
    ) -> Tuple[robotparser.RobotFileParser, Optional[float]]:
        rp = robotparser.RobotFileParser()
        robots_url = f"{base}/robots.txt"
        try:
            text = http_get_text_with_retry(
                robots_url,
                timeout=15,
                max_retries=2,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            )
            rp.set_url(robots_url)
            rp.parse((text or "").splitlines())
            crawl_delay = rp.crawl_delay(self.user_agent)
            if crawl_delay is None:
                crawl_delay = rp.crawl_delay("*")
            return rp, float(crawl_delay) if isinstance(crawl_delay, (int, float)) else None
        except Exception:
            # If robots is unavailable, keep default permissive behavior and use configured pacing.
            rp.parse([])
            return rp, None


def _get_http_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(SESSION_HEADERS)
        _THREAD_LOCAL.session = session
    return session


def _parse_retry_after_seconds(retry_after: Optional[str]) -> Optional[float]:
    if not retry_after:
        return None
    raw = retry_after.strip()
    if raw.isdigit():
        return max(0.0, float(raw))
    try:
        parsed = parsedate_to_datetime(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz.UTC)
    now_utc = datetime.now(tz.UTC)
    return max(0.0, (parsed.astimezone(tz.UTC) - now_utc).total_seconds())


def _retry_sleep_seconds(attempt: int, retry_after: Optional[str] = None) -> float:
    parsed = _parse_retry_after_seconds(retry_after)
    if parsed is not None:
        return min(parsed, 60.0)
    return float(min(2 * (attempt + 1), 30))


def http_get_json_with_retry(
    url: str,
    *,
    params: Optional[dict] = None,
    timeout: int = 30,
    max_retries: int = 5,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> Any:
    """GET JSON with basic backoff. Safe for GitHub Actions / flaky networks.

    Retries on: timeouts, connection errors, 429, 5xx.
    """
    host_key = _validate_request_url(url)

    for attempt in range(max_retries):
        try:
            if throttler:
                throttler.wait(host_key, min_request_interval_sec)
            resp = _get_http_session().get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                sleep_s = _retry_sleep_seconds(attempt, resp.headers.get("Retry-After"))
                if throttler:
                    throttler.defer(host_key, sleep_s)
                else:
                    time.sleep(sleep_s)
                continue
            if 500 <= resp.status_code < 600:
                sleep_s = _retry_sleep_seconds(attempt)
                if throttler:
                    throttler.defer(host_key, sleep_s)
                else:
                    time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.Timeout, requests.ConnectionError):
            sleep_s = _retry_sleep_seconds(attempt)
            if throttler:
                throttler.defer(host_key, sleep_s)
            else:
                time.sleep(sleep_s)
            continue
        except ValueError:
            # JSON parse error
            raise

    # last attempt (surface real error)
    if throttler:
        throttler.wait(host_key, min_request_interval_sec)
    resp = _get_http_session().get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def http_get_text_with_retry(
    url: str,
    *,
    params: Optional[dict] = None,
    timeout: int = 30,
    max_retries: int = 5,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> str:
    """GET text with backoff and host-level throttling."""
    host_key = _validate_request_url(url)
    request_headers = {
        "Accept": "text/html,application/xhtml+xml,text/plain,application/json;q=0.8,*/*;q=0.5"
    }

    for attempt in range(max_retries):
        try:
            if throttler:
                throttler.wait(host_key, min_request_interval_sec)
            resp = _get_http_session().get(url, params=params, timeout=timeout, headers=request_headers)
            if resp.status_code == 429:
                sleep_s = _retry_sleep_seconds(attempt, resp.headers.get("Retry-After"))
                if throttler:
                    throttler.defer(host_key, sleep_s)
                else:
                    time.sleep(sleep_s)
                continue
            if 500 <= resp.status_code < 600:
                sleep_s = _retry_sleep_seconds(attempt)
                if throttler:
                    throttler.defer(host_key, sleep_s)
                else:
                    time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.text or ""
        except (requests.Timeout, requests.ConnectionError):
            sleep_s = _retry_sleep_seconds(attempt)
            if throttler:
                throttler.defer(host_key, sleep_s)
            else:
                time.sleep(sleep_s)
            continue

    if throttler:
        throttler.wait(host_key, min_request_interval_sec)
    resp = _get_http_session().get(url, params=params, timeout=timeout, headers=request_headers)
    resp.raise_for_status()
    return resp.text or ""


def http_post_json_with_retry(
    url: str,
    *,
    json_payload: Optional[dict] = None,
    timeout: int = 30,
    max_retries: int = 5,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> Any:
    """POST JSON with backoff and host-level throttling."""
    host_key = _validate_request_url(url)
    request_headers = {"Accept": "application/json", "Content-Type": "application/json"}

    for attempt in range(max_retries):
        try:
            if throttler:
                throttler.wait(host_key, min_request_interval_sec)
            resp = _get_http_session().post(
                url,
                json=json_payload or {},
                timeout=timeout,
                headers=request_headers,
            )
            if resp.status_code == 429:
                sleep_s = _retry_sleep_seconds(attempt, resp.headers.get("Retry-After"))
                if throttler:
                    throttler.defer(host_key, sleep_s)
                else:
                    time.sleep(sleep_s)
                continue
            if 500 <= resp.status_code < 600:
                sleep_s = _retry_sleep_seconds(attempt)
                if throttler:
                    throttler.defer(host_key, sleep_s)
                else:
                    time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.Timeout, requests.ConnectionError):
            sleep_s = _retry_sleep_seconds(attempt)
            if throttler:
                throttler.defer(host_key, sleep_s)
            else:
                time.sleep(sleep_s)
            continue
        except ValueError:
            raise

    if throttler:
        throttler.wait(host_key, min_request_interval_sec)
    resp = _get_http_session().post(
        url,
        json=json_payload or {},
        timeout=timeout,
        headers=request_headers,
    )
    resp.raise_for_status()
    return resp.json()


ROBOTS_CHECKER = RobotsChecker(user_agent=SESSION_HEADERS["User-Agent"])


# =========================
# Helpers
# =========================
def now_local() -> datetime:
    return datetime.now(tz.gettz(TIMEZONE))

def to_iso_utc(dt: datetime) -> str:
    return dt.astimezone(tz.UTC).isoformat()

def parse_any_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = pd.to_datetime(value, utc=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def contains_any(text: str, keywords: Iterable[str]) -> bool:
    t = normalize_text(text)
    return any(normalize_text(k) in t for k in keywords)

def location_is_us_or_remote(location: str) -> bool:
    loc = normalize_text(location)

    # Remote
    if ALLOW_REMOTE and ("remote" in loc or "work from home" in loc or "wfh" in loc):
        return True

    if not ALLOW_US:
        return True

    # Common US patterns
    if "united states" in loc:
        return True

    # SmartRecruiters format: "Seattle WA US"
    if loc.endswith(" us") or " usa" in loc or loc.endswith(" usa"):
        return True

    # State abbreviations with comma (Greenhouse/Lever)
    if re.search(r",\s*(al|ak|az|ar|ca|co|ct|de|fl|ga|hi|ia|id|il|in|ks|ky|la|ma|md|me|mi|mn|mo|ms|mt|nc|nd|ne|nh|nj|nm|nv|ny|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|va|vt|wa|wi|wv|wy)\b", loc):
        return True

    return False


def score_job(title: str, location: str, description: str) -> int:
    s = 0
    t = normalize_text(title)
    loc = normalize_text(location)
    desc = normalize_text(description or "")

    # Location bonuses
    if "remote" in loc:
        s += 2
    
    # Seniority adjustment (CONFIGURABLE)
    # With 5 years experience, you might actually qualify for some senior roles!
    # Options:
    # 1. Keep penalty but reduce it: -1 instead of -3
    # 2. Remove penalty entirely (comment out these lines)
    # 3. Add bonus for mid-level: "ii", "iii", "2", "3" → +1
    if any(x in t for x in ["senior", "staff", "principal", "lead"]):
        s -= 1  # Changed from -3 to -1 (less harsh)
    
    # Mid-level bonus (you have the experience for these!)
    if any(x in t for x in [" ii", " iii", " 2", " 3"]):
        s += 1
    
    # Role type bonuses
    if "engineer" in t:
        s += 1
    if "scientist" in t:
        s += 1
    if "analyst" in t:
        s += 1

    # Skills bonuses (from description)
    skill_keywords = [
        ("python", 1), ("r programming", 1), ("sql", 1), 
        ("spark", 1), ("aws", 1), ("azure", 1), ("gcp", 1),
        ("tableau", 1), ("power bi", 1), ("looker", 1),
        ("pytorch", 1), ("tensorflow", 1), ("scikit-learn", 1),
        ("healthcare", 2), ("medical", 2), ("clinical", 2),
        ("education", 1), ("university", 1), ("research", 1),
    ]
    
    for kw, pts in skill_keywords:
        if kw in desc:
            s += pts

    return s

def is_stale_posting(posted_at: Optional[datetime]) -> bool:
    """Check if job posting is too old"""
    if not posted_at:
        return False
    
    age = now_local() - posted_at.astimezone(tz.gettz(TIMEZONE))
    return age.days > MAX_POSTING_AGE_DAYS

def ensure_dirs() -> None:
    VALID_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

def resolve_master_companies_file() -> Path:
    env_path = os.getenv("COMPANIES_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return DEFAULT_MASTER_LIST_LOCAL


def resolve_company_list_files(primary: Path) -> List[Path]:
    files: List[Path] = [primary]

    env_extra = os.getenv("COMPANIES_EXTRA_FILES", "").strip()
    if env_extra:
        for raw in env_extra.split(","):
            p = Path(raw.strip())
            if raw.strip():
                files.append(p)

    files.append(EXTRA_MASTER_LIST_LOCAL)
    if EXTRA_MASTER_LISTS_DIR.exists():
        files.extend(sorted(EXTRA_MASTER_LISTS_DIR.glob("*.txt")))

    seen: Set[str] = set()
    ordered: List[Path] = []
    for p in files:
        key = str(p.resolve()) if p.exists() else str(p)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(p)
    return ordered


# =========================
# Data model
# =========================
@dataclass
class Job:
    source: str
    job_id: str
    title: str
    company: str
    location: str
    url: str
    posted_at: Optional[datetime]
    first_seen_at: datetime
    description: str = ""

    @property
    def posted_at_iso(self) -> Optional[str]:
        return to_iso_utc(self.posted_at) if self.posted_at else None

    @property
    def first_seen_at_iso(self) -> str:
        return to_iso_utc(self.first_seen_at)


# =========================
# DB
# =========================
def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            source TEXT NOT NULL,
            job_id TEXT NOT NULL,
            title TEXT NOT NULL,
            company TEXT NOT NULL,
            location TEXT,
            url TEXT NOT NULL,
            posted_at TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            description TEXT,
            score INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (source, job_id)
        )
        """)
        # Backward-compatible migration for older DBs.
        cols = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if "last_seen_at" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN last_seen_at TEXT")
            conn.execute("UPDATE jobs SET last_seen_at = first_seen_at WHERE last_seen_at IS NULL")
            conn.execute("UPDATE jobs SET last_seen_at = ? WHERE last_seen_at IS NULL", (to_iso_utc(now_local()),))
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON jobs(first_seen_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON jobs(posted_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_score ON jobs(score)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company)")

def upsert_jobs(jobs: List[Job]) -> Tuple[int, int, int, int]:
    """Returns (inserted, updated_existing, skipped_stale, skipped_invalid)"""
    inserted = 0
    skipped_stale = 0
    updated_existing = 0
    skipped_invalid = 0
    
    now_seen_iso = to_iso_utc(now_local())

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        for j in jobs:
            # Basic sanity checks (fool-proofing)
            if not (j.source and j.job_id and j.title and j.company and j.url):
                skipped_invalid += 1
                continue

            # Skip stale postings
            if is_stale_posting(j.posted_at):
                skipped_stale += 1
                continue

            sc = score_job(j.title, j.location, j.description)

            # Insert or update last_seen_at for existing rows (no duplicates across runs).
            cur = conn.execute(
                """
                INSERT INTO jobs
                (source, job_id, title, company, location, url, posted_at, first_seen_at, last_seen_at, description, score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, job_id) DO UPDATE SET
                    last_seen_at = excluded.last_seen_at,
                    -- keep the most informative posted_at if it was missing before
                    posted_at = COALESCE(jobs.posted_at, excluded.posted_at),
                    -- allow description to be filled in later if empty
                    description = CASE
                        WHEN (jobs.description IS NULL OR jobs.description = '') AND (excluded.description IS NOT NULL AND excluded.description != '')
                        THEN excluded.description
                        ELSE jobs.description
                    END,
                    -- score can evolve if description is filled in later
                    score = MAX(jobs.score, excluded.score)
                """,
                (
                    j.source,
                    j.job_id,
                    j.title,
                    j.company,
                    j.location,
                    j.url,
                    j.posted_at_iso,
                    j.first_seen_at_iso,
                    now_seen_iso,
                    j.description,
                    sc,
                ),
            )
            # sqlite doesn't directly tell insert vs update reliably; use rowcount heuristic.
            # For INSERT it is typically 1, for UPDATE it is also 1.
            # We'll infer using a SELECT existence check only when needed.
            # Cheap approach: if it conflicted, we count it as updated.
            # Detect conflict by checking if it already existed before attempt.
            # (We do it with a quick SELECT.)
            existed = conn.execute(
                "SELECT 1 FROM jobs WHERE source=? AND job_id=? AND first_seen_at < ? LIMIT 1",
                (j.source, j.job_id, j.first_seen_at_iso),
            ).fetchone()
            if existed:
                updated_existing += 1
            else:
                inserted += 1

    return inserted, updated_existing, skipped_stale, skipped_invalid


# =========================
# Master list loader
# =========================
def load_master_company_list(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Master company list not found at: {path}\n"
            f"Create it at sources/companies.txt or set COMPANIES_FILE=/path/to/companies.txt"
        )

    input_files = [p for p in resolve_company_list_files(path) if p.exists()]

    items: List[str] = []
    for input_file in input_files:
        for raw in input_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#") or line.startswith("//"):
                continue
            line = line.split("#", 1)[0].split("//", 1)[0].strip()
            if not line:
                continue
            items.append(line)

    seen = set()
    out = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def save_list(path: Path, items: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(items) + ("\n" if items else ""), encoding="utf-8")


# =========================
# Job filters
# =========================
def job_passes_filters(title: str, location: str) -> bool:
    if not contains_any(title, ROLE_KEYWORDS):
        return False
    if contains_any(title, EXCLUDE_KEYWORDS):
        return False
    if location and not location_is_us_or_remote(location):
        return False
    return True


# =========================
# Validators
# =========================
VALID_GREENHOUSE = VALID_DIR / "greenhouse_valid.txt"
INVALID_GREENHOUSE = VALID_DIR / "greenhouse_invalid.txt"
VALID_LEVER = VALID_DIR / "lever_valid.txt"
INVALID_LEVER = VALID_DIR / "lever_invalid.txt"
VALID_SMART = VALID_DIR / "smartrecruiters_valid.txt"
INVALID_SMART = VALID_DIR / "smartrecruiters_invalid.txt"
VALID_ASHBY = VALID_DIR / "ashby_valid.txt"
INVALID_ASHBY = VALID_DIR / "ashby_invalid.txt"
VALID_WEBSCRAPE = VALID_DIR / "webscrape_valid.txt"
CAREER_URL_MAPPINGS = VALID_DIR / "career_url_mappings.txt"

JSON_LD_SCRIPT_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
ANCHOR_RE = re.compile(
    r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")
JOB_URL_HINTS = ("job", "jobs", "career", "careers", "position", "opening", "opportunit", "requisition")
GENERIC_LINK_TEXTS = {
    "learn more",
    "read more",
    "details",
    "apply",
    "apply now",
    "view",
    "view details",
    "more",
}

def _http_get_json(url: str, timeout: int, params: Optional[dict] = None) -> Any:
    _validate_request_url(url)
    r = _get_http_session().get(url, timeout=timeout, params=params)
    if r.status_code != 200:
        raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
    return r.json()

def validate_greenhouse_token(token: str) -> Tuple[bool, str]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    try:
        data = _http_get_json(url, timeout=VALIDATOR_TIMEOUT_SEC)
        if not isinstance(data, dict) or "jobs" not in data or not isinstance(data["jobs"], list):
            return False, "bad_json_shape"
        
        # Check if any jobs match our filters
        jobs = data.get("jobs", [])
        if not jobs:
            return False, "no_jobs"
        
        for job in jobs[:50]:  # Check first 50 jobs
            title = job.get("title", "")
            loc = ((job.get("location") or {}).get("name", "")) or ""
            if job_passes_filters(title, loc):
                return True, "ok"
        
        return False, "no_matching_jobs"
    except Exception:
        raise

def validate_lever_token(token: str) -> Tuple[bool, str]:
    url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    try:
        data = _http_get_json(url, timeout=VALIDATOR_TIMEOUT_SEC)
        if not isinstance(data, list):
            return False, "bad_json_shape"
        
        if not data:
            return False, "no_jobs"
        
        # Check if any jobs match our filters
        for job in data[:50]:  # Check first 50 jobs
            title = job.get("text") or job.get("title") or ""
            loc = (job.get("categories") or {}).get("location", "") or ""
            if job_passes_filters(title, loc):
                return True, "ok"
        
        return False, "no_matching_jobs"
    except Exception:
        raise

def validate_smartrecruiters_token(token: str) -> Tuple[bool, str]:
    """
    IMPROVED: Check if company has jobs that match our filters
    """
    url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
    params = {"limit": 100, "offset": 0}
    
    try:
        data = _http_get_json(url, timeout=VALIDATOR_TIMEOUT_SEC, params=params)
        
        if not isinstance(data, dict):
            return False, "bad_json_shape"
        
        # PostingList usually has: offset, limit, totalFound, content
        if "content" not in data or not isinstance(data["content"], list):
            return False, "missing_content"
        
        total = data.get("totalFound", None)
        if not isinstance(total, int):
            return False, "missing_totalFound"
        
        if total <= 0:
            return False, "no_postings"
        
        # NEW: Check if any jobs match our filters
        jobs = data.get("content", [])
        for job in jobs:
            title = job.get("name", "")
            loc_obj = job.get("location") or {}
            location = " ".join([
                x for x in [
                    loc_obj.get("city"),
                    loc_obj.get("region"),
                    loc_obj.get("country")
                ] if x
            ]) or ""
            
            if job_passes_filters(title, location):
                return True, "ok"  # Found at least 1 matching job!
        
        return False, "no_matching_jobs"
    
    except Exception:
        raise

def validate_ashby_token(token: str) -> Tuple[bool, str]:
    """
    Validate Ashby job board. Format: https://jobs.ashbyhq.com/{company}
    """
    url = f"https://jobs.ashbyhq.com/{token}"
    
    try:
        # Ashby returns HTML, but we can check if the page loads
        _validate_request_url(url)
        resp = _get_http_session().get(url, timeout=VALIDATOR_TIMEOUT_SEC)
        if resp.status_code != 200:
            raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
        
        # Try the API endpoint which returns JSON
        api_url = f"https://jobs.ashbyhq.com/{token}/jobs"
        data = _http_get_json(api_url, timeout=VALIDATOR_TIMEOUT_SEC)
        
        if not isinstance(data, dict):
            return False, "bad_json_shape"
        
        jobs = data.get("jobs", [])
        if not isinstance(jobs, list):
            return False, "bad_json_shape"
        
        if len(jobs) == 0:
            return False, "no_jobs"
        
        # Check if any jobs match our filters
        for job in jobs[:50]:
            title = job.get("title", "")
            location_name = job.get("locationName", "") or ""
            
            if job_passes_filters(title, location_name):
                return True, "ok"
        
        return False, "no_matching_jobs"
    
    except Exception:
        raise


def _run_validator_parallel(
    name: str,
    tokens: List[str],
    validator: Callable[[str], Tuple[bool, str]],
    valid_out: Path,
    invalid_out: Path,
    max_workers: int,
) -> List[str]:
    valid: List[str] = []
    invalid: List[Tuple[str, str]] = []

    def task(tok: str) -> Tuple[str, bool, str]:
        try:
            ok, reason = validator(tok)
            return tok, ok, reason
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            return tok, False, f"http_{code}"
        except Exception:
            return tok, False, "exception"

    print(f"[validate:{name}] Starting validation of {len(tokens)} companies...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(task, t): t for t in tokens}
        completed = 0
        for fut in as_completed(futures):
            tok, ok, reason = fut.result()
            completed += 1
            
            if ok:
                valid.append(tok)
            else:
                invalid.append((tok, reason))
            
            # Progress update every 100 companies
            if completed % 100 == 0:
                print(f"[validate:{name}] Progress: {completed}/{len(tokens)} ({len(valid)} valid so far)")

    valid_set = set(valid)
    valid_ordered = [t for t in tokens if t in valid_set]

    save_list(valid_out, valid_ordered)
    invalid_out.parent.mkdir(parents=True, exist_ok=True)
    invalid_out.write_text(
        "\n".join([f"{tok}\t{reason}" for tok, reason in invalid]) + ("\n" if invalid else ""),
        encoding="utf-8",
    )

    print(f"[validate:{name}] COMPLETE: total={len(tokens)} valid={len(valid_ordered)} invalid={len(invalid)}")
    
    # Show breakdown of invalid reasons
    reason_counts = {}
    for _, reason in invalid:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    
    if reason_counts:
        print(f"[validate:{name}] Invalid reasons breakdown:")
        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            print(f"  - {reason}: {count}")
    
    return valid_ordered

def validate_all_from_master_list() -> Dict[str, List[str]]:
    ensure_dirs()
    master_path = resolve_master_companies_file()
    tokens = load_master_company_list(master_path)
    print(f"\n{'='*70}")
    print(f"VALIDATION STARTING")
    print(f"{'='*70}")
    print(f"Master list: {master_path}")
    print(f"Total companies to validate: {len(tokens)}")
    print(f"Workers: {VALIDATOR_WORKERS}")
    print(f"Timeout per request: {VALIDATOR_TIMEOUT_SEC}s")
    print(f"{'='*70}\n")

    valid_gh = _run_validator_parallel(
        "greenhouse", tokens, validate_greenhouse_token, 
        VALID_GREENHOUSE, INVALID_GREENHOUSE, VALIDATOR_WORKERS
    )
    print()
    
    valid_lv = _run_validator_parallel(
        "lever", tokens, validate_lever_token, 
        VALID_LEVER, INVALID_LEVER, VALIDATOR_WORKERS
    )
    print()
    
    valid_sr = _run_validator_parallel(
        "smartrecruiters", tokens, validate_smartrecruiters_token, 
        VALID_SMART, INVALID_SMART, VALIDATOR_WORKERS
    )
    print()
    
    valid_ashby = _run_validator_parallel(
        "ashby", tokens, validate_ashby_token,
        VALID_ASHBY, INVALID_ASHBY, VALIDATOR_WORKERS
    )
    print()

    print(f"\n{'='*70}")
    print(f"VALIDATION SUMMARY")
    print(f"{'='*70}")
    print(f"Greenhouse:      {len(valid_gh):4d} valid out of {len(tokens)}")
    print(f"Lever:           {len(valid_lv):4d} valid out of {len(tokens)}")
    print(f"SmartRecruiters: {len(valid_sr):4d} valid out of {len(tokens)}")
    print(f"Ashby:           {len(valid_ashby):4d} valid out of {len(tokens)}")
    print(f"{'='*70}")
    print(f"Total unique companies ready to scrape: {len(set(valid_gh + valid_lv + valid_sr + valid_ashby))}")
    print(f"{'='*70}\n")

    return {
        "greenhouse": valid_gh, 
        "lever": valid_lv, 
        "smartrecruiters": valid_sr,
        "ashby": valid_ashby
    }

def load_validated_sources() -> Tuple[List[str], List[str], List[str], List[str]]:
    def load_if_exists(p: Path) -> List[str]:
        if not p.exists():
            return []
        return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]

    return (
        load_if_exists(VALID_GREENHOUSE), 
        load_if_exists(VALID_LEVER), 
        load_if_exists(VALID_SMART),
        load_if_exists(VALID_ASHBY)
    )


def load_career_url_mappings(path: Path = CAREER_URL_MAPPINGS) -> Dict[str, str]:
    if not path.exists():
        return {}
    out: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "|" not in line:
            continue
        company, url = line.split("|", 1)
        company = company.strip()
        url = url.strip()
        if not company or not url:
            continue
        out[company] = url
    return out


def load_webscrape_sources() -> Tuple[List[str], Dict[str, str]]:
    companies: List[str] = []
    if VALID_WEBSCRAPE.exists():
        companies = [ln.strip() for ln in VALID_WEBSCRAPE.read_text(encoding="utf-8").splitlines() if ln.strip()]

    mappings = load_career_url_mappings()
    filtered = [company for company in companies if company in mappings]
    return filtered, mappings


def _strip_html(raw: str) -> str:
    text = TAG_RE.sub(" ", raw or "")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _extract_jobposting_nodes(data: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(data, dict):
        data_type_raw = data.get("@type")
        if isinstance(data_type_raw, list):
            data_types = {str(x).strip() for x in data_type_raw}
        else:
            data_types = {str(data_type_raw).strip()} if data_type_raw is not None else set()
        if "JobPosting" in data_types:
            yield data
        graph = data.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                yield from _extract_jobposting_nodes(item)
        for v in data.values():
            if isinstance(v, (dict, list)):
                yield from _extract_jobposting_nodes(v)
    elif isinstance(data, list):
        for item in data:
            yield from _extract_jobposting_nodes(item)


def _jobposting_location(posting: Dict[str, Any]) -> str:
    loc_parts: List[str] = []
    job_location = posting.get("jobLocation")
    job_location_type = normalize_text(str(posting.get("jobLocationType", "")))
    if "telecommute" in job_location_type or "remote" in job_location_type:
        return "Remote"

    locations = job_location if isinstance(job_location, list) else [job_location]
    for loc in locations:
        if not isinstance(loc, dict):
            continue
        address = loc.get("address", {}) if isinstance(loc.get("address"), dict) else {}
        for key in ("addressLocality", "addressRegion", "addressCountry"):
            val = address.get(key)
            if val:
                loc_parts.append(str(val))
    if loc_parts:
        return ", ".join(dict.fromkeys(loc_parts))
    return ""


def _extract_jobs_from_json_ld(company: str, base_url: str, html_text: str, seen: datetime) -> List[Job]:
    out: List[Job] = []
    for match in JSON_LD_SCRIPT_RE.findall(html_text):
        block = html.unescape((match or "").strip())
        if not block:
            continue
        if block.startswith("<!--"):
            block = block.removeprefix("<!--").removesuffix("-->").strip()
        try:
            data = json.loads(block)
        except Exception:
            continue

        for posting in _extract_jobposting_nodes(data):
            title = str(posting.get("title") or "").strip()
            if not title:
                continue

            job_url = str(posting.get("url") or "").strip()
            if job_url:
                job_url = urljoin(base_url, job_url)
            else:
                job_url = base_url

            location = _jobposting_location(posting)
            description = _strip_html(str(posting.get("description") or ""))
            posted_at = parse_any_datetime(str(posting.get("datePosted") or ""))

            identifier = posting.get("identifier")
            job_id = ""
            if isinstance(identifier, dict):
                job_id = str(
                    identifier.get("value")
                    or identifier.get("@id")
                    or identifier.get("name")
                    or ""
                ).strip()
            elif isinstance(identifier, str):
                job_id = identifier.strip()
            if not job_id:
                digest = hashlib.sha1(f"{company}|{job_url}|{title}".encode("utf-8")).hexdigest()
                job_id = digest[:24]

            if not job_passes_filters(title, location):
                continue

            out.append(
                Job(
                    source="webscrape",
                    job_id=job_id,
                    title=title,
                    company=company,
                    location=location,
                    url=job_url,
                    posted_at=posted_at,
                    first_seen_at=seen,
                    description=description,
                )
            )
    return out


def _extract_jobs_from_links(company: str, base_url: str, html_text: str, seen: datetime) -> List[Job]:
    out: List[Job] = []
    for href, raw_text in ANCHOR_RE.findall(html_text):
        title = _strip_html(raw_text)
        if len(title) < 6:
            continue
        if normalize_text(title) in GENERIC_LINK_TEXTS:
            continue

        abs_url = urljoin(base_url, (href or "").strip())
        parsed = urlparse(abs_url)
        if parsed.scheme not in ("http", "https"):
            continue
        url_norm = normalize_text(abs_url)
        if not any(hint in url_norm for hint in JOB_URL_HINTS):
            continue
        if not job_passes_filters(title, ""):
            continue

        digest = hashlib.sha1(f"{company}|{abs_url}|{title}".encode("utf-8")).hexdigest()
        out.append(
            Job(
                source="webscrape",
                job_id=digest[:24],
                title=title,
                company=company,
                location="",
                url=abs_url,
                posted_at=None,
                first_seen_at=seen,
                description="",
            )
        )
    return out


def _relabel_company(jobs: List[Job], company: str) -> List[Job]:
    relabeled: List[Job] = []
    for j in jobs:
        if j.company == company:
            relabeled.append(j)
            continue
        relabeled.append(
            Job(
                source=j.source,
                job_id=j.job_id,
                title=j.title,
                company=company,
                location=j.location,
                url=j.url,
                posted_at=j.posted_at,
                first_seen_at=j.first_seen_at,
                description=j.description,
            )
        )
    return relabeled


def _extract_greenhouse_board_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    if "greenhouse.io" not in host:
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return None
    if parts[0] in {"boards", "job-boards"} and len(parts) >= 2:
        return parts[1]
    return parts[0]


def _extract_lever_company_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    if "lever.co" not in host:
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return None
    if host.startswith("jobs.") or host.endswith(".lever.co"):
        return parts[0]
    return None


def _extract_smartrecruiters_company_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    parts = [p for p in parsed.path.split("/") if p]
    if "smartrecruiters.com" not in host:
        return None
    if host.startswith("jobs.") and parts:
        return parts[0]
    if "companies" in parts:
        idx = parts.index("companies")
        if len(parts) > idx + 1:
            return parts[idx + 1]
    return None


def _extract_ashby_company_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    if "ashbyhq.com" not in host:
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return None
    return parts[0]


def _extract_workday_job_id(url_or_path: str, title: str) -> str:
    path = urlparse(url_or_path).path or url_or_path
    m = re.search(r"([A-Z]{1,4}-\d{3,}|\bR-\d{3,}\b|\bJR-?\d{3,}\b)", path, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    digest = hashlib.sha1(f"{url_or_path}|{title}".encode("utf-8")).hexdigest()
    return digest[:24]


def _stable_job_id(seed: str) -> str:
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]


def _extract_icims_base_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    if "icims.com" not in host:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    return f"{parsed.scheme}://{host}"


def _extract_jobvite_company_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    parts = [p for p in parsed.path.split("/") if p]
    if "jobvite.com" not in host:
        return None
    if host.startswith("jobs.") and parts:
        return parts[0]
    if parts:
        return parts[0]
    return None


def _extract_bamboohr_host_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    if "bamboohr.com" not in host:
        return None
    return host if parsed.scheme in ("http", "https") else None


def _extract_workable_account_from_url(career_url: str) -> Optional[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    parts = [p for p in parsed.path.split("/") if p]
    if host == "apply.workable.com" and parts:
        return parts[0]
    if host.endswith(".workable.com"):
        label = host.split(".", 1)[0].strip()
        return label or (parts[0] if parts else None)
    return None


def _parse_rss_items(xml_text: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return out
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        location = ""
        for child in list(item):
            tag = (child.tag or "").lower()
            if tag.endswith("location"):
                location = (child.text or "").strip()
                break
        out.append({"title": title, "url": link, "pub_date": pub_date, "location": location})
    return out


def _robots_allows(
    url: str,
    *,
    throttler: Optional[RequestThrottler],
    min_request_interval_sec: Optional[float],
) -> bool:
    allowed, _ = ROBOTS_CHECKER.can_fetch(
        url,
        throttler=throttler,
        min_request_interval_sec=min_request_interval_sec,
    )
    return allowed


def _workday_candidate_endpoints(career_url: str) -> List[str]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    if "myworkdayjobs.com" not in host:
        return []

    scheme = parsed.scheme or "https"
    parts = [p for p in parsed.path.split("/") if p]
    tenant_candidates: List[str] = []
    site_candidates: List[str] = []

    first_label = host.split(".", 1)[0].strip()
    if first_label:
        tenant_candidates.append(first_label)

    if parts:
        site_candidates.append(parts[-1])
        if len(parts) >= 2 and re.match(r"^[a-z]{2}-[A-Z]{2}$", parts[0]):
            site_candidates.append(parts[1])
        site_candidates.append(parts[0])

    if "recruiting" in parts:
        idx = parts.index("recruiting")
        if len(parts) > idx + 2:
            tenant_candidates.append(parts[idx + 1])
            site_candidates.append(parts[idx + 2])

    tenant_candidates = [t for t in dict.fromkeys([x for x in tenant_candidates if x])]
    site_candidates = [s for s in dict.fromkeys([x for x in site_candidates if x])]

    endpoints: List[str] = []
    for tenant in tenant_candidates:
        for site in site_candidates:
            endpoints.append(f"{scheme}://{host}/wday/cxs/{tenant}/{site}/jobs")

    # Fallback common public site names.
    for tenant in tenant_candidates:
        for site in ("careers", "external", "externalsite", "jobsearch"):
            endpoints.append(f"{scheme}://{host}/wday/cxs/{tenant}/{site}/jobs")

    return list(dict.fromkeys(endpoints))


def fetch_workday_from_career_url(
    company: str,
    career_url: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    parsed = urlparse(career_url)
    host = (parsed.netloc or "").lower()
    if "myworkdayjobs.com" not in host:
        return []

    base = f"{parsed.scheme or 'https'}://{host}"
    endpoints = _workday_candidate_endpoints(career_url)
    seen = now_local()

    for endpoint in endpoints:
        if not _robots_allows(
            endpoint,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        ):
            continue
        out: List[Job] = []
        limit = 20
        offset = 0
        max_pages = 50
        matched_endpoint = False

        for _ in range(max_pages):
            payload = {
                "appliedFacets": {},
                "limit": limit,
                "offset": offset,
                "searchText": "",
            }
            try:
                data = http_post_json_with_retry(
                    endpoint,
                    json_payload=payload,
                    timeout=30,
                    max_retries=4,
                    throttler=throttler,
                    min_request_interval_sec=min_request_interval_sec,
                )
            except Exception:
                break

            if not isinstance(data, dict):
                break

            postings = data.get("jobPostings") or data.get("job_postings") or []
            if not isinstance(postings, list):
                break
            if postings:
                matched_endpoint = True

            if not postings:
                break

            for item in postings:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title") or item.get("bulletTitle") or "").strip()
                if not title:
                    continue
                location = str(item.get("locationsText") or item.get("location") or "").strip()
                external_path = str(item.get("externalPath") or item.get("externalUrl") or "").strip()
                job_url = urljoin(base, external_path) if external_path else career_url
                posted_at = parse_any_datetime(str(item.get("postedOn") or item.get("postedOnDate") or ""))
                job_id = _extract_workday_job_id(external_path or job_url, title)

                if not job_passes_filters(title, location):
                    continue

                out.append(
                    Job(
                        source="workday",
                        job_id=job_id,
                        title=title,
                        company=company,
                        location=location,
                        url=job_url,
                        posted_at=posted_at,
                        first_seen_at=seen,
                        description="",
                    )
                )

            total = data.get("total")
            if isinstance(total, int) and (offset + limit) >= total:
                break
            if len(postings) < limit:
                break
            offset += limit

        if matched_endpoint or out:
            return out

    return []


def fetch_icims_from_career_url(
    company: str,
    career_url: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    base = _extract_icims_base_from_url(career_url)
    if not base:
        return []

    seen = now_local()
    candidate_urls = [
        career_url,
        f"{base}/jobs/search?ss=1",
        f"{base}/jobs/search",
    ]
    deduped: Dict[str, Job] = {}

    for candidate in dict.fromkeys(candidate_urls):
        if not _robots_allows(
            candidate,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        ):
            continue
        try:
            html_text = http_get_text_with_retry(
                candidate,
                timeout=25,
                max_retries=3,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            )
        except Exception:
            continue

        for href, raw_text in ANCHOR_RE.findall(html_text):
            abs_url = urljoin(base, (href or "").strip())
            path = (urlparse(abs_url).path or "").lower()
            if "/jobs/" not in path:
                continue

            title = _strip_html(raw_text)
            if len(title) < 6 or normalize_text(title) in GENERIC_LINK_TEXTS:
                continue
            if not job_passes_filters(title, ""):
                continue

            m = re.search(r"/jobs/(\d+)", path)
            job_id = m.group(1) if m else _stable_job_id(f"{company}|icims|{abs_url}|{title}")
            deduped[job_id] = Job(
                source="icims",
                job_id=job_id,
                title=title,
                company=company,
                location="",
                url=abs_url,
                posted_at=None,
                first_seen_at=seen,
                description="",
            )

    return list(deduped.values())


def _parse_jobvite_json(data: Any, company: str, seen: datetime) -> List[Job]:
    if isinstance(data, dict):
        jobs = data.get("jobs") or data.get("requisitions") or data.get("results") or []
    elif isinstance(data, list):
        jobs = data
    else:
        jobs = []

    out: List[Job] = []
    for item in jobs:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("name") or "").strip()
        if not title:
            continue
        location = str(item.get("location") or item.get("locationName") or "").strip()
        job_url = str(item.get("url") or item.get("jobUrl") or item.get("applyUrl") or "").strip()
        req_id = str(item.get("id") or item.get("jobId") or item.get("reqId") or "").strip()
        posted_at = parse_any_datetime(
            str(item.get("postedDate") or item.get("createdDate") or item.get("date") or "")
        )

        if not job_url:
            continue
        if not req_id:
            req_id = _stable_job_id(f"{company}|jobvite|{job_url}|{title}")
        if not job_passes_filters(title, location):
            continue

        out.append(
            Job(
                source="jobvite",
                job_id=req_id,
                title=title,
                company=company,
                location=location,
                url=job_url,
                posted_at=posted_at,
                first_seen_at=seen,
                description="",
            )
        )
    return out


def fetch_jobvite_from_career_url(
    company: str,
    career_url: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    slug = _extract_jobvite_company_from_url(career_url)
    if not slug:
        return []

    seen = now_local()
    endpoints = [
        f"https://jobs.jobvite.com/api/job/v1/search?company={slug}",
        f"https://jobs.jobvite.com/api/job/v1/search?company={slug}&count=500",
    ]
    for endpoint in endpoints:
        if not _robots_allows(
            endpoint,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        ):
            continue
        try:
            data = http_get_json_with_retry(
                endpoint,
                timeout=25,
                max_retries=3,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            )
        except Exception:
            continue
        parsed_jobs = _parse_jobvite_json(data, company, seen)
        if parsed_jobs:
            return parsed_jobs

    # Fallback to RSS if API endpoint is unavailable.
    rss_url = f"https://jobs.jobvite.com/{slug}/jobs/rss"
    if not _robots_allows(
        rss_url,
        throttler=throttler,
        min_request_interval_sec=min_request_interval_sec,
    ):
        return []
    try:
        xml_text = http_get_text_with_retry(
            rss_url,
            timeout=25,
            max_retries=3,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )
    except Exception:
        return []

    out: List[Job] = []
    for item in _parse_rss_items(xml_text):
        title = item.get("title", "")
        job_url = item.get("url", "")
        location = item.get("location", "")
        posted_at = parse_any_datetime(item.get("pub_date"))
        if not title or not job_url:
            continue
        if not job_passes_filters(title, location):
            continue
        job_id = _stable_job_id(f"{company}|jobvite|{job_url}|{title}")
        out.append(
            Job(
                source="jobvite",
                job_id=job_id,
                title=title,
                company=company,
                location=location,
                url=job_url,
                posted_at=posted_at,
                first_seen_at=seen,
                description="",
            )
        )
    return out


def fetch_bamboohr_from_career_url(
    company: str,
    career_url: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    host = _extract_bamboohr_host_from_url(career_url)
    if not host:
        return []
    base = f"https://{host}"
    seen = now_local()

    candidate_urls = [
        career_url,
        f"{base}/careers",
        f"{base}/careers/list",
    ]
    deduped: Dict[str, Job] = {}

    for candidate in dict.fromkeys(candidate_urls):
        if not _robots_allows(
            candidate,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        ):
            continue
        try:
            html_text = http_get_text_with_retry(
                candidate,
                timeout=25,
                max_retries=3,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            )
        except Exception:
            continue

        for href, raw_text in ANCHOR_RE.findall(html_text):
            abs_url = urljoin(base, (href or "").strip())
            path = (urlparse(abs_url).path or "").lower()
            if "/careers/" not in path:
                continue
            title = _strip_html(raw_text)
            if len(title) < 6 or normalize_text(title) in GENERIC_LINK_TEXTS:
                continue
            if not job_passes_filters(title, ""):
                continue

            m = re.search(r"/careers/([^/?#]+)", path)
            job_id = m.group(1) if m else _stable_job_id(f"{company}|bamboohr|{abs_url}|{title}")
            deduped[job_id] = Job(
                source="bamboohr",
                job_id=job_id,
                title=title,
                company=company,
                location="",
                url=abs_url,
                posted_at=None,
                first_seen_at=seen,
                description="",
            )

    return list(deduped.values())


def fetch_workable_from_career_url(
    company: str,
    career_url: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    account = _extract_workable_account_from_url(career_url)
    if not account:
        return []

    seen = now_local()
    endpoints = [
        f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true",
        f"https://apply.workable.com/api/v1/widget/accounts/{account}",
    ]

    for endpoint in endpoints:
        if not _robots_allows(
            endpoint,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        ):
            continue
        try:
            data = http_get_json_with_retry(
                endpoint,
                timeout=25,
                max_retries=3,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            )
        except Exception:
            continue

        if isinstance(data, dict):
            jobs = data.get("jobs") or data.get("results") or []
        elif isinstance(data, list):
            jobs = data
        else:
            jobs = []

        out: List[Job] = []
        for item in jobs:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("name") or "").strip()
            if not title:
                continue

            location = ""
            loc = item.get("location")
            if isinstance(loc, dict):
                location = ", ".join([str(x).strip() for x in [loc.get("city"), loc.get("country")] if x]).strip()
            elif isinstance(loc, str):
                location = loc.strip()

            shortcode = str(item.get("shortcode") or item.get("code") or item.get("id") or "").strip()
            job_url = str(item.get("url") or "").strip()
            if not job_url:
                if shortcode:
                    job_url = f"https://apply.workable.com/{account}/j/{shortcode}/"
                else:
                    job_url = career_url

            posted_at = parse_any_datetime(str(item.get("published") or item.get("created_at") or ""))
            job_id = shortcode or _stable_job_id(f"{company}|workable|{job_url}|{title}")

            if not job_passes_filters(title, location):
                continue

            out.append(
                Job(
                    source="workable",
                    job_id=job_id,
                    title=title,
                    company=company,
                    location=location,
                    url=job_url,
                    posted_at=posted_at,
                    first_seen_at=seen,
                    description="",
                )
            )
        if out:
            return out

    # Fallback: parse job links from the public page.
    if not _robots_allows(
        career_url,
        throttler=throttler,
        min_request_interval_sec=min_request_interval_sec,
    ):
        return []
    try:
        html_text = http_get_text_with_retry(
            career_url,
            timeout=25,
            max_retries=3,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )
    except Exception:
        return []

    out: List[Job] = []
    for href, raw_text in ANCHOR_RE.findall(html_text):
        abs_url = urljoin(career_url, (href or "").strip())
        path = (urlparse(abs_url).path or "").lower()
        if "/j/" not in path:
            continue
        title = _strip_html(raw_text)
        if len(title) < 6 or not job_passes_filters(title, ""):
            continue
        job_id = _stable_job_id(f"{company}|workable|{abs_url}|{title}")
        out.append(
            Job(
                source="workable",
                job_id=job_id,
                title=title,
                company=company,
                location="",
                url=abs_url,
                posted_at=None,
                first_seen_at=seen,
                description="",
            )
        )
    return out


def _fetch_jobs_from_known_ats(
    company: str,
    career_url: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> Optional[List[Job]]:
    board = _extract_greenhouse_board_from_url(career_url)
    if board:
        return _relabel_company(
            fetch_greenhouse(
                board,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            ),
            company,
        )

    lever_company = _extract_lever_company_from_url(career_url)
    if lever_company:
        return _relabel_company(
            fetch_lever(
                lever_company,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            ),
            company,
        )

    sr_company = _extract_smartrecruiters_company_from_url(career_url)
    if sr_company:
        return _relabel_company(
            fetch_smartrecruiters(
                sr_company,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            ),
            company,
        )

    ashby_company = _extract_ashby_company_from_url(career_url)
    if ashby_company:
        return _relabel_company(
            fetch_ashby(
                ashby_company,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            ),
            company,
        )

    if "myworkdayjobs.com" in (urlparse(career_url).netloc or "").lower():
        return fetch_workday_from_career_url(
            company,
            career_url,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )

    host = (urlparse(career_url).netloc or "").lower()
    if "icims.com" in host:
        return fetch_icims_from_career_url(
            company,
            career_url,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )

    if "jobvite.com" in host:
        return fetch_jobvite_from_career_url(
            company,
            career_url,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )

    if "bamboohr.com" in host:
        return fetch_bamboohr_from_career_url(
            company,
            career_url,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )

    if "workable.com" in host:
        return fetch_workable_from_career_url(
            company,
            career_url,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )

    return None


# =========================
# Connectors
# =========================
def fetch_greenhouse(
    board: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
    data = http_get_json_with_retry(
        url,
        timeout=30,
        throttler=throttler,
        min_request_interval_sec=min_request_interval_sec,
    )

    out: List[Job] = []
    seen = now_local()

    for item in data.get("jobs", []):
        job_id = str(item.get("id", "")).strip()
        if not job_id:
            continue
        title = item.get("title", "") or ""
        abs_url = item.get("absolute_url", "") or ""
        loc = ((item.get("location") or {}).get("name", "")) or ""

        # Greenhouse: updated_at exists and is usually decent
        posted_at = parse_any_datetime(item.get("updated_at"))

        if not job_passes_filters(title, loc):
            continue

        out.append(Job("greenhouse", job_id, title, board, loc, abs_url, posted_at, seen, ""))
    return out

def fetch_lever(
    company: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    url = f"https://api.lever.co/v0/postings/{company}?mode=json"
    data = http_get_json_with_retry(
        url,
        timeout=30,
        throttler=throttler,
        min_request_interval_sec=min_request_interval_sec,
    )

    out: List[Job] = []
    seen = now_local()

    for item in data:
        job_id = str(item.get("id", "")).strip()
        if not job_id:
            continue

        title = item.get("text") or item.get("title") or ""
        abs_url = item.get("hostedUrl") or item.get("applyUrl") or ""
        loc = (item.get("categories") or {}).get("location", "") or ""
        description = item.get("descriptionPlain") or item.get("description") or ""

        posted_at = None
        created_ms = item.get("createdAt")
        if isinstance(created_ms, (int, float)):
            posted_at = datetime.fromtimestamp(created_ms / 1000, tz=tz.UTC)

        if not job_passes_filters(title, loc):
            continue

        out.append(Job("lever", job_id, title, company, loc, abs_url, posted_at, seen, description))
    return out

def fetch_smartrecruiters(
    company: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    """
    SmartRecruiters postings endpoint returns a ListResult object:
    { offset, limit, totalFound, content: [...] }

    IMPORTANT: always pass limit/offset; some companies return empty content
    without an explicit limit. Also handle pagination.
    """
    out: List[Job] = []
    seen = now_local()

    limit = 100
    offset = 0
    max_pages = 50  # safety cap

    def get_page(off: int) -> dict:
        url = f"https://api.smartrecruiters.com/v1/companies/{company}/postings"
        params = {"limit": limit, "offset": off}
        data = http_get_json_with_retry(
            url,
            params=params,
            timeout=30,
            max_retries=5,
            throttler=throttler,
            min_request_interval_sec=min_request_interval_sec,
        )
        return data if isinstance(data, dict) else {}

    for _ in range(max_pages):
        data = get_page(offset)
        if not isinstance(data, dict):
            break

        content = data.get("content", [])
        if not isinstance(content, list) or len(content) == 0:
            break

        for item in content:
            job_id = str(item.get("id", "")).strip()
            if not job_id:
                continue

            title = item.get("name", "") or ""

            # Prefer a human-posting URL if present; otherwise keep ref.
            abs_url = item.get("postingUrl") or item.get("ref") or ""

            loc_obj = item.get("location") or {}
            loc = " ".join([x for x in [loc_obj.get("city"), loc_obj.get("region"), loc_obj.get("country")] if x]) or ""

            posted_at = parse_any_datetime(item.get("releasedDate"))

            if not job_passes_filters(title, loc):
                continue

            out.append(Job(
                source="smartrecruiters",
                job_id=job_id,
                title=title,
                company=company,
                location=loc,
                url=abs_url,
                posted_at=posted_at,
                first_seen_at=seen,
                description=""
            ))

        total = data.get("totalFound")
        if isinstance(total, int) and (offset + limit) >= total:
            break

        offset += limit

    return out


def fetch_ashby(
    company: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    """
    Fetch jobs from Ashby ATS. Format: https://jobs.ashbyhq.com/{company}/jobs
    Returns JSON with jobs array.
    """
    url = f"https://jobs.ashbyhq.com/{company}/jobs"
    data = http_get_json_with_retry(
        url,
        timeout=30,
        throttler=throttler,
        min_request_interval_sec=min_request_interval_sec,
    )

    out: List[Job] = []
    seen = now_local()

    jobs = data.get("jobs", [])
    if not isinstance(jobs, list):
        return out

    for item in jobs:
        job_id = str(item.get("id", "")).strip()
        if not job_id:
            continue

        title = item.get("title", "") or ""
        location_name = item.get("locationName", "") or ""
        
        # Ashby job URL
        job_url = item.get("jobUrl") or f"https://jobs.ashbyhq.com/{company}/{job_id}"
        
        # Posted date
        posted_at = parse_any_datetime(item.get("publishedDate"))

        if not job_passes_filters(title, location_name):
            continue

        out.append(Job(
            source="ashby",
            job_id=job_id,
            title=title,
            company=company,
            location=location_name,
            url=job_url,
            posted_at=posted_at,
            first_seen_at=seen,
            description=""
        ))

    return out


def fetch_webscrape(
    company: str,
    career_url: str,
    *,
    throttler: Optional[RequestThrottler] = None,
    min_request_interval_sec: Optional[float] = None,
) -> List[Job]:
    allowed, crawl_delay = ROBOTS_CHECKER.can_fetch(
        career_url,
        throttler=throttler,
        min_request_interval_sec=min_request_interval_sec,
    )
    if not allowed:
        return []

    effective_interval = max(
        0.0,
        float(min_request_interval_sec or 0.0),
        float(crawl_delay or 0.0),
    )

    ats_jobs = _fetch_jobs_from_known_ats(
        company,
        career_url,
        throttler=throttler,
        min_request_interval_sec=effective_interval,
    )
    if ats_jobs:
        deduped: Dict[str, Job] = {}
        for job in ats_jobs:
            deduped[f"{job.source}:{job.job_id}"] = job
        return list(deduped.values())

    html_text = http_get_text_with_retry(
        career_url,
        timeout=30,
        max_retries=4,
        throttler=throttler,
        min_request_interval_sec=effective_interval,
    )
    seen = now_local()

    jobs = _extract_jobs_from_json_ld(company, career_url, html_text, seen)
    if not jobs:
        jobs = _extract_jobs_from_links(company, career_url, html_text, seen)

    # Deduplicate within source fetch (same posting can appear in multiple widgets).
    deduped: Dict[str, Job] = {}
    for job in jobs:
        deduped[job.job_id] = job
    return list(deduped.values())




# =========================
# Collect
# =========================
def collect_once(
    *,
    days_back_posted: Optional[int] = None,
    require_posted_at: bool = False,
    max_per_source: Optional[int] = None,
    collect_workers: int = DEFAULT_COLLECT_WORKERS,
    min_request_interval_sec: float = DEFAULT_MIN_REQUEST_INTERVAL_SEC,
) -> None:
    ensure_dirs()
    init_db()

    gh, lv, sr, ashby = load_validated_sources()
    ws, ws_mapping = load_webscrape_sources()
    
    print(f"\n{'='*70}")
    print(f"COLLECTION STARTING")
    print(f"{'='*70}")
    print(f"Validated sources loaded:")
    print(f"  - Greenhouse:      {len(gh)} companies")
    print(f"  - Lever:           {len(lv)} companies")
    print(f"  - SmartRecruiters: {len(sr)} companies")
    print(f"  - Ashby:           {len(ashby)} companies")
    print(f"  - Webscrape:       {len(ws)} companies")
    print(f"  - Total:           {len(gh) + len(lv) + len(sr) + len(ashby) + len(ws)} companies")
    print(f"  - Parallel workers:{max(1, int(collect_workers))}")
    print(f"  - Min request gap: {max(0.0, float(min_request_interval_sec)):.2f}s per host")
    print(f"{'='*70}\n")
    
    if not gh and not lv and not sr and not ashby and not ws:
        print("[ERROR] No validated sources found!")
        print("Please run:")
        print("  python job_radar.py validate")
        print("  python webscrape_validator.py validate")
        return

    all_jobs: List[Job] = []
    errors: Dict[str, int] = {
        "greenhouse": 0,
        "lever": 0,
        "smartrecruiters": 0,
        "ashby": 0,
        "webscrape": 0,
    }

    cutoff_utc: Optional[datetime] = None
    if isinstance(days_back_posted, int) and days_back_posted > 0:
        cutoff_local = now_local() - timedelta(days=days_back_posted)
        cutoff_utc = cutoff_local.astimezone(tz.UTC)

    def keep_job(j: Job) -> bool:
        if require_posted_at and not j.posted_at:
            return False
        if cutoff_utc and j.posted_at and j.posted_at.astimezone(tz.UTC) < cutoff_utc:
            return False
        return True

    def extend_limited(jobs: List[Job]) -> None:
        if not jobs:
            return
        kept = [j for j in jobs if keep_job(j)]
        if max_per_source and max_per_source > 0:
            kept = kept[:max_per_source]
        all_jobs.extend(kept)

    collect_workers = max(1, int(collect_workers))
    min_request_interval_sec = max(0.0, float(min_request_interval_sec))
    throttler = RequestThrottler(default_min_interval_sec=min_request_interval_sec)

    source_totals = {
        "greenhouse": len(gh),
        "lever": len(lv),
        "smartrecruiters": len(sr),
        "ashby": len(ashby),
        "webscrape": len(ws),
    }
    completed_by_source = {k: 0 for k in source_totals}
    progress_every = 25

    tasks: List[Tuple[str, str, Callable[..., List[Job]], Tuple[Any, ...]]] = []
    tasks.extend([("greenhouse", board, fetch_greenhouse, (board,)) for board in gh])
    tasks.extend([("lever", company, fetch_lever, (company,)) for company in lv])
    tasks.extend([("smartrecruiters", company, fetch_smartrecruiters, (company,)) for company in sr])
    tasks.extend([("ashby", company, fetch_ashby, (company,)) for company in ashby])
    tasks.extend(
        [
            ("webscrape", company, fetch_webscrape, (company, ws_mapping[company]))
            for company in ws
            if company in ws_mapping
        ]
    )

    print("[collect] Fetching validated sources in parallel (with host-level throttling)...")
    with ThreadPoolExecutor(max_workers=collect_workers) as ex:
        futures = {
            ex.submit(
                fetcher,
                *args,
                throttler=throttler,
                min_request_interval_sec=min_request_interval_sec,
            ): (source, token)
            for source, token, fetcher, args in tasks
        }
        for fut in as_completed(futures):
            source, token = futures[fut]
            completed_by_source[source] += 1
            done = completed_by_source[source]
            total = source_totals[source]
            try:
                jobs = fut.result()
                extend_limited(jobs)
            except Exception as e:
                errors[source] += 1
                if errors[source] <= 5:
                    print(f"  [ERROR] [{source}] {token}: {e}")
            if (done % progress_every == 0) or (done == total):
                print(f"  [{source}] Progress: {done}/{total} ({len(all_jobs)} jobs so far)")

    # Insert into database
    inserted, updated_existing, skipped_stale, skipped_invalid = upsert_jobs(all_jobs)
    
    print(f"\n{'='*70}")
    print(f"COLLECTION COMPLETE")
    print(f"{'='*70}")
    print(f"Jobs fetched:        {len(all_jobs)}")
    print(f"Jobs inserted:       {inserted}")
    print(f"Jobs updated (seen): {updated_existing}")
    print(f"Jobs skipped (stale):{skipped_stale}")
    print(f"Jobs skipped (bad):  {skipped_invalid}")
    print(f"Errors:")
    print(f"  - Greenhouse:      {errors['greenhouse']}")
    print(f"  - Lever:           {errors['lever']}")
    print(f"  - SmartRecruiters: {errors['smartrecruiters']}")
    print(f"  - Ashby:           {errors['ashby']}")
    print(f"  - Webscrape:       {errors['webscrape']}")
    print(f"Timestamp:           {now_local().isoformat()}")
    print(f"{'='*70}\n")


# =========================
# Reporting: posted_at in last N days + dedupe vs last export
# =========================
def find_latest_export_file() -> Optional[Path]:
    if not EXPORT_DIR.exists():
        return None
    files = sorted(EXPORT_DIR.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def load_export_keys(path: Path) -> Set[str]:
    try:
        df_prev = pd.read_excel(path)
    except Exception:
        return set()
    if "source" not in df_prev.columns or "job_id" not in df_prev.columns:
        return set()
    return set((df_prev["source"].astype(str) + "::" + df_prev["job_id"].astype(str)).tolist())

def report_now(days: int) -> None:
    """
    Export jobs with posted_at within last `days` from the time you run it (local Chicago time).
    Excludes rows where posted_at is NULL (since you asked: posted in last N days).
    Also dedupes vs most recent prior export file.
    """
    ensure_dirs()
    init_db()

    current = now_local()
    cutoff_local = current - timedelta(days=days)

    cutoff_iso = to_iso_utc(cutoff_local)
    now_iso = to_iso_utc(current)

    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("""
            SELECT
                source, job_id, title, company, location, url,
                posted_at, first_seen_at, score
            FROM jobs
            WHERE posted_at IS NOT NULL
              AND posted_at >= ?
              AND posted_at < ?
            ORDER BY score DESC, posted_at DESC
        """, conn, params=[cutoff_iso, now_iso])

    prev_file = find_latest_export_file()
    prev_keys = load_export_keys(prev_file) if prev_file else set()

    if len(df) > 0 and prev_keys:
        cur_keys = (df["source"].astype(str) + "::" + df["job_id"].astype(str))
        df = df[~cur_keys.isin(prev_keys)].copy()

    stamp = current.strftime("%Y%m%d_%H%M")
    out_path = EXPORT_DIR / f"jobs_posted_last{days}d_{stamp}.xlsx"
    df.to_excel(out_path, index=False)

    print(f"\n{'='*70}")
    print(f"JOB REPORT (Posted Recency)")
    print(f"{'='*70}")
    print(f"Now (local):       {current.isoformat()}")
    print(f"Cutoff (local):    {cutoff_local.isoformat()}  (last {days} day(s))")
    if prev_file:
        print(f"Deduped vs file:   {prev_file.name}")
    else:
        print("Deduped vs file:   (none found, this is the first export)")
    print(f"Rows in Excel:     {len(df)}")
    print(f"Saved Excel:       {out_path}")
    print(f"{'='*70}\n")


# =========================
# GitHub UI exports (README + JSON)
# =========================
README_JOBS_START = "<!-- JOBS:START -->"
README_JOBS_END = "<!-- JOBS:END -->"
PAGES_DIR = EXPORT_DIR / "pages"


def _read_pipeline_state() -> Dict[str, Any]:
    if not PIPELINE_STATE_PATH.exists():
        return {"initialized": False}
    try:
        return json.loads(PIPELINE_STATE_PATH.read_text(encoding="utf-8")) or {"initialized": False}
    except Exception:
        # Corrupted state file: fail-safe to reinitialize.
        return {"initialized": False}


def _write_pipeline_state(state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    PIPELINE_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _query_jobs_for_feed(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return jobs for feed with dated postings first, then undated by recency seen."""
    init_db()

    sql = """
        SELECT
            source, job_id, title, company, location, url,
            posted_at, first_seen_at, last_seen_at, score
        FROM jobs
        ORDER BY
            CASE WHEN posted_at IS NULL THEN 1 ELSE 0 END,
            posted_at DESC,
            last_seen_at DESC
    """
    params: List[Any] = []
    if isinstance(limit, int) and limit > 0:
        sql += " LIMIT ?"
        params.append(limit)

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(sql, params).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "source": r[0],
                "job_id": r[1],
                "title": r[2],
                "company": r[3],
                "location": r[4] or "",
                "url": r[5],
                "posted_at": r[6],
                "first_seen_at": r[7],
                "last_seen_at": r[8],
                "score": r[9],
            }
        )
    return out


def export_jobs_json(out_path: Path, *, limit: Optional[int] = None) -> None:
    ensure_dirs()
    jobs = _query_jobs_for_feed(limit=limit)
    payload = {
        "generated_at": to_iso_utc(now_local()),
        "timezone": TIMEZONE,
        "count": len(jobs),
        "jobs": jobs,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _md_escape(text: str) -> str:
    return (text or "").replace("|", "\\|").strip()


def _render_jobs_table(jobs: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    lines.append("| Posted | Company | Title | Location | Source |")
    lines.append("|---|---|---|---|---|")

    for j in jobs:
        posted = (j.get("posted_at") or "")
        # Show just date part if ISO-like.
        posted_short = posted[:10] if len(posted) >= 10 else (posted or "—")
        company = _md_escape(j.get("company", ""))
        title = _md_escape(j.get("title", ""))
        loc = _md_escape(j.get("location", "")) or "—"
        source = _md_escape(j.get("source", ""))
        url = (j.get("url") or "").strip()

        title_cell = f"[{title}]({url})" if url else title
        lines.append(f"| {posted_short} | {company} | {title_cell} | {loc} | {source} |")
    return lines


def _render_page_nav(page_no: int, total_pages: int, *, from_readme: bool) -> str:
    if total_pages <= 1:
        return ""
    parts: List[str] = [f"Page {page_no}/{total_pages}"]
    if page_no > 1:
        prev_target = f"exports/pages/jobs_page_{page_no - 1}.md" if from_readme else f"jobs_page_{page_no - 1}.md"
        parts.append(f"[Prev]({prev_target})")
    if page_no < total_pages:
        next_target = f"exports/pages/jobs_page_{page_no + 1}.md" if from_readme else f"jobs_page_{page_no + 1}.md"
        parts.append(f"[Next]({next_target})")
    return " | ".join(parts)


def _write_paginated_job_pages(jobs: List[Dict[str, Any]], *, page_size: int) -> int:
    page_size = max(1, int(page_size))
    total_pages = max(1, int(math.ceil(len(jobs) / page_size)))
    PAGES_DIR.mkdir(parents=True, exist_ok=True)

    for page_no in range(1, total_pages + 1):
        chunk = jobs[(page_no - 1) * page_size : page_no * page_size]
        nav = _render_page_nav(page_no, total_pages, from_readme=False)
        lines: List[str] = [f"# Jobs Feed (Page {page_no}/{total_pages})", ""]
        lines.append(f"_Last updated: {now_local().strftime('%Y-%m-%d %H:%M %Z')}_")
        if nav:
            lines.extend(["", nav, ""])
        lines.extend(["", *(_render_jobs_table(chunk)), ""])
        if nav:
            lines.extend([nav, ""])
        lines.append("[Back to README](../../README.md)")
        (PAGES_DIR / f"jobs_page_{page_no}.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    # Remove stale page files from previous larger runs.
    for old in PAGES_DIR.glob("jobs_page_*.md"):
        m = re.match(r"jobs_page_(\d+)\.md$", old.name)
        if not m:
            continue
        if int(m.group(1)) > total_pages:
            old.unlink(missing_ok=True)

    return total_pages


def export_readme(
    *,
    readme_path: Path = Path("README.md"),
    limit: int = 500,
) -> None:
    """Inject page-1 jobs into README and generate paginated job pages."""
    ensure_dirs()
    jobs = _query_jobs_for_feed(limit=None)
    page_size = max(1, int(limit))
    total_pages = _write_paginated_job_pages(jobs, page_size=page_size)
    page_1 = jobs[:page_size]
    nav = _render_page_nav(1, total_pages, from_readme=True)
    now_stamp = now_local().strftime("%Y-%m-%d %H:%M %Z")
    lines: List[str] = [f"_Last updated: {now_stamp}_", ""]
    if nav:
        lines.extend([nav, ""])
    lines.extend(_render_jobs_table(page_1))
    if nav:
        lines.extend(["", nav])
    content = "\n".join(lines).rstrip() + "\n"

    if not readme_path.exists():
        # Fail-safe: create a minimal README.
        readme_path.write_text("# Job Radar\n\n" + README_JOBS_START + "\n" + README_JOBS_END + "\n", encoding="utf-8")

    readme_text = readme_path.read_text(encoding="utf-8")

    if (README_JOBS_START not in readme_text) or (README_JOBS_END not in readme_text):
        # Fail-safe: append a jobs section if markers are missing.
        readme_text = readme_text.rstrip() + "\n\n## Jobs\n\n" + README_JOBS_START + "\n" + README_JOBS_END + "\n"

    before = readme_text.split(README_JOBS_START, 1)[0]
    after = readme_text.split(README_JOBS_END, 1)[1]
    updated = before + README_JOBS_START + "\n" + content + README_JOBS_END + after
    readme_path.write_text(updated, encoding="utf-8")


def pipeline_update(
    *,
    first_run_days: int = 30,
    daily_days: int = 2,
    max_per_source: int = 200,
    readme_limit: int = 500,
    collect_workers: int = DEFAULT_COLLECT_WORKERS,
    min_request_interval_sec: float = DEFAULT_MIN_REQUEST_INTERVAL_SEC,
) -> None:
    """One command for GitHub Actions.

    - First ever run: ingest jobs posted in last `first_run_days` days.
    - Daily runs: ingest jobs posted in last `daily_days` days (safe buffer).
    - Dedupe is guaranteed by the DB primary key.
    - README shows page 1 (newest first) and links to paginated pages for all jobs.
    """
    ensure_dirs()
    state = _read_pipeline_state()
    initialized = bool(state.get("initialized"))

    days_back = daily_days if initialized else first_run_days
    print(f"[pipeline] initialized={initialized} -> ingest posted last {days_back} day(s)")

    collect_once(
        days_back_posted=days_back,
        require_posted_at=True,
        max_per_source=max_per_source,
        collect_workers=collect_workers,
        min_request_interval_sec=min_request_interval_sec,
    )

    # Export JSON for future dashboards / debugging
    export_jobs_json(EXPORT_DIR / "jobs.json")

    # Update README feed
    export_readme(limit=readme_limit)

    state.update(
        {
            "initialized": True,
            "last_run_at": to_iso_utc(now_local()),
            "days_back_used": days_back,
        }
    )
    _write_pipeline_state(state)


def full_refresh(
    *,
    web_validate_workers: int = 10,
    skip_web_validate: bool = False,
    skip_api_validate: bool = False,
    days_back_posted: Optional[int] = None,
    require_posted_at: bool = False,
    max_per_source: Optional[int] = None,
    collect_workers: int = DEFAULT_COLLECT_WORKERS,
    min_request_interval_sec: float = DEFAULT_MIN_REQUEST_INTERVAL_SEC,
    readme_limit: int = 500,
) -> None:
    """Run web validation + ATS validation + collection + exports in one command."""
    ensure_dirs()

    if not skip_web_validate:
        validator_path = Path(__file__).with_name("webscrape_validator.py")
        if not validator_path.exists():
            raise FileNotFoundError(f"Missing web validator: {validator_path}")
        cmd = [
            sys.executable,
            str(validator_path),
            "validate",
            "--workers",
            str(max(1, int(web_validate_workers))),
        ]
        print(f"\n[full-refresh] Running web source validation: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    if not skip_api_validate:
        print("\n[full-refresh] Running ATS token validation...")
        validate_all_from_master_list()

    print("\n[full-refresh] Collecting jobs...")
    collect_once(
        days_back_posted=days_back_posted,
        require_posted_at=require_posted_at,
        max_per_source=max_per_source,
        collect_workers=collect_workers,
        min_request_interval_sec=min_request_interval_sec,
    )

    print("\n[full-refresh] Exporting JSON + README...")
    export_jobs_json(EXPORT_DIR / "jobs.json")
    export_readme(limit=readme_limit)


# =========================
# Scheduler
# =========================
def run_scheduler() -> None:
    ensure_dirs()
    init_db()

    sched = BlockingScheduler(timezone=TIMEZONE)
    sched.add_job(collect_once, "interval", minutes=COLLECT_EVERY_MINUTES, id="collector")
    
    print(f"\n{'='*70}")
    print(f"SCHEDULER STARTED")
    print(f"{'='*70}")
    print(f"Collection interval: {COLLECT_EVERY_MINUTES} minutes")
    print(f"Timezone:            {TIMEZONE}")
    print(f"Next run:            {(now_local() + timedelta(minutes=COLLECT_EVERY_MINUTES)).isoformat()}")
    print(f"{'='*70}")
    print(f"\nTo generate reports, run in another terminal:")
    print(f"  python job_radar.py report-now --days 2")
    print(f"{'='*70}\n")
    
    sched.start()


# =========================
# CLI
# =========================
def _prompt_days() -> int:
    while True:
        raw = input("How many days back should the job be POSTED? (e.g., 2): ").strip()
        try:
            days = int(raw)
            if days <= 0:
                print("Enter a positive integer like 1, 2, 7.")
                continue
            return days
        except ValueError:
            print("Enter a number like 2 (no text).")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Job Radar v2: Intelligent job scraping across ATS platforms",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate companies (smart filtering)
  python job_radar.py validate
  
  # Collect jobs once
  python job_radar.py collect-once
  
  # Generate report for jobs posted in last 2 days
  python job_radar.py report-now --days 2
  
  # Full run: validate everything + collect + export
  python job_radar.py full-refresh --collect-workers 20 --min-request-interval 0.6
  
  # Run continuous collection (every 20 minutes)
  python job_radar.py run
        """
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("validate", help="Validate master companies list against ATS endpoints with smart filtering")
    p_collect = sub.add_parser("collect-once", help="Collect jobs once right now (uses validated_sources/*_valid.txt)")
    p_collect.add_argument("--days-back-posted", type=int, default=None, help="Only ingest jobs posted in the last N days")
    p_collect.add_argument("--require-posted-at", action="store_true", help="Skip jobs that don't provide posted date")
    p_collect.add_argument("--max-per-source", type=int, default=None, help="Safety cap: max jobs ingested per company/source fetch")
    p_collect.add_argument("--collect-workers", type=int, default=DEFAULT_COLLECT_WORKERS, help="Parallel fetch workers across validated companies")
    p_collect.add_argument("--min-request-interval", type=float, default=DEFAULT_MIN_REQUEST_INTERVAL_SEC, help="Minimum seconds between requests to the same host")

    p_report = sub.add_parser("report-now", help="Export jobs posted in last N days (deduped vs last export)")
    p_report.add_argument("--days", type=int, default=None, help="Days back (if omitted, prompts)")

    p_export_readme = sub.add_parser("export-readme", help="Update README feed + paginated pages from DB")
    p_export_readme.add_argument("--limit", type=int, default=500, help="Jobs per page (README page 1 + exports/pages)")
    p_export_readme.add_argument("--readme", type=str, default="README.md", help="Path to README.md")

    p_pipeline = sub.add_parser("pipeline-update", help="One-shot pipeline for GitHub Actions: ingest + export JSON + update README")
    p_pipeline.add_argument("--first-run-days", type=int, default=30, help="First ever run: ingest jobs posted in last N days")
    p_pipeline.add_argument("--daily-days", type=int, default=2, help="Daily runs: ingest jobs posted in last N days (buffer)")
    p_pipeline.add_argument("--max-per-source", type=int, default=200, help="Safety cap per company/source fetch")
    p_pipeline.add_argument("--readme-limit", type=int, default=500, help="Jobs per page for README/page exports")
    p_pipeline.add_argument("--collect-workers", type=int, default=DEFAULT_COLLECT_WORKERS, help="Parallel fetch workers across validated companies")
    p_pipeline.add_argument("--min-request-interval", type=float, default=DEFAULT_MIN_REQUEST_INTERVAL_SEC, help="Minimum seconds between requests to the same host")

    p_full = sub.add_parser("full-refresh", help="Run web validation + ATS validation + collection + exports")
    p_full.add_argument("--web-validate-workers", type=int, default=10, help="Workers for webscrape_validator.py validate")
    p_full.add_argument("--skip-web-validate", action="store_true", help="Skip running webscrape validator")
    p_full.add_argument("--skip-api-validate", action="store_true", help="Skip ATS token validation")
    p_full.add_argument("--days-back-posted", type=int, default=None, help="Only ingest jobs posted in the last N days")
    p_full.add_argument("--require-posted-at", action="store_true", help="Skip jobs without posted date")
    p_full.add_argument("--max-per-source", type=int, default=200, help="Safety cap per company/source fetch")
    p_full.add_argument("--collect-workers", type=int, default=DEFAULT_COLLECT_WORKERS, help="Parallel fetch workers across validated companies")
    p_full.add_argument("--min-request-interval", type=float, default=DEFAULT_MIN_REQUEST_INTERVAL_SEC, help="Minimum seconds between requests to the same host")
    p_full.add_argument("--readme-limit", type=int, default=500, help="Jobs per page for README/page exports")

    sub.add_parser("run", help="Run scheduler: collect periodically (report is manual)")

    args = parser.parse_args()

    if args.cmd == "validate":
        validate_all_from_master_list()
        print("\n✓ Validation complete! Check validated_sources/ for results.\n")
    elif args.cmd == "collect-once":
        collect_once(
            days_back_posted=args.days_back_posted,
            require_posted_at=bool(args.require_posted_at),
            max_per_source=args.max_per_source,
            collect_workers=args.collect_workers,
            min_request_interval_sec=args.min_request_interval,
        )
    elif args.cmd == "report-now":
        days = args.days if args.days is not None else _prompt_days()
        report_now(days=days)
    elif args.cmd == "export-readme":
        export_readme(readme_path=Path(args.readme), limit=args.limit)
    elif args.cmd == "pipeline-update":
        pipeline_update(
            first_run_days=args.first_run_days,
            daily_days=args.daily_days,
            max_per_source=args.max_per_source,
            readme_limit=args.readme_limit,
            collect_workers=args.collect_workers,
            min_request_interval_sec=args.min_request_interval,
        )
    elif args.cmd == "full-refresh":
        full_refresh(
            web_validate_workers=args.web_validate_workers,
            skip_web_validate=bool(args.skip_web_validate),
            skip_api_validate=bool(args.skip_api_validate),
            days_back_posted=args.days_back_posted,
            require_posted_at=bool(args.require_posted_at),
            max_per_source=args.max_per_source,
            collect_workers=args.collect_workers,
            min_request_interval_sec=args.min_request_interval,
            readme_limit=args.readme_limit,
        )
    elif args.cmd == "run":
        run_scheduler()
    else:
        raise RuntimeError("Unknown command")

if __name__ == "__main__":
    main()
