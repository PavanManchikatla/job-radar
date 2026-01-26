from __future__ import annotations

import argparse
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Tuple, Dict, Any, Set

import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import tz


# =========================
# CONFIG
# =========================
TIMEZONE = "America/Chicago"

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

DEFAULT_MASTER_LIST_LOCAL = Path("sources/companies.txt")

VALIDATOR_WORKERS = 25
VALIDATOR_TIMEOUT_SEC = 20

# Max days to consider a job posting as "fresh"
MAX_POSTING_AGE_DAYS = 90

USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY", "")
USAJOBS_USER_AGENT_EMAIL = os.getenv("USAJOBS_USER_AGENT_EMAIL", "")

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "job-radar/2.0"
})


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

def resolve_master_companies_file() -> Path:
    env_path = os.getenv("COMPANIES_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return DEFAULT_MASTER_LIST_LOCAL


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
            description TEXT,
            score INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (source, job_id)
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON jobs(first_seen_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON jobs(posted_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_score ON jobs(score)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company)")

def upsert_jobs(jobs: List[Job]) -> Tuple[int, int, int]:
    """Returns (inserted, skipped_duplicate, skipped_stale)"""
    inserted = 0
    skipped_duplicate = 0
    skipped_stale = 0
    
    with sqlite3.connect(DB_PATH) as conn:
        for j in jobs:
            # Skip stale postings
            if is_stale_posting(j.posted_at):
                skipped_stale += 1
                continue
            
            try:
                conn.execute("""
                    INSERT INTO jobs
                    (source, job_id, title, company, location, url, posted_at, first_seen_at, description, score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    j.source, j.job_id, j.title, j.company, j.location, j.url,
                    j.posted_at_iso, j.first_seen_at_iso, j.description,
                    score_job(j.title, j.location, j.description)
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                skipped_duplicate += 1
    
    return inserted, skipped_duplicate, skipped_stale


# =========================
# Master list loader
# =========================
def load_master_company_list(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Master company list not found at: {path}\n"
            f"Create it at sources/companies.txt or set COMPANIES_FILE=/path/to/companies.txt"
        )

    items: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
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

def _http_get_json(url: str, timeout: int, params: Optional[dict] = None) -> Any:
    r = SESSION.get(url, timeout=timeout, params=params)
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
        resp = SESSION.get(url, timeout=VALIDATOR_TIMEOUT_SEC)
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


# =========================
# Connectors
# =========================
def fetch_greenhouse(board: str) -> List[Job]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

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

def fetch_lever(company: str) -> List[Job]:
    url = f"https://api.lever.co/v0/postings/{company}?mode=json"
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

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

import time

def fetch_smartrecruiters(company: str) -> List[Job]:
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

        # small retry for rate limits
        for attempt in range(5):
            resp = SESSION.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = int(retry_after) if (retry_after and retry_after.isdigit()) else (2 * (attempt + 1))
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json()

        resp.raise_for_status()
        return {}

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


def fetch_ashby(company: str) -> List[Job]:
    """
    Fetch jobs from Ashby ATS. Format: https://jobs.ashbyhq.com/{company}/jobs
    Returns JSON with jobs array.
    """
    url = f"https://jobs.ashbyhq.com/{company}/jobs"
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

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




# =========================
# Collect
# =========================
def collect_once() -> None:
    ensure_dirs()
    init_db()

    gh, lv, sr, ashby = load_validated_sources()
    
    print(f"\n{'='*70}")
    print(f"COLLECTION STARTING")
    print(f"{'='*70}")
    print(f"Validated sources loaded:")
    print(f"  - Greenhouse:      {len(gh)} companies")
    print(f"  - Lever:           {len(lv)} companies")
    print(f"  - SmartRecruiters: {len(sr)} companies")
    print(f"  - Ashby:           {len(ashby)} companies")
    print(f"  - Total:           {len(gh) + len(lv) + len(sr) + len(ashby)} companies")
    print(f"{'='*70}\n")
    
    if not gh and not lv and not sr and not ashby:
        print("[ERROR] No validated sources found!")
        print("Please run: python job_radar.py validate")
        return

    all_jobs: List[Job] = []
    errors: Dict[str, int] = {"greenhouse": 0, "lever": 0, "smartrecruiters": 0, "ashby": 0}

    # Greenhouse
    print(f"[collect] Fetching from {len(gh)} Greenhouse boards...")
    for i, board in enumerate(gh, 1):
        try:
            jobs = fetch_greenhouse(board)
            all_jobs.extend(jobs)
            if (i % 10 == 0) or i == len(gh):
                print(f"  Progress: {i}/{len(gh)} boards ({len(all_jobs)} jobs so far)")
        except Exception as e:
            errors["greenhouse"] += 1
            if errors["greenhouse"] <= 5:  # Only show first 5 errors
                print(f"  [ERROR] {board}: {e}")

    # Lever
    print(f"\n[collect] Fetching from {len(lv)} Lever companies...")
    for i, company in enumerate(lv, 1):
        try:
            jobs = fetch_lever(company)
            all_jobs.extend(jobs)
            if (i % 10 == 0) or i == len(lv):
                print(f"  Progress: {i}/{len(lv)} companies ({len(all_jobs)} jobs so far)")
        except Exception as e:
            errors["lever"] += 1
            if errors["lever"] <= 5:
                print(f"  [ERROR] {company}: {e}")

    # SmartRecruiters
    print(f"\n[collect] Fetching from {len(sr)} SmartRecruiters companies...")
    for i, company in enumerate(sr, 1):
        try:
            jobs = fetch_smartrecruiters(company)
            all_jobs.extend(jobs)
            if (i % 10 == 0) or i == len(sr):
                print(f"  Progress: {i}/{len(sr)} companies ({len(all_jobs)} jobs so far)")
        except Exception as e:
            errors["smartrecruiters"] += 1
            if errors["smartrecruiters"] <= 5:
                print(f"  [ERROR] {company}: {e}")

    # Ashby
    print(f"\n[collect] Fetching from {len(ashby)} Ashby companies...")
    for i, company in enumerate(ashby, 1):
        try:
            jobs = fetch_ashby(company)
            all_jobs.extend(jobs)
            if (i % 10 == 0) or i == len(ashby):
                print(f"  Progress: {i}/{len(ashby)} companies ({len(all_jobs)} jobs so far)")
        except Exception as e:
            errors["ashby"] += 1
            if errors["ashby"] <= 5:
                print(f"  [ERROR] {company}: {e}")

    # Insert into database
    inserted, skipped_dup, skipped_stale = upsert_jobs(all_jobs)
    
    print(f"\n{'='*70}")
    print(f"COLLECTION COMPLETE")
    print(f"{'='*70}")
    print(f"Jobs fetched:        {len(all_jobs)}")
    print(f"Jobs inserted:       {inserted}")
    print(f"Jobs skipped (dup):  {skipped_dup}")
    print(f"Jobs skipped (stale):{skipped_stale}")
    print(f"Errors:")
    print(f"  - Greenhouse:      {errors['greenhouse']}")
    print(f"  - Lever:           {errors['lever']}")
    print(f"  - SmartRecruiters: {errors['smartrecruiters']}")
    print(f"  - Ashby:           {errors['ashby']}")
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
  
  # Run continuous collection (every 20 minutes)
  python job_radar.py run
        """
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("validate", help="Validate master companies list against ATS endpoints with smart filtering")
    sub.add_parser("collect-once", help="Collect jobs once right now (uses validated_sources/*_valid.txt)")

    p_report = sub.add_parser("report-now", help="Export jobs posted in last N days (deduped vs last export)")
    p_report.add_argument("--days", type=int, default=None, help="Days back (if omitted, prompts)")

    sub.add_parser("run", help="Run scheduler: collect periodically (report is manual)")

    args = parser.parse_args()

    if args.cmd == "validate":
        validate_all_from_master_list()
        print("\n✓ Validation complete! Check validated_sources/ for results.\n")
    elif args.cmd == "collect-once":
        collect_once()
    elif args.cmd == "report-now":
        days = args.days if args.days is not None else _prompt_days()
        report_now(days=days)
    elif args.cmd == "run":
        run_scheduler()
    else:
        raise RuntimeError("Unknown command")

if __name__ == "__main__":
    main()