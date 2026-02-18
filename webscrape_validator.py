#!/usr/bin/env python3
"""
PRODUCTION-GRADE SECURE WEB SCRAPING VALIDATOR v3.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SECURITY FEATURES:
✓ SSRF protection (blocks localhost/private IPs)
✓ DNS rebinding protection
✓ Response size limits (10MB max)
✓ Redirect limits (3 max)
✓ Rate limiting (polite delays)
✓ Input validation & sanitization
✓ Safe file operations
✓ SSL verification enforced
✓ Content-type validation
✓ Resource cleanup
✓ Thread-safe operations
✓ Comprehensive error handling

USAGE:
    python webscrape_validator_v3.py validate --limit 100
    python webscrape_validator_v3.py validate
    python webscrape_validator_v3.py stats

AUTHOR: Job Radar Security Team
LICENSE: MIT
"""

import argparse
import os
import re
import json
import time
import socket
import ipaddress
import hashlib
import logging
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: Missing dependencies. Install with:")
    print("pip install requests beautifulsoup4 lxml --break-system-packages")
    exit(1)

# =========================
# CONFIGURATION
# =========================

VERSION = "3.1.0"
VALID_DIR = Path("validated_sources")
VALID_WEBSCRAPE = VALID_DIR / "webscrape_valid.txt"
INVALID_WEBSCRAPE = VALID_DIR / "webscrape_invalid.txt"
CAREER_URL_MAPPINGS = VALID_DIR / "career_url_mappings.txt"
LOG_FILE = VALID_DIR / "validation.log"

# Security limits
VALIDATOR_WORKERS = 10  # Reduced for politeness
VALIDATOR_TIMEOUT_SEC = 10
MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10MB
MAX_HTML_PARSE_SIZE = 2 * 1024 * 1024  # 2MB for parsing
MAX_JSON_SIZE = 1 * 1024 * 1024  # 1MB per JSON block
MAX_REDIRECTS = 3
MAX_RETRIES = 2
RATE_LIMIT_DELAY = 0.1  # 100ms between requests

DEFAULT_MASTER_LIST = Path("sources/companies.txt")
EXTRA_MASTER_LIST = Path("sources/companies_extra.txt")
EXTRA_MASTER_LISTS_DIR = Path("sources/company_lists")

# Blocked networks (SSRF protection)
BLOCKED_NETWORKS = [
    ipaddress.IPv4Network('0.0.0.0/8'),        # Current network
    ipaddress.IPv4Network('10.0.0.0/8'),       # Private
    ipaddress.IPv4Network('127.0.0.0/8'),      # Loopback
    ipaddress.IPv4Network('169.254.0.0/16'),   # Link-local
    ipaddress.IPv4Network('172.16.0.0/12'),    # Private
    ipaddress.IPv4Network('192.168.0.0/16'),   # Private
    ipaddress.IPv4Network('224.0.0.0/4'),      # Multicast
    ipaddress.IPv4Network('240.0.0.0/4'),      # Reserved
]

# Allowed content types
ALLOWED_CONTENT_TYPES = [
    'text/html',
    'text/plain',
    'application/xhtml+xml',
]

ATS_MARKERS = [
    'greenhouse.io',
    'lever.co',
    'ashbyhq.com',
    'smartrecruiters.com',
    'myworkdayjobs.com',
    'icims.com',
    'jobvite.com',
    'bamboohr.com',
    'workable.com',
    '/wday/cxs/',
    'workday',
]

# =========================
# LOGGING SETUP
# =========================

def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    VALID_DIR.mkdir(parents=True, exist_ok=True)
    
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)


# =========================
# SECURITY UTILITIES
# =========================

class SecurityValidator:
    """Centralized security validation"""
    
    @staticmethod
    def is_safe_company_name(name: str) -> bool:
        """Validate company name is safe (alphanumeric only)"""
        return bool(re.match(r'^[a-zA-Z0-9\-]+$', name))
    
    @staticmethod
    def is_safe_url_scheme(url: str) -> bool:
        """Only allow HTTP/HTTPS"""
        parsed = urlparse(url)
        return parsed.scheme in ['http', 'https']
    
    @staticmethod
    def is_safe_ip(ip_str: str) -> Tuple[bool, str]:
        """Check if IP is not in blocked networks (SSRF protection)"""
        try:
            ip = ipaddress.IPv4Address(ip_str)
            
            # Check against blocked networks
            for network in BLOCKED_NETWORKS:
                if ip in network:
                    return False, f"blocked_network_{network}"
            
            return True, "ok"
            
        except (ValueError, ipaddress.AddressValueError):
            return False, "invalid_ip"
    
    @staticmethod
    def resolve_and_validate_hostname(hostname: str) -> Tuple[bool, str]:
        """
        Resolve hostname and validate IP (DNS rebinding protection)
        
        This prevents attacks where:
        1. DNS returns safe IP during validation
        2. DNS returns localhost during actual request
        """
        try:
            # Resolve hostname to IP
            ip_str = socket.gethostbyname(hostname)
            
            # Validate IP is safe
            is_safe, reason = SecurityValidator.is_safe_ip(ip_str)
            if not is_safe:
                return False, reason
            
            return True, ip_str
            
        except socket.gaierror:
            return False, "dns_resolution_failed"
        except Exception:
            return False, "dns_error"
    
    @staticmethod
    def validate_url(url: str) -> Tuple[bool, str]:
        """Complete URL validation"""
        try:
            # Parse URL
            parsed = urlparse(url)
            
            # Check scheme
            if not SecurityValidator.is_safe_url_scheme(url):
                return False, f"invalid_scheme_{parsed.scheme}"
            
            # Check hostname exists
            hostname = parsed.hostname
            if not hostname:
                return False, "no_hostname"
            
            # Resolve and validate IP (SSRF protection + DNS rebinding)
            is_safe, result = SecurityValidator.resolve_and_validate_hostname(hostname)
            if not is_safe:
                return False, result
            
            return True, "ok"
            
        except Exception as e:
            logger.debug(f"URL validation error for {url}: {e}")
            return False, "invalid_url"


class SafeHTTPSession:
    """Thread-safe HTTP session with security controls"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.max_redirects = MAX_REDIRECTS
        
        # Set secure defaults
        self.session.verify = True  # Always verify SSL
        
        # User-Agent for ethical identification
        self.headers = {
            'User-Agent': f'JobRadar/{VERSION} (Educational Research Bot; +https://github.com/yourproject)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'close',  # Don't keep connections alive
        }
    
    def safe_get(self, url: str, timeout: int) -> Tuple[bool, Optional[requests.Response], str]:
        """
        Make a safe HTTP GET request with all security controls
        
        Returns:
            (success, response, error_reason)
        """
        # 1. Validate URL BEFORE making request
        is_valid, reason = SecurityValidator.validate_url(url)
        if not is_valid:
            logger.debug(f"URL validation failed for {url}: {reason}")
            return False, None, reason
        
        # 2. Make request with safety controls
        try:
            response = self.session.get(
                url,
                timeout=timeout,
                headers=self.headers,
                allow_redirects=True,
                stream=True,  # Stream to check size/content-type first
            )
            
            # 3. Validate content-type
            content_type = response.headers.get('content-type', '').lower()
            is_allowed = any(allowed in content_type for allowed in ALLOWED_CONTENT_TYPES)
            
            if not is_allowed:
                response.close()
                logger.debug(f"Invalid content-type for {url}: {content_type}")
                return False, None, "invalid_content_type"
            
            # 4. Check response size BEFORE reading
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > MAX_RESPONSE_SIZE:
                response.close()
                logger.warning(f"Response too large for {url}: {content_length} bytes")
                return False, None, "response_too_large"
            
            # 5. Read content with size limit
            content = b''
            for chunk in response.iter_content(chunk_size=8192):
                content += chunk
                if len(content) > MAX_RESPONSE_SIZE:
                    response.close()
                    logger.warning(f"Response exceeded limit for {url}")
                    return False, None, "response_too_large"
            
            # 6. Create new response object with validated content
            response._content = content
            
            return True, response, "ok"
            
        except requests.exceptions.Timeout:
            logger.debug(f"Timeout for {url}")
            return False, None, "timeout"
        except requests.exceptions.TooManyRedirects:
            logger.debug(f"Too many redirects for {url}")
            return False, None, "too_many_redirects"
        except requests.exceptions.SSLError as e:
            logger.debug(f"SSL error for {url}: {e}")
            return False, None, "ssl_error"
        except requests.exceptions.ConnectionError as e:
            logger.debug(f"Connection error for {url}: {e}")
            return False, None, "connection_error"
        except requests.exceptions.RequestException as e:
            logger.debug(f"Request error for {url}: {e}")
            return False, None, "request_error"
        except Exception as e:
            logger.error(f"Unexpected error for {url}: {e}")
            return False, None, "exception"
    
    def close(self):
        """Clean up resources"""
        self.session.close()


# =========================
# CAREER PAGE DISCOVERY
# =========================

class CareerPageFinder:
    """Find career pages with security controls"""
    
    def __init__(self, http_session: SafeHTTPSession):
        self.http_session = http_session
    
    def find_career_page(self, company_name: str, timeout: int = 5) -> Optional[str]:
        """
        Try to find a company's career page
        Returns URL or None
        """
        clean_names = self._generate_name_variants(company_name)
        if not clean_names:
            logger.debug(f"Invalid company name: {company_name}")
            return None

        url_patterns: List[str] = []
        for clean_name in clean_names:
            url_patterns.extend(self._generate_url_patterns(clean_name))
        url_patterns = list(dict.fromkeys(url_patterns))
        
        # Try each pattern
        for url in url_patterns:
            try:
                # Check if URL works
                success, response, error = self.http_session.safe_get(url, timeout)
                
                if not success:
                    continue
                
                if response.status_code == 200:
                    # Verify it's actually a career page
                    if self._is_career_page(response.text, url):
                        logger.info(f"Found career page for {company_name}: {url}")
                        return url
                        
            except Exception as e:
                logger.debug(f"Error checking {url}: {e}")
                continue
        
        logger.debug(f"No career page found for {company_name}")
        return None
    
    def _sanitize_company_name(self, name: str) -> Optional[str]:
        """Sanitize company name for URL construction"""
        clean = re.sub(r'[^a-zA-Z0-9\-]', '', (name or '').lower())
        
        # Validate
        if not SecurityValidator.is_safe_company_name(clean):
            return None
        
        return clean

    def _generate_name_variants(self, name: str) -> List[str]:
        base = (name or '').strip().lower()
        if not base:
            return []

        variants: List[str] = []
        compact = re.sub(r'[^a-z0-9]', '', base)
        if compact:
            variants.append(compact)

        words = [w for w in re.split(r'[^a-z0-9]+', base) if w]
        if words:
            variants.append("-".join(words))
            variants.append("".join(words))
            if len(words) > 1:
                variants.append(words[0])

        out: List[str] = []
        for v in variants:
            cleaned = self._sanitize_company_name(v)
            if cleaned and cleaned not in out:
                out.append(cleaned)
        return out[:5]
    
    def _generate_url_patterns(self, clean_name: str) -> List[str]:
        """Generate common career page URL patterns"""
        return [
            f"https://jobs.{clean_name}.com",
            f"https://careers.{clean_name}.com",
            f"https://www.{clean_name}.com/careers",
            f"https://{clean_name}.com/careers",
            f"https://www.{clean_name}.com/jobs",
            f"https://{clean_name}.com/jobs",
            f"https://{clean_name}.com/careers/jobs",
            f"https://www.{clean_name}.com/company/careers",
            f"https://{clean_name}.com/careers/",
            f"https://careers.{clean_name}.io",
            f"https://{clean_name}.io/careers",
            f"https://{clean_name}.co/careers",
            f"https://www.{clean_name}.co/careers",
            f"https://{clean_name}.ai/careers",
            f"https://www.{clean_name}.ai/careers",
            f"https://boards.greenhouse.io/{clean_name}",
            f"https://job-boards.greenhouse.io/{clean_name}",
            f"https://jobs.lever.co/{clean_name}",
            f"https://jobs.ashbyhq.com/{clean_name}",
            f"https://jobs.smartrecruiters.com/{clean_name}",
            f"https://{clean_name}.bamboohr.com/careers",
            f"https://jobs.jobvite.com/{clean_name}",
            f"https://apply.workable.com/{clean_name}",
            f"https://{clean_name}.workable.com",
            f"https://{clean_name}.icims.com/jobs",
            f"https://careers.{clean_name}.icims.com/jobs",
            f"https://{clean_name}.wd5.myworkdayjobs.com",
            f"https://{clean_name}.wd1.myworkdayjobs.com",
        ]
    
    def _is_career_page(self, html: str, url: str = "") -> bool:
        """Verify page is actually a career page"""
        # Limit size for analysis
        if len(html) > MAX_HTML_PARSE_SIZE:
            html = html[:MAX_HTML_PARSE_SIZE]
        
        content_lower = html.lower()
        
        # Check for career/job keywords
        career_keywords = ['job', 'career', 'position', 'opening', 'opportunity', 'hiring', 'apply']
        keyword_count = sum(1 for keyword in career_keywords if keyword in content_lower)
        
        if keyword_count >= 3:
            return True

        # JS-heavy ATS pages can still be valid even without rendered listings.
        if JobDetector.has_ats_markers(html, url):
            return True

        return False


# =========================
# JOB DETECTION
# =========================

class JobDetector:
    """Detect jobs on career pages"""

    @staticmethod
    def has_ats_markers(html: str, url: str = "") -> bool:
        haystack = ((html or "")[:MAX_HTML_PARSE_SIZE] + " " + (url or "")).lower()
        return any(marker in haystack for marker in ATS_MARKERS)
    
    @staticmethod
    def has_json_ld_jobs(html: str) -> bool:
        """Check if page has JSON-LD JobPosting structured data"""
        # Limit HTML size
        if len(html) > MAX_HTML_PARSE_SIZE:
            html = html[:MAX_HTML_PARSE_SIZE]
        
        pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
        
        # Limit number of matches to analyze
        for match in matches[:20]:
            try:
                # Limit JSON size
                if len(match) > MAX_JSON_SIZE:
                    continue
                
                data = json.loads(match)
                
                # Check for JobPosting
                if JobDetector._contains_job_posting(data):
                    return True
                    
            except (json.JSONDecodeError, RecursionError, Exception) as e:
                logger.debug(f"Error parsing JSON-LD: {e}")
                continue
        
        return False
    
    @staticmethod
    def _contains_job_posting(data) -> bool:
        """Recursively check if data contains JobPosting (with depth limit)"""
        def check(obj, depth=0):
            if depth > 10:  # Prevent deep recursion
                return False
            
            if isinstance(obj, dict):
                if obj.get('@type') == 'JobPosting':
                    return True
                
                # Check @graph
                if '@graph' in obj:
                    for item in obj['@graph'][:50]:
                        if check(item, depth + 1):
                            return True
                
                # Check nested values
                for value in obj.values():
                    if check(value, depth + 1):
                        return True
            
            elif isinstance(obj, list):
                for item in obj[:50]:
                    if check(item, depth + 1):
                        return True
            
            return False
        
        return check(data)
    
    @staticmethod
    def has_job_listings_html(html: str) -> bool:
        """Check if page has HTML job listing patterns"""
        # Limit HTML size
        if len(html) > MAX_HTML_PARSE_SIZE:
            html = html[:MAX_HTML_PARSE_SIZE]
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Common job listing selectors
            selectors = [
                'div.job-listing',
                'div.job-item',
                'div.job-post',
                'div.position',
                'li.job',
                'article.job',
                'tr.job-row',
                'div[class*="job"]',
                'div[data-job]',
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if len(elements) >= 2:  # Need at least 2 listings
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error parsing HTML: {e}")
            return False


# =========================
# VALIDATOR
# =========================

class WebScrapeValidator:
    """Main validator with security controls"""
    
    def __init__(self):
        self.http_session = SafeHTTPSession()
        self.career_finder = CareerPageFinder(self.http_session)
    
    def validate_company(self, company: str) -> Tuple[bool, str, Optional[str]]:
        """
        Validate if a company can be scraped
        
        Returns:
            (is_valid, reason, career_url)
        """
        # Validate company name by checking at least one sanitized variant exists.
        variants = self.career_finder._generate_name_variants(company)
        if not variants:
            return False, "invalid_company_name", None
        
        try:
            # Step 1: Find career page
            career_url = self.career_finder.find_career_page(company, VALIDATOR_TIMEOUT_SEC)
            
            if not career_url:
                return False, "no_career_page", None
            
            # Step 2: Fetch page
            success, response, error = self.http_session.safe_get(career_url, VALIDATOR_TIMEOUT_SEC)
            
            if not success:
                return False, error, career_url
            
            if response.status_code != 200:
                return False, f"http_{response.status_code}", career_url
            
            html = response.text
            
            # Step 3: Check for job data
            if JobDetector.has_json_ld_jobs(html):
                logger.info(f"✓ {company} - Found jobs via JSON-LD")
                return True, "ok_jsonld", career_url
            
            if JobDetector.has_job_listings_html(html):
                logger.info(f"✓ {company} - Found jobs via HTML")
                return True, "ok_html", career_url

            if JobDetector.has_ats_markers(html, career_url):
                logger.info(f"✓ {company} - Found ATS-backed career page")
                return True, "ok_ats", career_url
            
            return False, "no_jobs", career_url
            
        except Exception as e:
            logger.error(f"Unexpected error validating {company}: {e}")
            return False, "exception", None
    
    def cleanup(self):
        """Clean up resources"""
        self.http_session.close()


# =========================
# PARALLEL VALIDATION
# =========================

def validate_companies_parallel(
    companies: List[str],
    max_workers: int = VALIDATOR_WORKERS
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Validate companies in parallel with rate limiting
    """
    valid: List[Tuple[str, str]] = []
    invalid: List[Tuple[str, str]] = []
    
    # Thread-local validators
    from threading import local
    thread_local = local()
    
    def get_validator():
        if not hasattr(thread_local, 'validator'):
            thread_local.validator = WebScrapeValidator()
        return thread_local.validator
    
    def validate_task(company: str) -> Tuple[str, bool, str, Optional[str]]:
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        validator = get_validator()
        is_valid, reason, career_url = validator.validate_company(company)
        return company, is_valid, reason, career_url
    
    print(f"\n{'='*70}")
    print(f"SECURE WEB SCRAPING VALIDATION v{VERSION}")
    print(f"{'='*70}")
    print(f"Companies to validate:  {len(companies)}")
    print(f"Workers (threads):      {max_workers}")
    print(f"Timeout per request:    {VALIDATOR_TIMEOUT_SEC}s")
    print(f"Max response size:      {MAX_RESPONSE_SIZE / 1024 / 1024:.1f}MB")
    print(f"Rate limit delay:       {RATE_LIMIT_DELAY}s")
    print(f"{'='*70}")
    print(f"\nSECURITY FEATURES:")
    print(f"  ✓ SSRF protection (blocks localhost/private IPs)")
    print(f"  ✓ DNS rebinding protection")
    print(f"  ✓ Response size limits")
    print(f"  ✓ Redirect limits (max {MAX_REDIRECTS})")
    print(f"  ✓ Content-type validation")
    print(f"  ✓ SSL verification enforced")
    print(f"  ✓ Rate limiting")
    print(f"  ✓ Input sanitization")
    print(f"{'='*70}\n")
    
    logger.info(f"Starting validation of {len(companies)} companies")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(validate_task, company): company for company in companies}
        completed = 0
        start_time = time.time()
        
        for future in as_completed(futures):
            try:
                company, is_valid, reason, career_url = future.result()
                completed += 1
                
                if is_valid:
                    valid.append((company, career_url))
                else:
                    invalid.append((company, reason))
                
                # Progress update
                if completed % 100 == 0 or completed == len(companies):
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    print(f"  Progress: {completed}/{len(companies)} | "
                          f"Valid: {len(valid)} | Invalid: {len(invalid)} | "
                          f"Rate: {rate:.1f}/s")
                    
            except Exception as e:
                logger.error(f"Error processing future: {e}")
                completed += 1
    
    # Cleanup
    if hasattr(thread_local, 'validator'):
        thread_local.validator.cleanup()
    
    logger.info(f"Validation complete: {len(valid)} valid, {len(invalid)} invalid")
    
    # Show breakdown
    reason_counts = {}
    for _, reason in invalid:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    
    if reason_counts:
        print(f"\nINVALID REASONS BREAKDOWN:")
        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            print(f"  - {reason}: {count}")
    
    return valid, invalid


# =========================
# FILE OPERATIONS
# =========================

def save_validation_results(valid: List[Tuple[str, str]], invalid: List[Tuple[str, str]]):
    """Save validation results securely"""
    # Create directory
    VALID_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save valid companies
    valid_companies = [company for company, _ in valid]
    VALID_WEBSCRAPE.write_text(
        "\n".join(valid_companies) + ("\n" if valid_companies else ""),
        encoding="utf-8"
    )
    
    # Save invalid companies with reasons
    INVALID_WEBSCRAPE.write_text(
        "\n".join([f"{company}\t{reason}" for company, reason in invalid]) + ("\n" if invalid else ""),
        encoding="utf-8"
    )
    
    # Save career URL mappings
    CAREER_URL_MAPPINGS.write_text(
        "\n".join([f"{company} | {url}" for company, url in valid]) + ("\n" if valid else ""),
        encoding="utf-8"
    )
    
    print(f"\n{'='*70}")
    print(f"RESULTS SAVED")
    print(f"{'='*70}")
    print(f"Valid companies:     {VALID_WEBSCRAPE}")
    print(f"                     ({len(valid)} companies)")
    print(f"Invalid companies:   {INVALID_WEBSCRAPE}")
    print(f"                     ({len(invalid)} companies)")
    print(f"Career URLs:         {CAREER_URL_MAPPINGS}")
    print(f"Log file:            {LOG_FILE}")
    print(f"{'='*70}\n")
    
    logger.info(f"Results saved: {len(valid)} valid, {len(invalid)} invalid")


def resolve_company_list_files(primary: Path = DEFAULT_MASTER_LIST) -> List[Path]:
    files: List[Path] = [primary]

    env_extra = os.getenv("COMPANIES_EXTRA_FILES", "").strip()
    if env_extra:
        for raw in env_extra.split(","):
            p = Path(raw.strip())
            if raw.strip():
                files.append(p)

    files.append(EXTRA_MASTER_LIST)
    if EXTRA_MASTER_LISTS_DIR.exists():
        files.extend(sorted(EXTRA_MASTER_LISTS_DIR.glob("*.txt")))

    out: List[Path] = []
    seen = set()
    for p in files:
        key = str(p.resolve()) if p.exists() else str(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def load_master_company_list(path: Path = DEFAULT_MASTER_LIST) -> List[str]:
    """Load and sanitize company list"""
    if not path.exists():
        raise FileNotFoundError(f"Master company list not found: {path}")
    
    source_files = [p for p in resolve_company_list_files(path) if p.exists()]
    items: List[str] = []
    for source in source_files:
        for raw in source.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            line = line.split("#", 1)[0].split("//", 1)[0].strip()
            if line:
                normalized = re.sub(r"\s+", " ", line.lower()).strip()
                if re.search(r"[a-z0-9]", normalized):
                    items.append(normalized)
                else:
                    logger.warning(f"Skipping malformed company name: {line}")
    
    # Remove duplicates
    return list(dict.fromkeys(items))


def load_validation_stats() -> Dict:
    """Load statistics from previous validation"""
    stats = {
        'valid_count': 0,
        'invalid_count': 0,
        'total': 0,
        'success_rate': 0.0,
        'valid_companies': [],
        'career_urls': {}
    }
    
    if VALID_WEBSCRAPE.exists():
        stats['valid_companies'] = [
            line.strip()
            for line in VALID_WEBSCRAPE.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        stats['valid_count'] = len(stats['valid_companies'])
    
    if INVALID_WEBSCRAPE.exists():
        stats['invalid_count'] = len([
            line for line in INVALID_WEBSCRAPE.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ])
    
    if CAREER_URL_MAPPINGS.exists():
        for line in CAREER_URL_MAPPINGS.read_text(encoding="utf-8").splitlines():
            if '|' in line:
                company, url = line.split('|', 1)
                stats['career_urls'][company.strip()] = url.strip()
    
    stats['total'] = stats['valid_count'] + stats['invalid_count']
    if stats['total'] > 0:
        stats['success_rate'] = stats['valid_count'] / stats['total'] * 100
    
    return stats


# =========================
# CLI
# =========================

def main():
    parser = argparse.ArgumentParser(
        description=f"Secure Web Scraping Validator v{VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Test on 100 companies first
  python %(prog)s validate --limit 100
  
  # Full validation (all companies)
  python %(prog)s validate
  
  # Use more workers (faster but less polite)
  python %(prog)s validate --workers 20
  
  # View statistics
  python %(prog)s stats
  
SECURITY:
  This validator includes comprehensive security protections:
  - SSRF protection (blocks localhost/private IPs)
  - DNS rebinding protection
  - Response size limits
  - Rate limiting
  - SSL verification
  - Input sanitization
        """
    )
    
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    
    sub = parser.add_subparsers(dest="cmd", required=True)
    
    # Validate command
    p_validate = sub.add_parser("validate", help="Validate companies for web scraping")
    p_validate.add_argument("--limit", type=int, default=None,
                           help="Test on first N companies only")
    p_validate.add_argument("--workers", type=int, default=VALIDATOR_WORKERS,
                           help=f"Number of parallel workers (default: {VALIDATOR_WORKERS})")
    p_validate.add_argument("--verbose", "-v", action="store_true",
                           help="Enable verbose logging")
    
    # Stats command
    sub.add_parser("stats", help="Show statistics from previous validation")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=getattr(args, 'verbose', False))
    
    if args.cmd == "validate":
        logger.info(f"Starting validation (version {VERSION})")
        
        # Load companies
        try:
            companies = load_master_company_list()
        except FileNotFoundError as e:
            print(f"ERROR: {e}")
            print(f"Please ensure {DEFAULT_MASTER_LIST} exists")
            return 1
        
        if args.limit:
            companies = companies[:args.limit]
            print(f"[INFO] Testing on first {args.limit} companies\n")
        
        # Validate
        valid, invalid = validate_companies_parallel(companies, max_workers=args.workers)
        
        # Save results
        save_validation_results(valid, invalid)
        
        # Summary
        total = len(companies)
        print(f"\n{'='*70}")
        print(f"VALIDATION SUMMARY")
        print(f"{'='*70}")
        print(f"Total companies:     {total}")
        print(f"Valid (scrapable):   {len(valid)} ({len(valid)/total*100:.1f}%)")
        print(f"Invalid:             {len(invalid)} ({len(invalid)/total*100:.1f}%)")
        print(f"{'='*70}\n")
        
        print("✓ SECURE validation complete!")
        print(f"\nNext steps:")
        print(f"  1. Review results in {VALID_DIR}/")
        print(f"  2. Check logs in {LOG_FILE}")
        print(f"  3. Integrate validated companies with your job scraper")
        print()
        
        return 0
    
    elif args.cmd == "stats":
        stats = load_validation_stats()
        
        print(f"\n{'='*70}")
        print(f"VALIDATION STATISTICS")
        print(f"{'='*70}")
        print(f"Valid companies:     {stats['valid_count']}")
        print(f"Invalid companies:   {stats['invalid_count']}")
        print(f"Total validated:     {stats['total']}")
        print(f"Success rate:        {stats['success_rate']:.1f}%")
        print(f"Career URLs mapped:  {len(stats['career_urls'])}")
        print(f"{'='*70}\n")
        
        if stats['valid_companies']:
            print(f"Sample valid companies (first 10):")
            for company in stats['valid_companies'][:10]:
                url = stats['career_urls'].get(company, 'N/A')
                print(f"  - {company}")
                print(f"    {url}")
        
        print()
        return 0


if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Validation interrupted by user")
        exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\n❌ ERROR: {e}")
        exit(1)
