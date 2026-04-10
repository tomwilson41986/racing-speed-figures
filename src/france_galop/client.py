"""HTTP client for France Galop website: race discovery and PDF download.

After authenticating via auth.py (Playwright), this module uses a
requests.Session with auth cookies for all page scraping.  BeautifulSoup
parses the HTML to find race meetings, individual races, and PDF links.

Base URL: https://www.france-galop.com
Navigation:
    /fr/courses/hier          → yesterday's races
    /fr/courses/aujourdhui    → today's races
    /fr/courses/demain        → tomorrow's races
    /fr/courses/toutes-les-courses → all dates (needs auth)
"""

import datetime
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .auth import FranceGalopAuth, PlaywrightSession, check_authenticated

log = logging.getLogger(__name__)

SITE_BASE = "https://www.france-galop.com"

CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Keywords that identify sectional times PDFs (French and English)
SECTIONAL_KEYWORDS = [
    "sectionnel", "intermediaire", "intermédiaire",
    "sectional", "split", "fraction",
    "temps inter", "temps section",
]


def _jittered_sleep(base: float = 2.0):
    """Sleep with random jitter (0.5x to 1.5x base)."""
    time.sleep(base * random.uniform(0.5, 1.5))


class FranceGalopClient:
    """HTTP client for navigating France Galop, discovering races, and downloading PDFs.

    Parameters
    ----------
    email : str, optional
        France Galop email (or set FG_EMAIL env var).
    password : str, optional
        France Galop password (or set FG_PASSWORD env var).
    delay_between_requests : float
        Minimum seconds between requests (default 2.0).
    headless : bool
        Run Playwright browser in headless mode for login (default True).
    """

    def __init__(
        self,
        email: Optional[str] = None,
        password: Optional[str] = None,
        delay_between_requests: float = 2.0,
        headless: bool = True,
    ):
        self._email = email or os.environ.get("FG_EMAIL", "")
        self._password = password or os.environ.get("FG_PASSWORD", "")
        self.delay = delay_between_requests
        self._headless = headless
        self._last_request_time: float = 0.0
        self.session: Optional[PlaywrightSession] = None
        self._authenticated = False

    # ---- Authentication ----------------------------------------------------

    def login(self) -> bool:
        """Authenticate with France Galop via Playwright.

        Returns True on success.
        """
        auth = FranceGalopAuth(
            email=self._email,
            password=self._password,
            headless=self._headless,
        )
        self.session = auth.login()
        self._authenticated = check_authenticated(self.session)

        if self._authenticated:
            log.info("France Galop login successful.")
        else:
            log.error("France Galop login failed — session not authenticated.")

        return self._authenticated

    def ensure_authenticated(self):
        """Login if not already authenticated."""
        if not self._authenticated or self.session is None:
            if not self.login():
                raise RuntimeError("Failed to authenticate with France Galop.")

    # ---- Rate limiting / HTTP helpers --------------------------------------

    def _rate_limit(self):
        """Sleep if necessary to respect the minimum delay between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get(self, url: str):
        """Perform a GET request with rate limiting and retry."""
        self._rate_limit()
        log.debug("GET %s", url)
        self._last_request_time = time.monotonic()
        resp = self.session.get(url, timeout=30)
        return resp

    def _fetch_html(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a page and return parsed HTML, or None on failure."""
        try:
            resp = self._get(url)
        except Exception:
            log.error("Request failed after retries: %s", url)
            return None

        if resp.status_code != 200:
            log.error("HTTP %s for %s", resp.status_code, url)
            return None

        # Check if we got redirected to the login page
        if "ciamlogin" in resp.url:
            log.error("Session expired — redirected to login: %s", resp.url)
            self._authenticated = False
            return None

        return BeautifulSoup(resp.text, "lxml")

    # ---- Race meeting discovery -------------------------------------------

    def get_meetings_for_date(self, date: datetime.date) -> list[dict]:
        """Discover race meetings on a given date from France Galop.

        Navigates to the results page for the given date and extracts
        links to individual meetings.

        Returns list of dicts:
            [{'meeting_url': str, 'venue': str, 'date': date, 'meeting_fg_id': str}, ...]
        """
        self.ensure_authenticated()

        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        tomorrow = today + datetime.timedelta(days=1)

        if date == yesterday:
            url = f"{SITE_BASE}/fr/courses/hier"
        elif date == today:
            url = f"{SITE_BASE}/fr/courses/aujourdhui"
        elif date == tomorrow:
            url = f"{SITE_BASE}/fr/courses/demain"
        else:
            # For other dates, use the "toutes-les-courses" page with date filter
            # France Galop uses /fr/courses/autres-dates/YYYY-MM-DD
            url = f"{SITE_BASE}/fr/courses/autres-dates/{date.isoformat()}"

        log.info("Fetching meetings for %s from %s", date.isoformat(), url)
        soup = self._fetch_html(url)
        if soup is None:
            return []

        meetings = []
        # France Galop meeting URLs:
        #   /fr/courses/reunion/{DATE}/{ENCODED-ID}
        #   /en/racing/meeting/{DATE}/{ENCODED-ID}
        meeting_pattern = re.compile(
            r"/(?:courses/reunion|racing/meeting)/([^/]+)/([^/]+)"
        )

        for link in soup.find_all("a", href=True):
            href = link["href"]
            match = meeting_pattern.search(href)
            if not match:
                continue

            full_url = urljoin(SITE_BASE, href)

            # Skip duplicates
            if any(m["meeting_url"] == full_url for m in meetings):
                continue

            venue = self._extract_venue_from_link(link)
            fg_id = match.group(2)

            meetings.append({
                "meeting_url": full_url,
                "venue": venue,
                "date": date,
                "meeting_fg_id": fg_id,
            })

        log.info("Found %d meetings for %s", len(meetings), date.isoformat())
        return meetings

    def _extract_venue_from_link(self, link_tag) -> str:
        """Extract venue name from a meeting link element."""
        # Try the text content
        text = link_tag.get_text(strip=True)
        if text:
            # Clean up — remove race count suffixes like "(8 courses)"
            cleaned = re.sub(r'\s*\(\d+\s*course.*?\)', '', text).strip()
            if cleaned:
                return cleaned.upper()

        # Try title attribute
        title = link_tag.get("title", "")
        if title:
            return title.upper()

        return "UNKNOWN"

    # ---- Race link discovery -----------------------------------------------

    def get_race_links(self, meeting_url: str) -> list[dict]:
        """Parse a meeting page to find individual race result links.

        Returns list of dicts:
            [{'race_url': str, 'race_number': int, 'race_name': str}, ...]
        """
        self.ensure_authenticated()

        log.debug("Fetching race links from %s", meeting_url)
        _jittered_sleep(self.delay)
        soup = self._fetch_html(meeting_url)
        if soup is None:
            return []

        races = []
        # Race URLs: /fr/courses/course/{...} or /en/racing/race/{...}
        race_pattern = re.compile(r"/(?:courses/course|racing/race)/")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not race_pattern.search(href):
                continue

            full_url = urljoin(SITE_BASE, href)

            # Skip duplicates
            if any(r["race_url"] == full_url for r in races):
                continue

            race_name = link.get_text(strip=True)
            race_number = self._extract_race_number(link, href)

            races.append({
                "race_url": full_url,
                "race_number": race_number,
                "race_name": race_name,
            })

        # Sort by race number
        races.sort(key=lambda r: r["race_number"])
        log.info("Found %d races at %s", len(races), meeting_url)
        return races

    def _extract_race_number(self, link_tag, href: str) -> int:
        """Try to extract race number from link text or URL."""
        text = link_tag.get_text(strip=True)

        # Patterns: "Race 1", "Course 1", "1ère Course", "R1", "C1", "1."
        match = re.search(
            r'(?:Race|Course|Prix|R|C)\s*(\d+)', text, re.IGNORECASE
        )
        if match:
            return int(match.group(1))

        # Try leading number: "1 - Prix de..."
        match = re.search(r'^(\d+)\s*[-–.]', text)
        if match:
            return int(match.group(1))

        # Try from URL path
        match = re.search(r'/(\d+)/?$', href)
        if match:
            return int(match.group(1))

        # Data attributes
        data_num = link_tag.get("data-race-number") or link_tag.get("data-num")
        if data_num and data_num.isdigit():
            return int(data_num)

        return 0

    # ---- PDF link discovery -----------------------------------------------

    def get_pdf_links(self, race_url: str) -> list[dict]:
        """Parse a race result page to find PDF download links.

        Looks for links to PDFs, particularly those containing
        sectional/intermediate times.

        Returns list of dicts:
            [{'pdf_url': str, 'pdf_type': str, 'filename': str, 'link_text': str}, ...]
        """
        self.ensure_authenticated()

        log.debug("Fetching PDF links from %s", race_url)
        _jittered_sleep(self.delay)
        soup = self._fetch_html(race_url)
        if soup is None:
            return []

        pdfs = []

        # Method 1: Find <a> tags linking to PDFs
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not self._is_pdf_link(href):
                continue

            full_url = urljoin(SITE_BASE, href)
            if any(p["pdf_url"] == full_url for p in pdfs):
                continue

            link_text = link.get_text(strip=True).lower()
            title = (link.get("title") or "").lower()
            combined = f"{link_text} {title} {href.lower()}"
            pdf_type = self._classify_pdf(combined)

            pdfs.append({
                "pdf_url": full_url,
                "pdf_type": pdf_type,
                "filename": self._extract_filename(href),
                "link_text": link_text,
            })

        # Method 2: Check for download buttons/icons with data-href
        for el in soup.find_all(["button", "a", "span"], attrs=True):
            data_href = el.get("data-href") or el.get("data-url") or ""
            if self._is_pdf_link(data_href):
                full_url = urljoin(SITE_BASE, data_href)
                if any(p["pdf_url"] == full_url for p in pdfs):
                    continue
                el_text = el.get_text(strip=True).lower()
                pdfs.append({
                    "pdf_url": full_url,
                    "pdf_type": self._classify_pdf(f"{el_text} {data_href}"),
                    "filename": self._extract_filename(data_href),
                    "link_text": el_text,
                })

        # Method 3: Check for iframes embedding PDFs
        for iframe in soup.find_all("iframe", src=True):
            src = iframe["src"]
            if self._is_pdf_link(src):
                full_url = urljoin(SITE_BASE, src)
                if any(p["pdf_url"] == full_url for p in pdfs):
                    continue
                pdfs.append({
                    "pdf_url": full_url,
                    "pdf_type": self._classify_pdf(src),
                    "filename": self._extract_filename(src),
                    "link_text": "embedded_pdf",
                })

        log.info("Found %d PDF links at %s", len(pdfs), race_url)
        return pdfs

    def _is_pdf_link(self, href: str) -> bool:
        """Check if a URL/href points to a PDF."""
        if not href:
            return False
        lower = href.lower()
        return (
            lower.endswith(".pdf")
            or "format=pdf" in lower
            or "/pdf/" in lower
            or "type=pdf" in lower
        )

    def _classify_pdf(self, text: str) -> str:
        """Classify a PDF by its context text into a type."""
        text_lower = text.lower()
        for kw in SECTIONAL_KEYWORDS:
            if kw in text_lower:
                return "sectional_times"

        result_kws = [
            "result", "résultat", "resultat",
            "arrivée", "arrivee", "arrival",
        ]
        for kw in result_kws:
            if kw in text_lower:
                return "result"

        return "other"

    def _extract_filename(self, url: str) -> str:
        """Extract filename from a URL."""
        parsed = urlparse(url)
        path = parsed.path
        return path.split("/")[-1] if "/" in path else path

    # ---- PDF download -----------------------------------------------------

    def download_pdf(self, pdf_url: str, dest_path: str) -> bool:
        """Download a PDF file to dest_path.  Returns True on success."""
        self.ensure_authenticated()

        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        log.info("Downloading PDF: %s → %s", pdf_url, dest_path)
        _jittered_sleep(self.delay)

        try:
            resp = self._get(pdf_url)
        except Exception:
            log.error("PDF download failed after retries: %s", pdf_url)
            return False

        if resp.status_code != 200:
            log.error("HTTP %s downloading PDF: %s", resp.status_code, pdf_url)
            return False

        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not pdf_url.lower().endswith(".pdf"):
            log.warning(
                "Unexpected content type: %s (URL: %s)", content_type, pdf_url,
            )

        dest.write_bytes(resp.content)
        size_kb = len(resp.content) / 1024
        log.info("Downloaded %.1f KB → %s", size_kb, dest_path)
        return True

    def get_pdf_content(self, pdf_url: str) -> Optional[bytes]:
        """Download a PDF and return its content as bytes, or None on failure."""
        self.ensure_authenticated()

        _jittered_sleep(self.delay)
        try:
            resp = self._get(pdf_url)
        except Exception:
            log.error("PDF download failed: %s", pdf_url)
            return None

        if resp.status_code != 200:
            log.error("HTTP %s for PDF: %s", resp.status_code, pdf_url)
            return None

        return resp.content
