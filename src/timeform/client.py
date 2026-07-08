"""Timeform fetch client: login, human-paced navigation, XHR/JSON sniffing, and
raw-material dumping.

Network-only — ``timeform.com`` is blocked in the dev sandbox, so this runs only
in CI.  Everything it produces (index HTML, per-meeting HTML, sniffed JSON) is
written to ``output/timeform_recon/raw/<date>/`` regardless of parse success, so
the first CI run yields a usable artifact for finalising the parsers.
"""

from __future__ import annotations

import logging
import random
import time
from pathlib import Path
from typing import Optional

from .auth import RESULTS_YESTERDAY, TimeformAuth
from .models import RawCapture
from . import results as results_mod
from .store import RAW_DIR

log = logging.getLogger(__name__)

# Content-type / URL hints for responses worth capturing as a results feed.
_JSON_URL_HINTS = ("result", "racecard", "api", "timefigure", "tfig", ".json")


class TimeformClient:
    """Drives an authenticated Playwright session to capture yesterday's results."""

    def __init__(
        self,
        email: Optional[str] = None,
        password: Optional[str] = None,
        headless: bool = True,
        delay_between_requests: float = 4.0,
        max_meetings: int = 60,
    ):
        self._auth = TimeformAuth(email=email, password=password, headless=headless)
        self._session = None
        self._delay = delay_between_requests
        self._last_request = 0.0
        self._max_meetings = max_meetings
        self._json_payloads: list[str] = []

    # ── session lifecycle ──────────────────────────────────────────
    def login(self) -> bool:
        try:
            self._session = self._auth.login()
            self._session._page.on("response", self._sniff)  # noqa: SLF001
            return True
        except Exception as e:
            log.error("Timeform login failed: %s", e)
            return False

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    # ── politeness ─────────────────────────────────────────────────
    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request
        wait = self._delay - elapsed
        if wait > 0:
            time.sleep(wait)
        time.sleep(random.uniform(0.4, 1.6))  # human jitter on top of the floor
        self._last_request = time.time()

    def _sniff(self, response) -> None:
        """Capture JSON bodies from results-ish responses (robust data path)."""
        try:
            url = response.url.lower()
            ctype = (response.headers or {}).get("content-type", "").lower()
            if "json" in ctype and any(h in url for h in _JSON_URL_HINTS):
                body = response.text()
                if body and len(body) < 5_000_000:
                    self._json_payloads.append(body)
        except Exception:
            pass

    # ── capture ────────────────────────────────────────────────────
    def capture_yesterday(self, date: str) -> RawCapture:
        """Load the yesterday index + each meeting's result page; dump everything."""
        if self._session is None:
            raise RuntimeError("Not logged in — call login() first.")
        page = self._session._page  # noqa: SLF001
        out_dir = RAW_DIR / date
        out_dir.mkdir(parents=True, exist_ok=True)
        capture = RawCapture(date=date)

        # Index page.
        self._rate_limit()
        page.goto(RESULTS_YESTERDAY, wait_until="domcontentloaded", timeout=45000)
        self._wait_idle(page)
        capture.index_html = page.content()
        (out_dir / "index.html").write_text(capture.index_html, encoding="utf-8")
        self._screenshot(page, out_dir / "index.png")

        meetings = results_mod.parse_yesterday_index(capture.index_html, date=date)
        log.info("Found %d meetings for %s", len(meetings), date)

        for meeting in meetings[: self._max_meetings]:
            try:
                self._rate_limit()
                if self._session_expired(page):
                    log.warning("Session appears expired — re-logging in.")
                    self.close()
                    if not self.login():
                        break
                    page = self._session._page  # noqa: SLF001
                page.goto(meeting.result_url, wait_until="domcontentloaded", timeout=45000)
                self._wait_idle(page)
                html = page.content()
                safe = _slug(meeting.course)
                (out_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                capture.page_htmls[meeting.course] = html
            except Exception as e:
                log.warning("Failed to load meeting %s: %s", meeting.course, e)

        capture.json_payloads = list(self._json_payloads)
        # One file per payload — payloads may themselves contain newlines, so a
        # single joined file could not be split back apart reliably.
        for i, payload in enumerate(capture.json_payloads[:50]):
            (out_dir / f"payload_{i}.json").write_text(payload, encoding="utf-8")
        log.info("Captured %d meeting pages + %d JSON payloads",
                 len(capture.page_htmls), len(capture.json_payloads))
        return capture

    # ── helpers ────────────────────────────────────────────────────
    @staticmethod
    def _wait_idle(page) -> None:
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

    @staticmethod
    def _session_expired(page) -> bool:
        url = (page.url or "").lower()
        return "sign-in" in url or "/account/login" in url

    @staticmethod
    def _screenshot(page, path: Path) -> None:
        try:
            page.screenshot(path=str(path), full_page=True)
        except Exception:
            pass


def _slug(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name.strip().lower()).strip("_") or "meeting"
