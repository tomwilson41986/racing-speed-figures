"""Playwright login for Timeform.

Mirrors ``src/france_galop/auth.py`` but targets Timeform's own sign-in form
(not Microsoft CIAM).  The browser/network layer only runs in CI — ``timeform.com``
is blocked by the dev sandbox's egress policy — so this module is not covered by
the offline unit tests; keep all parsing logic in ``results.py`` instead.

Dual-environment launch: ``executable_path`` and ``proxy`` default to None (plain
headless launch, as in GitHub Actions) but can be supplied via ``PW_EXECUTABLE_PATH``
/ ``PW_PROXY`` so the same code runs inside the sandbox if egress is ever opened.
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Optional

from tenacity import retry, stop_after_attempt, wait_exponential

# Reuse the France Galop primitives rather than duplicating them.
from src.france_galop.auth import (
    CHROME_USER_AGENT,
    PlaywrightResponse,  # noqa: F401  (re-exported for callers)
    PlaywrightSession,
    _find_chromium_executable,
)

log = logging.getLogger(__name__)

SITE_BASE = "https://www.timeform.com"
LOGIN_URL = f"{SITE_BASE}/account/sign-in"
HOME_URL = f"{SITE_BASE}/horse-racing"
RESULTS_YESTERDAY = f"{SITE_BASE}/horse-racing/results/yesterday"

# Selector fallbacks — the exact Timeform form is confirmed from the first CI capture.
EMAIL_SELECTORS = (
    'input[type="email"], input[name="Email"], input[name="email"], '
    'input[name="username"], input[name="Username"], #Email, #EmailAddress, #username'
)
PASSWORD_SELECTORS = (
    'input[type="password"], input[name="Password"], input[name="password"], #Password'
)
SUBMIT_SELECTORS = (
    'button[type="submit"], input[type="submit"], button:has-text("Sign in"), '
    'button:has-text("Log in"), button:has-text("Login")'
)
COOKIE_ACCEPT_SELECTORS = (
    '#onetrust-accept-btn-handler, button:has-text("Accept All"), '
    'button:has-text("Accept all"), button:has-text("Accept"), '
    'button:has-text("I Accept")'
)


def _human_pause(a: float = 1.5, b: float = 3.5) -> None:
    """Deliberate, randomised pause so navigation looks human, not bot-like."""
    time.sleep(random.uniform(a, b))


class TimeformAuth:
    """Log into Timeform via Playwright and return an authenticated session."""

    def __init__(
        self,
        email: Optional[str] = None,
        password: Optional[str] = None,
        headless: bool = True,
        executable_path: Optional[str] = None,
        proxy: Optional[str] = None,
    ):
        self._email = email or os.environ.get("TIMEFORM_USER") \
            or os.environ.get("TF_EMAIL", "")
        self._password = password or os.environ.get("TIMEFORM_PASS") \
            or os.environ.get("TF_PASSWORD", "")
        self._headless = headless
        self._executable_path = executable_path or os.environ.get("PW_EXECUTABLE_PATH")
        self._proxy = proxy or os.environ.get("PW_PROXY")
        if not self._email or not self._password:
            raise ValueError(
                "Timeform credentials required. Pass email/password or set "
                "TIMEFORM_USER and TIMEFORM_PASS env vars."
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def login(self) -> PlaywrightSession:
        """Perform the login and return a cookie-carrying PlaywrightSession."""
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        log.info("Starting Playwright login to Timeform...")
        pw = sync_playwright().start()
        launch_kwargs = {"headless": self._headless}
        executable = self._executable_path or _find_chromium_executable(
            pw.chromium.executable_path)
        if executable and executable != pw.chromium.executable_path:
            launch_kwargs["executable_path"] = executable
        if self._proxy:
            launch_kwargs["proxy"] = {"server": self._proxy}

        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(user_agent=CHROME_USER_AGENT, locale="en-GB",
                                      viewport={"width": 1400, "height": 1000})
        page = context.new_page()
        try:
            # 1. Land on the homepage first, like a person would.
            log.info("Navigating to homepage...")
            page.goto(HOME_URL, wait_until="domcontentloaded", timeout=45000)
            self._accept_cookies(page)
            _human_pause()

            # 2. Go to the sign-in page.
            log.info("Navigating to sign-in page...")
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=45000)
            self._accept_cookies(page)
            _human_pause()

            # Already signed in? (site may redirect away from the form.)
            if "sign-in" not in page.url and "login" not in page.url.lower():
                log.info("Appears already authenticated (no login form). URL=%s", page.url)
                return PlaywrightSession(pw, browser, context, page)

            # 3. Fill email.
            try:
                email_input = page.wait_for_selector(EMAIL_SELECTORS, state="visible",
                                                     timeout=20000)
            except PWTimeout:
                self._dump_debug(page, "email-field-timeout")
                raise RuntimeError(f"Email field not found. URL: {page.url}")
            email_input.click()
            email_input.fill(self._email)
            _human_pause(0.6, 1.4)

            # Some flows reveal the password only after an email "continue" step.
            pwd_input = page.query_selector(PASSWORD_SELECTORS)
            if pwd_input is None or not pwd_input.is_visible():
                cont = page.query_selector(SUBMIT_SELECTORS)
                if cont and cont.is_visible():
                    cont.click()
                else:
                    page.keyboard.press("Enter")
                _human_pause(0.8, 1.6)

            # 4. Fill password.
            try:
                pwd_input = page.wait_for_selector(PASSWORD_SELECTORS, state="visible",
                                                   timeout=15000)
            except PWTimeout:
                self._dump_debug(page, "password-field-timeout")
                raise RuntimeError(f"Password field not found. URL: {page.url}")
            pwd_input.click()
            pwd_input.fill(self._password)
            _human_pause(0.6, 1.4)

            # 5. Submit.
            submit = page.query_selector(SUBMIT_SELECTORS)
            if submit and submit.is_visible():
                submit.click()
            else:
                page.keyboard.press("Enter")

            # 6. Wait to land back on the site (not the form).
            try:
                page.wait_for_load_state("networkidle", timeout=25000)
            except PWTimeout:
                pass
            _human_pause()

            if "sign-in" in page.url or "login" in page.url.lower():
                err = page.query_selector(".validation-summary-errors, .field-validation-error, "
                                          "[role='alert'], .error")
                msg = err.inner_text() if err else "still on the login page"
                self._dump_debug(page, "login-not-completed")
                raise RuntimeError(f"Login did not complete: {msg}. URL: {page.url}")

            log.info("Login complete. URL=%s", page.url[:120])
            return PlaywrightSession(pw, browser, context, page)
        except Exception:
            browser.close()
            pw.stop()
            raise

    @staticmethod
    def _accept_cookies(page) -> None:
        try:
            btn = page.query_selector(COOKIE_ACCEPT_SELECTORS)
            if btn and btn.is_visible():
                btn.click()
                log.debug("Accepted cookie consent.")
        except Exception:
            pass

    @staticmethod
    def _dump_debug(page, label: str) -> None:
        """Log the page's inputs + screenshot to help fix selectors from CI logs."""
        log.error("=== TIMEFORM LOGIN DEBUG: %s ===", label)
        log.error("URL: %s", page.url)
        try:
            for inp in page.query_selector_all("input"):
                attrs = inp.evaluate(
                    "el => ({id: el.id, name: el.name, type: el.type, ph: el.placeholder})")
                log.error("  <input id=%s name=%s type=%s ph=%s>",
                          attrs.get("id"), attrs.get("name"), attrs.get("type"), attrs.get("ph"))
        except Exception:
            pass
        try:
            from .store import RAW_DIR
            RAW_DIR.mkdir(parents=True, exist_ok=True)
            path = RAW_DIR / f"login_debug_{label}.png"
            page.screenshot(path=str(path))
            log.error("Screenshot: %s", path)
        except Exception:
            pass
