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
# The sign-in form lives under /horse-racing (the old /account/sign-in now 404s
# once you clear the WAF — confirmed from the homepage's own sign-in link).
LOGIN_URL = f"{SITE_BASE}/horse-racing/account/sign-in?returnUrl=%2Fhorse-racing"
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


def parse_cookie_header(raw: str, domain_url: str = SITE_BASE) -> list:
    """Turn a browser ``Cookie:`` header string into Playwright cookie dicts.

    Accepts the whole header (``a=b; c=d; ...``) — copy it from DevTools rather
    than picking out the one auth cookie. An optional ``Cookie:`` prefix is
    tolerated. Each cookie is scoped to ``domain_url`` so it applies to the
    Timeform pages we fetch.
    """
    raw = (raw or "").strip()
    if raw.lower().startswith("cookie:"):
        raw = raw.split(":", 1)[1].strip()
    cookies = []
    for part in raw.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        name, value = name.strip(), value.strip()
        if name:
            cookies.append({"name": name, "value": value, "url": domain_url})
    return cookies


class TimeformAuth:
    """Log into Timeform via Playwright and return an authenticated session."""

    def __init__(
        self,
        email: Optional[str] = None,
        password: Optional[str] = None,
        headless: bool = True,
        executable_path: Optional[str] = None,
        proxy: Optional[str] = None,
        cookie: Optional[str] = None,
    ):
        self._email = email or os.environ.get("TIMEFORM_USER") \
            or os.environ.get("TF_EMAIL", "")
        self._password = password or os.environ.get("TIMEFORM_PASS") \
            or os.environ.get("TF_PASSWORD", "")
        self._headless = headless
        self._executable_path = executable_path or os.environ.get("PW_EXECUTABLE_PATH")
        self._proxy = proxy or os.environ.get("PW_PROXY")
        # A pre-authenticated session cookie skips the (CAPTCHA-gated) sign-in
        # form entirely — the supported way to run the reconciliation.
        self._cookie = cookie or os.environ.get("TIMEFORM_COOKIE", "")
        if not self._cookie and not (self._email and self._password):
            raise ValueError(
                "Timeform auth required. Set TIMEFORM_COOKIE (a logged-in session "
                "cookie) — recommended, as the sign-in form has a CAPTCHA — or "
                "TIMEFORM_USER / TIMEFORM_PASS."
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

        # Normalise the automation fingerprint so Timeform's Azure Front Door WAF
        # treats the session as an ordinary browser. Paired with a *headed* run
        # under xvfb (see the workflow), this is what clears the challenge from a
        # datacenter IP; a proxy (PW_PROXY) is the fallback for a hard IP block.
        # (Account owner explicitly authorised this to reach their own paid data.)
        launch_kwargs.setdefault("args", [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ])

        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(user_agent=CHROME_USER_AGENT, locale="en-GB",
                                      viewport={"width": 1400, "height": 1000},
                                      timezone_id="Europe/London")
        # Hide the webdriver flag the WAF sniffs for.
        try:
            context.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        except Exception:  # pragma: no cover - best effort
            pass

        # ── Preferred path: reuse a logged-in session cookie ──────────
        # The sign-in form has an image CAPTCHA, so an automated form login
        # cannot complete. A cookie captured from a real browser session skips
        # the form entirely.
        if self._cookie:
            try:
                cookies = parse_cookie_header(self._cookie)
                if cookies:
                    context.add_cookies(cookies)
                page = context.new_page()
                log.info("Trying TIMEFORM_COOKIE session (%d cookies)...", len(cookies))
                page.goto(HOME_URL, wait_until="domcontentloaded", timeout=45000)
                self._settle_waf(page)
                if self._is_authenticated(page):
                    log.info("Authenticated via TIMEFORM_COOKIE — skipped the sign-in form.")
                    return PlaywrightSession(pw, browser, context, page)
                log.warning("TIMEFORM_COOKIE did not authenticate (expired/invalid?). "
                            "Refresh it from a fresh browser login.")
                page.close()
            except Exception as e:
                log.warning("Cookie auth failed: %s", e)
            if not (self._email and self._password):
                browser.close()
                pw.stop()
                raise RuntimeError(
                    "TIMEFORM_COOKIE did not authenticate and no user/pass fallback "
                    "is set. Refresh the cookie from a fresh Timeform login.")

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
            self._settle_waf(page)
            self._accept_cookies(page)
            _human_pause()

            # Already signed in? (site may redirect away from the form.)
            if "sign-in" not in page.url and "login" not in page.url.lower():
                log.info("Appears already authenticated (no login form). URL=%s", page.url)
                return PlaywrightSession(pw, browser, context, page)

            # 3. Fill email.
            try:
                email_input = page.wait_for_selector(EMAIL_SELECTORS, state="visible",
                                                     timeout=30000)
            except PWTimeout:
                self._dump_debug(page, "email-field-timeout")
                blocked = "azwaf" in (page.url or "").lower()
                hint = (" The page is an Azure Front Door WAF challenge (afd_azwaf_tok "
                        "in the URL) — the runner IP is being blocked. Set PW_PROXY to a "
                        "residential proxy to log in from CI." if blocked else "")
                raise RuntimeError(f"Email field not found. URL: {page.url}.{hint}")
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
    def _is_authenticated(page) -> bool:
        """Best-effort check that a cookie session is logged in (not on sign-in)."""
        url = (page.url or "").lower()
        if "sign-in" in url or "/account/login" in url:
            return False
        try:
            html = (page.content() or "").lower()
        except Exception:
            return True  # page loaded and URL isn't the sign-in form
        if any(m in html for m in ("sign out", "signout", "log out", "logout", "my timeform")):
            return True
        if "register-free-account" in html or ">sign in<" in html:
            return False
        # Reachable, non-redirected page: the per-meeting _session_expired check
        # in the capture step will still catch a stale cookie.
        return True

    @staticmethod
    def _settle_waf(page) -> None:
        """Give an Azure Front Door WAF JS-challenge a chance to resolve.

        The challenge sets ``afd_azwaf_tok`` and redirects back to the form; it
        needs the page's JS to run and network to go idle. Best-effort: wait for
        networkidle, and if we're still on a challenge URL, reload once.
        """
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        try:
            if "azwaf" in (page.url or "").lower():
                _human_pause(2.0, 4.0)
                page.reload(wait_until="domcontentloaded", timeout=45000)
                try:
                    page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
        except Exception:
            pass

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
