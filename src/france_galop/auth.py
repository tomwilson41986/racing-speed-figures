"""Playwright-based OAuth authentication for France Galop.

France Galop uses Microsoft CIAM (Azure AD) for authentication.  The login
page is a JavaScript SPA that cannot be replicated with plain HTTP requests.

Playwright drives a headless Chromium browser to complete the login.  After
authentication, the Playwright browser context is kept alive and its
built-in APIRequestContext is used for all subsequent HTTP requests — this
shares the browser's cookie jar automatically, avoiding fragile cookie
transfer to a requests.Session.

Usage:
    from src.france_galop.auth import FranceGalopAuth

    auth = FranceGalopAuth(email="...", password="...")
    ctx = auth.login()  # returns a PlaywrightSession wrapping the context
"""

import glob
import logging
import os
import shutil
from typing import Optional

from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

SITE_BASE = "https://www.france-galop.com"
LOGIN_TRIGGER_URL = f"{SITE_BASE}/en/racing/yesterday"

CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _find_chromium_executable(default_path: str) -> str:
    """Return a working chromium executable path."""
    if os.path.exists(default_path):
        return default_path

    for pattern in [
        "/opt/pw-browsers/chromium-*/chrome-linux/chrome",
        "/opt/pw-browsers/chromium-*/chrome-linux64/chrome",
    ]:
        matches = sorted(glob.glob(pattern), reverse=True)
        if matches:
            log.info("Found chromium fallback: %s", matches[0])
            return matches[0]

    system_chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if system_chromium:
        return system_chromium

    log.warning("No chromium executable found, will try Playwright default.")
    return default_path


class PlaywrightSession:
    """Wraps a Playwright browser context to provide an HTTP-request interface.

    Uses Playwright's Page for navigation.  After SSO login, navigates
    via in-page link clicks or page.goto() to carry the session.
    """

    def __init__(self, playwright, browser, context, page):
        self._playwright = playwright
        self._browser = browser
        self._context = context
        self._page = page

    def get(self, url: str, **kwargs) -> "PlaywrightResponse":
        """GET request using the browser page (carries session cookies).

        First tries clicking an in-page link to the URL (same-origin
        navigation preserves the Drupal session).  Falls back to
        page.goto() if no matching link is found.
        """
        timeout = kwargs.pop("timeout", 30) * 1000  # convert s → ms

        # Try clicking a link on the current page first
        # (same-origin click navigation preserves session better than goto)
        from urllib.parse import urlparse
        path = urlparse(url).path
        if path:
            link = self._page.query_selector(f'a[href="{path}"]')
            if link and link.is_visible():
                log.debug("Navigating via click: %s", path)
                link.click()
                self._page.wait_for_load_state("networkidle", timeout=min(timeout, 15000))
                return PlaywrightResponse(self._page)

        # Fallback to page.goto
        log.debug("Navigating via goto: %s", url)
        self._page.goto(url, wait_until="load", timeout=timeout)
        self._page.wait_for_load_state("networkidle", timeout=min(timeout, 15000))
        return PlaywrightResponse(self._page)

    def close(self):
        """Clean up browser resources."""
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._playwright.stop()
        except Exception:
            pass

    @property
    def cookies(self):
        """Return cookies from the browser context (for debugging)."""
        return self._context.cookies()


class PlaywrightResponse:
    """Thin wrapper so PlaywrightSession.get() returns a requests-like object."""

    def __init__(self, page):
        self.url = page.url
        self.status_code = 200  # page.goto succeeded
        self.text = page.content()
        self.content = self.text.encode("utf-8")
        self.headers = {}

    @property
    def ok(self):
        return 200 <= self.status_code < 400


class FranceGalopAuth:
    """Handles authentication with France Galop via Playwright.

    Parameters
    ----------
    email : str
        France Galop account email (or set FG_EMAIL env var).
    password : str
        France Galop account password (or set FG_PASSWORD env var).
    headless : bool
        Run browser in headless mode (default True).
    """

    def __init__(
        self,
        email: Optional[str] = None,
        password: Optional[str] = None,
        headless: bool = True,
    ):
        self._email = email or os.environ.get("FG_EMAIL", "")
        self._password = password or os.environ.get("FG_PASSWORD", "")
        self._headless = headless

        if not self._email or not self._password:
            raise ValueError(
                "France Galop credentials required. "
                "Pass email/password or set FG_EMAIL and FG_PASSWORD env vars."
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def login(self) -> PlaywrightSession:
        """Perform OAuth login and return a PlaywrightSession.

        The PlaywrightSession wraps the browser context so that
        subsequent HTTP requests use the browser's cookie jar.

        Returns
        -------
        PlaywrightSession
            Session that shares cookies with the authenticated browser.
        """
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        log.info("Starting Playwright login to France Galop...")

        pw = sync_playwright().start()
        launch_kwargs = {"headless": self._headless}
        executable = _find_chromium_executable(pw.chromium.executable_path)
        if executable != pw.chromium.executable_path:
            log.info("Using chromium at: %s", executable)
            launch_kwargs["executable_path"] = executable

        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            user_agent=CHROME_USER_AGENT,
            locale="en-US",
        )
        page = context.new_page()

        try:
            # 1. Navigate to a protected page to trigger OAuth redirect
            log.info("Navigating to %s", LOGIN_TRIGGER_URL)
            page.goto(LOGIN_TRIGGER_URL, wait_until="load", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)
            log.info("Page loaded. URL: %s", page.url[:120])

            # If already authenticated, skip login
            if "france-galop.com" in page.url and "ciamlogin" not in page.url:
                log.info("Already authenticated — no OAuth redirect.")
                return PlaywrightSession(pw, browser, context)

            # 2. Wait for the CIAM login form
            log.info("Waiting for email input field...")
            email_selector = (
                'input[name="username"], '
                '#i0116, input[name="loginfmt"], input[type="email"], '
                '#signInName, input[name="signInName"]'
            )
            try:
                email_input = page.wait_for_selector(
                    email_selector, state="visible", timeout=30000,
                )
            except PWTimeout:
                self._dump_debug(page, "email-field-timeout")
                raise RuntimeError(
                    f"Email input not found after 30s. URL: {page.url}"
                )

            # 3. Fill email and submit
            log.info("Found email field, filling...")
            email_input.fill(self._email)
            next_btn = page.query_selector(
                '#idSIButton9, input[type="submit"]#next, '
                'button[type="submit"]'
            )
            if next_btn and next_btn.is_visible():
                next_btn.click()
            else:
                page.keyboard.press("Enter")

            # 4. Wait for password field
            log.info("Waiting for password field...")
            try:
                password_input = page.wait_for_selector(
                    'input[name="password"], input[type="password"], '
                    '#i0118, input[name="passwd"]',
                    state="visible", timeout=15000,
                )
            except PWTimeout:
                self._dump_debug(page, "password-field-timeout")
                raise RuntimeError(
                    f"Password field not found after 15s. URL: {page.url}"
                )

            # 5. Fill password and submit
            log.info("Filling password and submitting...")
            password_input.fill(self._password)
            page.keyboard.press("Enter")

            # 6. Handle "Stay signed in?" prompt
            try:
                page.wait_for_selector(
                    '#idBtn_Back, #idSIButton9, '
                    'button:has-text("No"), button:has-text("Yes")',
                    state="visible", timeout=5000,
                )
                yes_btn = page.query_selector(
                    '#idSIButton9, button:has-text("Yes")'
                )
                if yes_btn and yes_btn.is_visible():
                    log.info("Clicking 'Yes' on 'Stay signed in' prompt")
                    yes_btn.click()
                else:
                    no_btn = page.query_selector(
                        '#idBtn_Back, button:has-text("No")'
                    )
                    if no_btn:
                        log.info("Clicking 'No' on 'Stay signed in' prompt")
                        no_btn.click()
            except PWTimeout:
                log.debug("No 'Stay signed in' prompt appeared.")

            # 7. Wait for redirect back to france-galop.com.
            #    The SSO callback (/openid-connect/sso?code=...) processes
            #    the auth code and renders the page content directly at
            #    that URL (Drupal does NOT redirect to a different URL).
            log.info("Waiting for redirect back to france-galop.com...")
            try:
                page.wait_for_url(
                    "https://www.france-galop.com/**", timeout=30000,
                )
            except PWTimeout:
                error_el = page.query_selector(
                    '#usernameError, #passwordError, '
                    '.alert-error, [role="alert"]'
                )
                if error_el:
                    error_text = error_el.inner_text()
                    raise RuntimeError(
                        f"Login failed: {error_text}. URL: {page.url}"
                    )
                self._dump_debug(page, "redirect-timeout")
                raise RuntimeError(
                    f"Login timed out. Current URL: {page.url}"
                )

            # 8. Wait for the page to fully render
            page.wait_for_load_state("networkidle", timeout=15000)
            log.info("Login complete! URL: %s", page.url[:120])

            # Log cookies for debugging
            fg_cookies = [
                c for c in context.cookies()
                if "france-galop" in c.get("domain", "")
            ]
            log.info(
                "France-galop cookies after login: %s",
                [(c["name"], c["domain"], c.get("httpOnly")) for c in fg_cookies],
            )

            return PlaywrightSession(pw, browser, context, page)

        except Exception:
            # Clean up on failure
            browser.close()
            pw.stop()
            raise

    @staticmethod
    def _dump_debug(page, label: str):
        """Log page state for debugging login failures."""
        log.error("=== DEBUG DUMP: %s ===", label)
        log.error("URL: %s", page.url)
        inputs = page.query_selector_all("input")
        for inp in inputs:
            try:
                visible = inp.is_visible()
                attrs = inp.evaluate(
                    "el => ({id: el.id, name: el.name, type: el.type, "
                    "placeholder: el.placeholder})"
                )
                log.error(
                    "  <input id=%s name=%s type=%s placeholder=%s visible=%s>",
                    attrs.get("id"), attrs.get("name"),
                    attrs.get("type"), attrs.get("placeholder"), visible,
                )
            except Exception:
                pass
        try:
            page.screenshot(path=f"/tmp/fg_debug_{label}.png")
            log.error("Screenshot saved: /tmp/fg_debug_%s.png", label)
        except Exception:
            pass


def check_authenticated(pw_session: PlaywrightSession) -> bool:
    """Verify the PlaywrightSession is authenticated with France Galop.

    Navigates to a protected page and checks if we get content
    (authenticated) or the OAuth login page (not authenticated).
    """
    try:
        resp = pw_session.get(f"{SITE_BASE}/fr/courses/hier", timeout=15)
        if "ciamlogin" in resp.url:
            log.debug("Not authenticated — redirected to OAuth.")
            return False
        # If we're on france-galop.com, check for authenticated content
        if "france-galop.com" in resp.url:
            # Look for site elements that only appear when logged in
            if "Cherchez un cheval" in resp.text or "courses" in resp.url:
                return True
        log.debug("Auth check: URL=%s", resp.url[:120])
        return False
    except Exception as e:
        log.warning("Auth check failed: %s", e)
        return False
