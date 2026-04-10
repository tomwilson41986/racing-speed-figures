"""Playwright-based OAuth authentication for France Galop.

France Galop uses Microsoft CIAM (Azure AD) for authentication.  The login
page is a JavaScript SPA that cannot be replicated with plain HTTP requests.
Playwright drives a headless Chromium browser to complete the login, then
extracts session cookies and injects them into a requests.Session for
efficient subsequent page scraping.

Usage:
    from src.france_galop.auth import FranceGalopAuth

    auth = FranceGalopAuth(email="...", password="...")
    session = auth.login()  # returns a requests.Session with auth cookies
"""

import glob
import logging
import os
import shutil
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

SITE_BASE = "https://www.france-galop.com"
# Use the French URL — avoids language redirects
LOGIN_TRIGGER_URL = f"{SITE_BASE}/fr/courses/hier"

CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _find_chromium_executable(default_path: str) -> str:
    """Return a working chromium executable path.

    Checks the Playwright default first, then searches /opt/pw-browsers/
    for any installed chromium revision.
    """
    if os.path.exists(default_path):
        return default_path

    # Search for any chromium revision under /opt/pw-browsers/
    for pattern in [
        "/opt/pw-browsers/chromium-*/chrome-linux/chrome",
        "/opt/pw-browsers/chromium-*/chrome-linux64/chrome",
    ]:
        matches = sorted(glob.glob(pattern), reverse=True)
        if matches:
            log.info("Found chromium fallback: %s", matches[0])
            return matches[0]

    # Check PATH
    system_chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if system_chromium:
        return system_chromium

    log.warning("No chromium executable found, will try Playwright default.")
    return default_path


class FranceGalopAuth:
    """Handles authentication with France Galop via Playwright.

    Launches a headless Chromium browser, navigates to a page that requires
    login, completes the Microsoft CIAM OAuth flow, extracts cookies, and
    transfers them to a requests.Session.

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
    def login(self) -> requests.Session:
        """Perform OAuth login and return a requests.Session with auth cookies.

        Returns
        -------
        requests.Session
            Session with authentication cookies set.

        Raises
        ------
        RuntimeError
            If login fails after retries.
        """
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        log.info("Starting Playwright login to France Galop...")

        with sync_playwright() as p:
            launch_kwargs = {"headless": self._headless}
            executable = _find_chromium_executable(p.chromium.executable_path)
            if executable != p.chromium.executable_path:
                log.info("Using chromium at: %s", executable)
                launch_kwargs["executable_path"] = executable

            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context(
                user_agent=CHROME_USER_AGENT,
                locale="en-US",
            )
            page = context.new_page()

            try:
                # 1. Navigate to a protected page to trigger OAuth redirect.
                #    Use "commit" — lightest wait; we handle readiness below.
                log.info("Navigating to %s", LOGIN_TRIGGER_URL)
                page.goto(LOGIN_TRIGGER_URL, wait_until="commit", timeout=30000)

                # Give the CIAM SPA time to bootstrap and render.
                # The page does multiple internal navigations/redirects.
                page.wait_for_load_state("domcontentloaded", timeout=30000)
                log.info("Page loaded. URL: %s", page.url[:120])

                # 2. Wait for the Microsoft CIAM login form to render.
                #    The page is a JS SPA; form elements are created by JS.
                #    We try a broad set of selectors and catch the timeout
                #    so we can dump debug info if nothing matches.
                log.info("Waiting for email input field...")
                email_selector = (
                    'input[name="username"], '
                    '#i0116, input[name="loginfmt"], input[type="email"], '
                    '#signInName, input[name="signInName"], '
                    '#email, input[name="email"]'
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

                # 3. Fill email and click Next
                log.info("Found email field, filling...")
                email_input.fill(self._email)

                # Click the "Next" button — try known MS login button IDs
                next_btn = page.query_selector(
                    '#idSIButton9, input[type="submit"]#next, '
                    'button[type="submit"]'
                )
                if next_btn and next_btn.is_visible():
                    next_btn.click()
                else:
                    page.keyboard.press("Enter")

                # 4. Wait for password field (appears after email validation)
                log.info("Waiting for password field...")
                try:
                    password_input = page.wait_for_selector(
                        'input[name="password"], input[type="password"], '
                        '#i0118, input[name="passwd"], #password',
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

                # 6. Handle "Stay signed in?" prompt if it appears
                try:
                    stay_signed_in = page.wait_for_selector(
                        '#idBtn_Back, #idSIButton9, '
                        'button:has-text("No"), button:has-text("Yes")',
                        state="visible",
                        timeout=5000,
                    )
                    if stay_signed_in:
                        yes_btn = page.query_selector(
                            '#idSIButton9, button:has-text("Yes")'
                        )
                        if yes_btn and yes_btn.is_visible():
                            log.info("Clicking 'Yes' on 'Stay signed in' prompt")
                            yes_btn.click()
                        else:
                            # Click "No" as fallback
                            no_btn = page.query_selector(
                                '#idBtn_Back, button:has-text("No")'
                            )
                            if no_btn:
                                log.info("Clicking 'No' on 'Stay signed in' prompt")
                                no_btn.click()
                except PWTimeout:
                    log.debug("No 'Stay signed in' prompt appeared.")

                # 7. Wait for redirect back to france-galop.com
                log.info("Waiting for redirect back to france-galop.com...")
                try:
                    page.wait_for_url(
                        "https://www.france-galop.com/**",
                        timeout=30000,
                    )
                except PWTimeout:
                    # Check if there's an error message on the page
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
                        f"Login timed out waiting for redirect. "
                        f"Current URL: {page.url}"
                    )

                log.info("Login successful! URL: %s", page.url[:120])

                # 8. Extract all cookies from the browser
                cookies = context.cookies()
                log.info("Extracted %d cookies from browser", len(cookies))

                # 9. Build a requests.Session with these cookies
                session = self._build_session(cookies)
                return session

            finally:
                browser.close()

    def _build_session(self, playwright_cookies: list[dict]) -> requests.Session:
        """Convert Playwright cookies into a requests.Session.

        Uses http.cookiejar for proper cookie handling — Playwright's
        cookie format needs careful mapping to requests' internal jar.
        """
        import http.cookiejar

        session = requests.Session()
        session.headers.update({"User-Agent": CHROME_USER_AGENT})

        for cookie in playwright_cookies:
            domain = cookie.get("domain", "")
            # Skip cookies for non-france-galop domains
            if "france-galop" not in domain and "france_galop" not in domain:
                log.debug("  Skipping cookie %s (domain=%s)", cookie["name"], domain)
                continue

            # Create a proper cookie via http.cookiejar
            c = http.cookiejar.Cookie(
                version=0,
                name=cookie["name"],
                value=cookie["value"],
                port=None,
                port_specified=False,
                domain=domain,
                domain_specified=bool(domain),
                domain_initial_dot=domain.startswith("."),
                path=cookie.get("path", "/"),
                path_specified=bool(cookie.get("path")),
                secure=cookie.get("secure", False),
                expires=int(cookie.get("expires", 0)) or None,
                discard=not cookie.get("expires"),
                comment=None,
                comment_url=None,
                rest={"HttpOnly": ""} if cookie.get("httpOnly") else {},
            )
            session.cookies.set_cookie(c)
            log.debug(
                "  Cookie: %s domain=%s path=%s secure=%s",
                cookie["name"], domain, cookie.get("path", "/"),
                cookie.get("secure", False),
            )

        log.info(
            "Built requests.Session with %d france-galop cookies",
            len(session.cookies),
        )
        return session

    @staticmethod
    def _dump_debug(page, label: str):
        """Log page state for debugging login failures."""
        log.error("=== DEBUG DUMP: %s ===", label)
        log.error("URL: %s", page.url)
        # Log all visible input elements on the page
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
        # Save screenshot
        try:
            page.screenshot(path=f"/tmp/fg_debug_{label}.png")
            log.error("Screenshot saved: /tmp/fg_debug_%s.png", label)
        except Exception:
            pass


def check_authenticated(session: requests.Session) -> bool:
    """Verify that the session is still authenticated with France Galop.

    Follows redirects and checks whether the final URL is on
    france-galop.com (authenticated) or ciamlogin (not authenticated).
    """
    try:
        resp = session.get(
            f"{SITE_BASE}/fr/courses/hier",
            allow_redirects=True,
            timeout=15,
        )
        final_url = resp.url
        log.debug("Auth check: status=%d, final URL=%s", resp.status_code, final_url[:120])

        # If we end up on ciamlogin, we're not authenticated
        if "ciamlogin" in final_url:
            log.debug("Not authenticated — redirected to OAuth.")
            return False

        # If we're still on france-galop.com, we're authenticated
        if "france-galop.com" in final_url:
            return True

        log.debug("Auth check: unexpected final URL: %s", final_url[:120])
        return False
    except requests.RequestException as e:
        log.warning("Auth check failed: %s", e)
        return False
