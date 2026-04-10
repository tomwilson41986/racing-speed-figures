"""Playwright-based OAuth authentication for France Galop.

France Galop uses Azure AD B2C (CIAM) for authentication, which involves
JavaScript-heavy OAuth flows.  Playwright drives a headless Chromium browser
to complete the login, then extracts session cookies and injects them into
a requests.Session for efficient subsequent page scraping.

Usage:
    from src.france_galop.auth import FranceGalopAuth

    auth = FranceGalopAuth(email="...", password="...")
    session = auth.login()  # returns a requests.Session with auth cookies
"""

import logging
import os
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

SITE_BASE = "https://www.france-galop.com"
LOGIN_URL = f"{SITE_BASE}/en/racing/yesterday"

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

    import glob

    # Search for any chromium revision under /opt/pw-browsers/
    patterns = [
        "/opt/pw-browsers/chromium-*/chrome-linux/chrome",
        "/opt/pw-browsers/chromium-*/chrome-linux64/chrome",
    ]
    for pattern in patterns:
        matches = sorted(glob.glob(pattern), reverse=True)
        if matches:
            log.info("Found chromium fallback: %s", matches[0])
            return matches[0]

    # Last resort: check PATH
    import shutil
    system_chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if system_chromium:
        return system_chromium

    log.warning("No chromium executable found, will try Playwright default.")
    return default_path


class FranceGalopAuth:
    """Handles authentication with France Galop via Playwright.

    Launches a headless Chromium browser, navigates to the login page,
    fills credentials, waits for redirect, then transfers cookies to
    a requests.Session for all subsequent HTTP calls.

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

        Launches headless Chromium, navigates to France Galop, handles the
        Azure AD B2C login flow, extracts cookies, and returns an authenticated
        requests.Session.

        Returns
        -------
        requests.Session
            Session with authentication cookies set.

        Raises
        ------
        RuntimeError
            If login fails after retries.
        """
        from playwright.sync_api import sync_playwright

        log.info("Starting Playwright login to France Galop...")

        with sync_playwright() as p:
            launch_kwargs = {"headless": self._headless}

            # Resolve chromium executable.  Playwright's bundled version
            # may not match the system-installed revision, so we search
            # common paths if the default is missing.
            executable = _find_chromium_executable(p.chromium.executable_path)
            if executable != p.chromium.executable_path:
                log.info("Using chromium at: %s", executable)
                launch_kwargs["executable_path"] = executable

            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context(user_agent=CHROME_USER_AGENT)
            page = context.new_page()

            try:
                # Navigate to a page that requires authentication.
                # This triggers the OAuth redirect to Azure AD B2C.
                log.debug("Navigating to %s", LOGIN_URL)
                page.goto(LOGIN_URL, wait_until="networkidle", timeout=30000)

                # The page should redirect to the Azure AD B2C login form.
                # Wait for the email input field to appear.
                log.debug("Current URL: %s", page.url)

                # Azure AD B2C login form: try common field selectors
                email_selector = self._find_input_selector(
                    page,
                    [
                        'input[type="email"]',
                        'input[name="loginfmt"]',
                        'input[name="signInName"]',
                        'input[id="signInName"]',
                        'input[id="email"]',
                        'input[name="email"]',
                    ],
                )
                if not email_selector:
                    # Dump page content for debugging
                    log.error(
                        "Could not find email input field. Page URL: %s",
                        page.url,
                    )
                    raise RuntimeError(
                        f"Login form not found. Current URL: {page.url}"
                    )

                log.debug("Found email field: %s", email_selector)
                page.fill(email_selector, self._email)

                # Some Azure B2C flows have a "Next" button before password
                next_button = self._find_button(
                    page,
                    [
                        'input[type="submit"][id="next"]',
                        'button[type="submit"]#next',
                        'button:has-text("Next")',
                    ],
                )
                if next_button:
                    log.debug("Clicking 'Next' button")
                    page.click(next_button)
                    page.wait_for_load_state("networkidle", timeout=10000)

                # Fill password
                password_selector = self._find_input_selector(
                    page,
                    [
                        'input[type="password"]',
                        'input[name="passwd"]',
                        'input[name="password"]',
                        'input[id="password"]',
                    ],
                )
                if not password_selector:
                    raise RuntimeError(
                        f"Password field not found. Current URL: {page.url}"
                    )

                log.debug("Found password field: %s", password_selector)
                page.fill(password_selector, self._password)

                # Click sign-in / submit button
                submit_selector = self._find_button(
                    page,
                    [
                        'button[type="submit"]#next',
                        'input[type="submit"]#next',
                        'button[type="submit"]',
                        'input[type="submit"]',
                        'button:has-text("Sign in")',
                        'button:has-text("Log in")',
                        'button:has-text("Se connecter")',
                    ],
                )
                if not submit_selector:
                    raise RuntimeError(
                        f"Submit button not found. Current URL: {page.url}"
                    )

                log.debug("Clicking submit button: %s", submit_selector)
                page.click(submit_selector)

                # Wait for redirect back to france-galop.com
                log.debug("Waiting for redirect back to france-galop.com...")
                page.wait_for_url(
                    f"{SITE_BASE}/**",
                    timeout=30000,
                )
                log.info("Login successful. URL: %s", page.url)

                # Extract cookies from the browser context
                cookies = context.cookies()
                log.debug("Extracted %d cookies", len(cookies))

                # Build a requests.Session with the auth cookies
                session = self._build_session(cookies)
                return session

            finally:
                browser.close()

    def _find_input_selector(self, page, selectors: list[str]) -> Optional[str]:
        """Try multiple selectors, return the first one that matches a visible element."""
        for selector in selectors:
            try:
                el = page.query_selector(selector)
                if el and el.is_visible():
                    return selector
            except Exception:
                continue
        return None

    def _find_button(self, page, selectors: list[str]) -> Optional[str]:
        """Try multiple button selectors, return the first visible match."""
        for selector in selectors:
            try:
                el = page.query_selector(selector)
                if el and el.is_visible():
                    return selector
            except Exception:
                continue
        return None

    def _build_session(self, playwright_cookies: list[dict]) -> requests.Session:
        """Convert Playwright cookies into a requests.Session."""
        session = requests.Session()
        session.headers.update({"User-Agent": CHROME_USER_AGENT})

        for cookie in playwright_cookies:
            session.cookies.set(
                name=cookie["name"],
                value=cookie["value"],
                domain=cookie.get("domain", ""),
                path=cookie.get("path", "/"),
            )

        log.info(
            "Built requests.Session with %d cookies", len(playwright_cookies)
        )
        return session


def check_authenticated(session: requests.Session) -> bool:
    """Verify that the session is still authenticated with France Galop.

    Makes a lightweight request to a page requiring auth and checks
    whether we get redirected to the login page.
    """
    try:
        resp = session.get(
            f"{SITE_BASE}/en/racing/yesterday",
            allow_redirects=False,
            timeout=15,
        )
        # If authenticated, we get 200; if not, we get a 302 to the OAuth endpoint
        if resp.status_code == 200:
            return True
        log.debug(
            "Auth check: status=%d, location=%s",
            resp.status_code,
            resp.headers.get("Location", ""),
        )
        return False
    except requests.RequestException as e:
        log.warning("Auth check failed: %s", e)
        return False
