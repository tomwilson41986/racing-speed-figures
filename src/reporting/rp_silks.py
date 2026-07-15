"""Racing Post racecard silks → per-runner silk URL, keyed by horse name.

Racing Post *results* are login-gated, but *racecards* are public and carry the
same per-runner jockey silks.  For a race date we fetch the day's cards from
``/racecards/<date>/``, pull ``horseName → silkImage`` pairs out of the embedded
JSON on each card, and expose a name-keyed map the reporting layer joins onto our
runners to populate ``silk_url`` (which ``silks.py`` then rasterises to a CID).

Everything is best-effort: any network/parse failure just leaves silks empty and
the email still renders (cards are silk-optional).  The pure ``parse_*`` helpers
take HTML strings so they unit-test offline; only ``fetch_silk_map`` touches the
network.

Politeness: one shared session, a real UA, and a jittered human pause between
card fetches (Racing Post asks nicely; we ask nicely back).
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from typing import Dict, List, Optional

import pandas as pd

from src.sectionals.normalize import normalise_name

log = logging.getLogger(__name__)

BASE = "https://www.racingpost.com"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

# Full browser-like header set. Racing Post sits behind a WAF that rejects
# datacenter traffic (GitHub Actions runners get HTTP 403/406); a complete
# header set is not enough to defeat an IP-based block, but it is the correct
# baseline and lets the request succeed anywhere the IP is not blocked.
_BROWSER_HEADERS = {
    "User-Agent": UA,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,image/apng,*/*;q=0.8"),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# HTTP statuses that indicate a WAF/IP block rather than a genuine "no data".
_BLOCKED_STATUSES = {401, 403, 405, 406, 429}


def _proxies() -> Optional[Dict[str, str]]:
    """Optional proxy for RP requests only.

    GitHub Actions runner IPs are WAF-blocked by Racing Post, so silks cannot be
    fetched from CI without an egress that RP does not block. Set ``RP_SILKS_PROXY``
    (e.g. ``http://user:pass@host:port``) to route *only* the silk requests through
    a residential/unblocked proxy, leaving the ratings feeds untouched. Falls back
    to the standard ``HTTPS_PROXY``/``HTTP_PROXY`` if those are set.
    """
    proxy = os.environ.get("RP_SILKS_PROXY", "").strip()
    if proxy:
        return {"http": proxy, "https": proxy}
    return None  # requests already honours HTTPS_PROXY/HTTP_PROXY automatically

# A racecard link for a given date: /racecards/<courseId>/<course>/<date>/<raceId>
_CARD_LINK_RE_TMPL = r"/racecards/\d+/[a-z0-9-]+/{date}/\d+"
# Once a race is off, the day's landing page swaps the racecard link for a RESULT
# link of the same shape. At ~21:00 (when the email sends) most of the card is
# finished, so we harvest silks from result pages too — otherwise only the last
# few upcoming races carry silks.
_RESULT_LINK_RE_TMPL = r"/results/\d+/[a-z0-9-]+/{date}/\d+"
# Within a card's embedded JSON each runner has "horseName":"…" followed (still
# inside the same runner object, so before the next "horseName") by
# "silkImage":"https://www.rp-assets.com/svg/….svg".
_RUNNER_RE = re.compile(
    r'"horseName":"((?:[^"\\]|\\.)*)"'
    r'(?:(?!"horseName")[\s\S]){0,6000}?'
    r'"silkImage":"(https://www\.rp-assets\.com/svg/(?:[^"\\]|\\.)*?\.svg)"'
)
# Result pages render silks as HTML, not JSON: each runner cell holds a
# rp-horseTable__silk <img src="…svg"> and, a little later in the same cell, a
# rp-horseTable__horse__name link with the horse's name.
_RESULT_RUNNER_RE = re.compile(
    r'rp-horseTable__silk"\s+src="(https://www\.rp-assets\.com/svg/[^"]+?\.svg)"'
    r'[\s\S]{0,1500}?'
    r'rp-horseTable__horse__name[^>]*>\s*([^<]+?)\s*<'
)


def _unescape(raw: str) -> str:
    """Decode JSON string escapes (\\u0026 → &, accents, …)."""
    try:
        return json.loads(f'"{raw}"')
    except Exception:
        return raw.replace("\\u0026", "&")


def parse_card_links(landing_html: str, date: str) -> List[str]:
    """Distinct racecard paths for ``date`` from a ``/racecards/<date>/`` page."""
    rx = re.compile(_CARD_LINK_RE_TMPL.format(date=re.escape(date)))
    seen: dict[str, None] = {}
    for m in rx.findall(landing_html):
        seen.setdefault(m, None)
    return list(seen.keys())


def parse_card_silks(card_html: str) -> Dict[str, str]:
    """Map normalised horse name → silk SVG URL from one racecard's HTML."""
    out: Dict[str, str] = {}
    for raw_name, raw_url in _RUNNER_RE.findall(card_html):
        name = normalise_name(_unescape(raw_name))
        url = _unescape(raw_url)
        if name and url and name not in out:
            out[name] = url
    return out


def parse_result_links(landing_html: str, date: str) -> List[str]:
    """Distinct result paths for ``date`` from a ``/racecards/<date>/`` page.

    After a race is run its landing-page link becomes ``/results/…`` — the result
    page still carries the runners' silks (as HTML)."""
    rx = re.compile(_RESULT_LINK_RE_TMPL.format(date=re.escape(date)))
    seen: dict[str, None] = {}
    for m in rx.findall(landing_html):
        seen.setdefault(m, None)
    return list(seen.keys())


def parse_result_silks(result_html: str) -> Dict[str, str]:
    """Map normalised horse name → silk SVG URL from one result page's HTML."""
    out: Dict[str, str] = {}
    for raw_url, raw_name in _RESULT_RUNNER_RE.findall(result_html):
        name = normalise_name(raw_name)
        if name and raw_url and name not in out:
            out[name] = raw_url
    return out


def _get_with_retry(session, url: str, timeout: int = 20, attempts: int = 3):
    """GET with a short backoff on transient/blocked responses.

    Returns the last response (even a blocked one, so the caller can log the
    status) or None if every attempt raised.
    """
    resp = None
    for i in range(attempts):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200 or resp.status_code not in _BLOCKED_STATUSES:
                return resp
        except Exception as e:  # pragma: no cover - network best-effort
            log.debug("RP GET failed %s (attempt %d/%d): %s", url, i + 1, attempts, e)
            resp = None
        if i < attempts - 1:
            time.sleep(2 ** i + random.uniform(0, 0.5))
    return resp


def fetch_silk_map(
    date: str,
    session=None,
    max_races: int = 90,
    delay: float = 1.5,
) -> Dict[str, str]:
    """Build ``{horse_norm: silk_url}`` for a race date from RP racecards.

    Harvests silks from both upcoming (racecard, JSON) and finished (result, HTML)
    races so coverage is full whatever time of day the email sends.

    Best-effort: returns whatever it gathered (possibly empty) rather than raising.
    """
    try:
        import requests
    except Exception:  # pragma: no cover
        log.warning("requests unavailable — skipping RP silks")
        return {}

    close = False
    if session is None:
        session = requests.Session()
        session.headers.update(_BROWSER_HEADERS)
        proxies = _proxies()
        if proxies:
            session.proxies.update(proxies)
            log.info("RP silks: routing through RP_SILKS_PROXY")
        close = True

    silk_map: Dict[str, str] = {}
    try:
        landing = _get_with_retry(session, f"{BASE}/racecards/{date}/", timeout=20)
        if landing is None or landing.status_code != 200:
            status = "no response" if landing is None else landing.status_code
            if landing is not None and landing.status_code in _BLOCKED_STATUSES:
                log.warning(
                    "RP racecards landing HTTP %s for %s — the request is being "
                    "blocked (WAF/IP block). Silks will be empty. Set RP_SILKS_PROXY "
                    "to a residential proxy to fetch silks from CI.", status, date)
            else:
                log.info("RP racecards landing HTTP %s for %s — no silks", status, date)
            return {}
        cards = parse_card_links(landing.text, date)          # upcoming (JSON)
        results = parse_result_links(landing.text, date)      # finished (HTML)
        # A race is either upcoming or finished, never both, so the two lists
        # don't overlap. Tag each with the parser it needs.
        targets = ([(p, parse_card_silks) for p in cards]
                   + [(p, parse_result_silks) for p in results])
        log.info("RP silks: %d cards + %d results for %s", len(cards), len(results), date)
        # Single attempt per race (no retry storm) + a circuit breaker: if RP
        # starts blocking, stop quickly rather than hammering 90 URLs and
        # escalating the block.
        consecutive_blocks = 0
        for i, (path, parse) in enumerate(targets[:max_races]):
            time.sleep(delay + random.uniform(0.2, 1.0))  # polite, not glacial
            try:
                r = session.get(BASE + path, timeout=20)
            except Exception as e:  # pragma: no cover - network best-effort
                log.debug("RP fetch failed %s: %s", path, e)
                continue
            if r.status_code == 200:
                consecutive_blocks = 0
                for name, url in parse(r.text).items():
                    silk_map.setdefault(name, url)
            elif r.status_code in _BLOCKED_STATUSES:
                consecutive_blocks += 1
                if consecutive_blocks >= 6:
                    log.warning("RP silks: %d consecutive HTTP %s — stopping early "
                                "to avoid hammering (mapped %d so far)",
                                consecutive_blocks, r.status_code, len(silk_map))
                    break
        log.info("RP silks: mapped %d runners for %s", len(silk_map), date)
    except Exception as e:  # pragma: no cover - network best-effort
        log.warning("RP silks fetch failed for %s: %s", date, e)
    finally:
        if close:
            session.close()
    return silk_map


def enrich_silks(df: pd.DataFrame, date: str,
                 silk_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Add/populate a ``silk_url`` column on ``df`` by matching horse name.

    Pass ``silk_map`` to skip the network (tests / reuse across jurisdictions).
    """
    if df is None or "horseName" not in df.columns:
        return df
    if silk_map is None:
        silk_map = fetch_silk_map(date)
    if not silk_map:
        return df
    df = df.copy()
    df["silk_url"] = df["horseName"].map(
        lambda h: silk_map.get(normalise_name(str(h))) if pd.notna(h) else None
    )
    n = int(df["silk_url"].notna().sum())
    log.info("RP silks: matched %d/%d runners for %s", n, len(df), date)
    return df
