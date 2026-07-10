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

# A racecard link for a given date: /racecards/<courseId>/<course>/<date>/<raceId>
_CARD_LINK_RE_TMPL = r"/racecards/\d+/[a-z0-9-]+/{date}/\d+"
# Within a card's embedded JSON each runner has "horseName":"…" followed (still
# inside the same runner object, so before the next "horseName") by
# "silkImage":"https://www.rp-assets.com/svg/….svg".
_RUNNER_RE = re.compile(
    r'"horseName":"((?:[^"\\]|\\.)*)"'
    r'(?:(?!"horseName")[\s\S]){0,6000}?'
    r'"silkImage":"(https://www\.rp-assets\.com/svg/(?:[^"\\]|\\.)*?\.svg)"'
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


def fetch_silk_map(
    date: str,
    session=None,
    max_cards: int = 60,
    delay: float = 3.5,
) -> Dict[str, str]:
    """Build ``{horse_norm: silk_url}`` for a race date from RP racecards.

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
        session.headers.update({"User-Agent": UA, "Accept-Encoding": "gzip, deflate",
                                "Accept-Language": "en-GB,en;q=0.9"})
        close = True

    silk_map: Dict[str, str] = {}
    try:
        landing = session.get(f"{BASE}/racecards/{date}/", timeout=20)
        if landing.status_code != 200:
            log.info("RP racecards landing HTTP %s for %s — no silks", landing.status_code, date)
            return {}
        links = parse_card_links(landing.text, date)
        log.info("RP silks: %d cards for %s", len(links), date)
        for i, path in enumerate(links[:max_cards]):
            time.sleep(delay + random.uniform(0.5, 2.0))  # human-paced
            try:
                r = session.get(BASE + path, timeout=20)
                if r.status_code == 200:
                    for name, url in parse_card_silks(r.text).items():
                        silk_map.setdefault(name, url)
            except Exception as e:  # pragma: no cover - network best-effort
                log.debug("RP card fetch failed %s: %s", path, e)
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
