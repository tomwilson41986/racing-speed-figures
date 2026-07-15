"""France jockey silks (casaques) from the PMU programme — keyed by horse name.

Racing Post's silk scrape is blocked from CI (datacenter-IP WAF), and RP only
covers UK/IRE anyway. France has a cleaner, first-party source: the same PMU
Turfinfo API the ratings pipeline already calls carries a ready-made silk PNG
(``urlCasaque``) on every participant. We reuse it here so the France section of
the daily email shows colours without any proxy.

Everything is best-effort: any network/parse failure just leaves silks empty and
the email still renders (cards are silk-optional).
"""

from __future__ import annotations

import datetime
import logging
from typing import Dict, Optional

import pandas as pd

from src.sectionals.normalize import normalise_name

log = logging.getLogger(__name__)

# PMU marks each reunion with its country; we only want French fixtures (the
# France ratings pipeline rates French flat racing).
_FRANCE_LABELS = {"FRANCE", "FR"}


def _is_france(reunion: dict) -> bool:
    pays = reunion.get("pays")
    label = pays.get("libelle") if isinstance(pays, dict) else pays
    return str(label or "").strip().upper() in _FRANCE_LABELS


def fetch_silk_map(date: str, client=None) -> Dict[str, str]:
    """Build ``{horse_norm: casaque_png_url}`` for a race date from the PMU API.

    Best-effort: returns whatever it gathered (possibly empty) rather than raising.
    """
    try:
        from .pmu_client import PMUClient
    except Exception as e:  # pragma: no cover - import guard
        log.warning("PMU client unavailable — skipping France silks: %s", e)
        return {}

    try:
        d = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        log.warning("France silks: bad date %r", date)
        return {}

    client = client or PMUClient()
    silk_map: Dict[str, str] = {}
    try:
        programme = client.get_programme(d)
        reunions = (programme or {}).get("programme", {}).get("reunions", []) or []
        for reunion in reunions:
            if not _is_france(reunion):
                continue
            r_num = reunion.get("numOfficiel")
            for course in reunion.get("courses", []) or []:
                c_num = course.get("numOrdre")
                if r_num is None or c_num is None:
                    continue
                try:
                    url = client._build_url(d, f"R{r_num}", f"C{c_num}", "participants")
                    data = client._fetch_json(url) or {}
                except Exception as e:  # pragma: no cover - network best-effort
                    log.debug("France silks: R%sC%s fetch failed: %s", r_num, c_num, e)
                    continue
                for p in data.get("participants", []) or []:
                    name = normalise_name(str(p.get("nom", "")))
                    casaque = p.get("urlCasaque")
                    if name and casaque and name not in silk_map:
                        silk_map[name] = casaque
        log.info("PMU silks: mapped %d runners for %s", len(silk_map), date)
    except Exception as e:  # pragma: no cover - network best-effort
        log.warning("PMU silks fetch failed for %s: %s", date, e)
    return silk_map


def enrich_silks(df: pd.DataFrame, silk_map: Optional[Dict[str, str]] = None,
                 date: Optional[str] = None) -> pd.DataFrame:
    """Add/populate a ``silk_url`` column on ``df`` by matching horse name.

    Pass ``silk_map`` to skip the network (tests / reuse); otherwise it is fetched
    for ``date``.
    """
    if df is None or "horseName" not in df.columns:
        return df
    if silk_map is None:
        silk_map = fetch_silk_map(date) if date else {}
    if not silk_map:
        return df
    df = df.copy()
    df["silk_url"] = df["horseName"].map(
        lambda h: silk_map.get(normalise_name(str(h))) if pd.notna(h) else None
    )
    n = int(df["silk_url"].notna().sum())
    log.info("PMU silks: matched %d/%d France runners", n, len(df))
    return df
