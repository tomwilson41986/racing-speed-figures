"""Normalisation helpers for matching sectionals to ratings.

Horse names and track names arrive from At The Races PDFs (UK/IRE/FR) and must
join to the ratings CSVs, so both sides are normalised the same way.
"""

from __future__ import annotations

import re
import unicodedata

# Country / suffix tokens that appear on names but not in our ratings feed.
_COUNTRY_SUFFIX = re.compile(r"\s*\((?:IRE|GB|FR|USA|GER|ITY|SPA|JPN|AUS|NZ|CAN|ARG|BRZ|SAF|UAE|HK)\)\s*$", re.I)
_NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")
_WS = re.compile(r"\s+")

FURLONG_M = 201.168


def normalise_name(name: str) -> str:
    """Uppercase, strip accents, country suffix and punctuation for matching."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.upper().strip()
    s = _COUNTRY_SUFFIX.sub("", s)
    s = _NON_ALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def normalise_track(track: str) -> str:
    """Canonical uppercase track key (accent- and punctuation-free)."""
    if not track:
        return ""
    s = unicodedata.normalize("NFKD", str(track))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.upper().strip()
    s = _NON_ALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


# Going -> group (UK/IRE turf + AW + French terms).
_GOING_GROUPS = [
    (("HEAVY", "LOURD", "TRES SOUPLE"), "Heavy"),
    (("SOFT", "SOUPLE", "YIELDING TO SOFT"), "Soft"),
    (("GOOD TO SOFT", "GOOD-SOFT", "BON SOUPLE", "COLLANT"), "Good-Soft"),
    (("GOOD TO FIRM", "GOOD-FIRM", "BON LEGER"), "Good-Firm"),
    (("GOOD", "BON", "STANDARD", "STANDARD TO SLOW", "STANDARD TO FAST", "PSF", "SLOW", "FAST"), "Good"),
    (("FIRM", "HARD"), "Firm"),
]


def going_group(going: str) -> str:
    if not going:
        return "Unknown"
    g = str(going).upper().strip()
    # check the most specific compound goings first
    for keys, label in [
        (("GOOD TO SOFT", "GOOD-SOFT", "BON SOUPLE"), "Good-Soft"),
        (("GOOD TO FIRM", "GOOD-FIRM", "BON LEGER"), "Good-Firm"),
    ]:
        if any(k in g for k in keys):
            return label
    for keys, label in _GOING_GROUPS:
        if any(k in g for k in keys):
            return label
    return going.strip().title()


def distance_to_m(distance, unit: str) -> float | None:
    """Convert a distance to metres. unit in {'f','m','y'}."""
    if distance is None:
        return None
    try:
        d = float(distance)
    except (TypeError, ValueError):
        return None
    if unit == "m":
        return d
    if unit == "f":
        return d * FURLONG_M
    if unit == "y":
        return d * 0.9144
    return d


def distance_band_m(distance_m: float | None, width: int = 100) -> int | None:
    """Bucket a metre distance to the nearest ``width`` metres (pars key)."""
    if distance_m is None:
        return None
    return int(round(distance_m / width) * width)


def band_label(distance_band: int | None) -> str:
    if distance_band is None:
        return "?"
    f = distance_band / FURLONG_M
    return f"{distance_band}m (~{f:.1f}f)"
