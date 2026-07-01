"""Build At The Races race-review links.

ATR day replays live at ``/replays/DD-Month-YYYY``; individual races are reached
from the racecard/result URL ``/racecard/{Course}/{DD-Month-YYYY}/{HHMM}`` which
redirects to the result (with the replay) once the race is off. The per-race
pattern should be spot-checked against a live page; ``day_replays_url`` is the
always-valid fallback.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

BASE = "https://www.attheraces.com"


def _fmt_date(date: str) -> Optional[str]:
    try:
        return datetime.strptime(date, "%Y-%m-%d").strftime("%d-%B-%Y")
    except (TypeError, ValueError):
        return None


def _fmt_time(race_time: Optional[str]) -> Optional[str]:
    if not race_time:
        return None
    digits = re.sub(r"\D", "", str(race_time))     # "13_15" / "13:15" -> "1315"
    return digits if len(digits) == 4 else None


def _slug_track(track: str) -> str:
    s = re.sub(r"[^A-Za-z0-9 ]+", "", str(track)).strip()
    return re.sub(r"\s+", "-", s.title())


def day_replays_url(date: str) -> Optional[str]:
    d = _fmt_date(date)
    return f"{BASE}/replays/{d}" if d else None


def race_replay_url(track: str, date: str, race_time: Optional[str]) -> Optional[str]:
    d = _fmt_date(date)
    t = _fmt_time(race_time)
    slug = _slug_track(track)
    if not (d and slug):
        return day_replays_url(date)
    if not t:
        return f"{BASE}/racecard/{slug}/{d}"
    return f"{BASE}/racecard/{slug}/{d}/{t}"
