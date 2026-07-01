"""Join parsed sectionals to the original live ratings for a date.

Indexes the day's ratings (UK ``figure_calibrated`` + France ``figure_final``)
so a parsed sectional runner can be matched back to the rating it should adjust.
Primary key is (normalised track, normalised horse); a unique-horse fallback
covers the France case where the Drive track name (e.g. DEAUVILLE) differs from
the ratings course code (DEA).
"""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from .normalize import normalise_name, normalise_track

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
UK_CSV = ROOT / "data" / "live" / "ratings_{date}.csv"
# UK live CSVs are gitignored; the committed audit carries the same columns and
# is the fallback source of yesterday's UK/IRE ratings in CI.
UK_AUDIT = ROOT / "output" / "uk_audit" / "{date}" / "audit_full_{date}.csv"
FR_CSV = ROOT / "data" / "france_live" / "ratings_{date}.csv"

Key = Tuple[str, str]  # (track_norm, horse_norm)


class RatingsIndex:
    def __init__(self):
        self.by_key: Dict[Key, dict] = {}
        self.track_country: Dict[str, str] = {}
        self._horse_counts: Counter = Counter()
        self._by_horse: Dict[str, dict] = {}

    def add(self, row: dict, track_norm: str, horse_norm: str):
        self.by_key[(track_norm, horse_norm)] = row
        self.track_country[track_norm] = row["country"]
        self._horse_counts[horse_norm] += 1
        self._by_horse[horse_norm] = row

    def finalise(self):
        # keep only unambiguous horse-name fallbacks
        self._by_horse = {h: r for h, r in self._by_horse.items() if self._horse_counts[h] == 1}
        return self

    def match(self, track: str, horse: str) -> Optional[dict]:
        hn = normalise_name(horse)
        row = self.by_key.get((normalise_track(track), hn))
        if row is not None:
            return row
        return self._by_horse.get(hn)     # unique-horse fallback (e.g. FR codes)

    def __len__(self):
        return len(self.by_key)


def _add_df(index: RatingsIndex, df: pd.DataFrame, rating_col: str, country: str, unit: str):
    for _, r in df.iterrows():
        course = str(r.get("courseName", ""))
        horse = str(r.get("horseName", ""))
        fig = r.get(rating_col)
        row = {
            "figure": float(fig) if pd.notna(fig) else None,
            "course": course,
            "race_number": int(r["raceNumber"]) if pd.notna(r.get("raceNumber")) else None,
            "distance": r.get("distance"),
            "going": r.get("going"),
            "surface": r.get("raceSurfaceName"),
            "race_class": r.get("raceClass"),
            "pos": int(r["positionOfficial"]) if pd.notna(r.get("positionOfficial")) and r["positionOfficial"] > 0 else None,
            "country": country,
            "unit": unit,
            "horse": horse,
        }
        index.add(row, normalise_track(course), normalise_name(horse))


def load_ratings_index(date: str) -> RatingsIndex:
    """Index the day's UK/IRE and France ratings for matching."""
    index = RatingsIndex()
    uk = Path(str(UK_CSV).format(date=date))
    if not uk.exists():
        uk = Path(str(UK_AUDIT).format(date=date))   # committed fallback
    fr = Path(str(FR_CSV).format(date=date))
    if uk.exists():
        try:
            _add_df(index, pd.read_csv(uk), "figure_calibrated", "GB/IRE", "f")
        except Exception as e:  # pragma: no cover
            log.warning("UK ratings read failed: %s", e)
    if fr.exists():
        try:
            _add_df(index, pd.read_csv(fr), "figure_final", "FR", "m")
        except Exception as e:  # pragma: no cover
            log.warning("FR ratings read failed: %s", e)
    index.finalise()
    log.info("Ratings index for %s: %d runners", date, len(index))
    return index


def match(index: RatingsIndex, track: str, horse: str) -> Optional[dict]:
    return index.match(track, horse)
