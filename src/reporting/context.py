"""Normalise the UK/IRE and France ratings CSVs into a ReportContext.

Each jurisdiction has its own column conventions (see ``JURISDICTIONS``); this
module is the single seam that maps them onto the shared dataclasses in
``models.py`` so one template renders any report.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from .models import Race, ReportContext, Runner, Section, TopPerformer

# ── Per-jurisdiction column conventions ───────────────────────────────
JURISDICTIONS = {
    "uk": {
        "rating_col": "figure_final_uk",   # resolved below (falls back)
        "distance": "uk",                  # yards + furlongs
        "ga_unit": "s/f",
        "ga_decimals": 3,
        "show_or": True,
        "time_col": "est_time",
    },
    "fr": {
        "rating_col": "figure_final",
        "distance": "fr",                  # metres
        "ga_unit": "s/m",
        "ga_decimals": 6,
        "show_or": False,
        "time_col": "finishingTime",
    },
}

# IMPORTANT: ``figure_final`` means DIFFERENT things in the two pipelines:
#   UK  : figure_final is a PRE-calibration intermediate (raw_figure + wfa_adj,
#         on a ~180-240 scale). The published UK figure is ``figure_calibrated``.
#   FR  : figure_final == figure_calibrated (the published French figure).
# So UK must resolve to figure_calibrated ONLY (never figure_final), while France
# uses figure_final. Getting this wrong shows UK figures at ~180-240.
_UK_RATING_CANDIDATES = ["figure_calibrated"]
_FR_RATING_CANDIDATES = ["figure_final", "figure_calibrated"]


def _rating_col(df: pd.DataFrame, jurisdiction: str) -> str:
    candidates = _FR_RATING_CANDIDATES if jurisdiction == "fr" else _UK_RATING_CANDIDATES
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"No rating column found in {list(df.columns)}")


def _int(v) -> Optional[int]:
    if v is None or pd.isna(v):
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _float(v) -> Optional[float]:
    if v is None or pd.isna(v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _pos(v) -> Optional[int]:
    p = _int(v)
    return p if (p is not None and p > 0) else None


def _silk_url(v) -> Optional[str]:
    """A clean silk URL or None.

    Guards against pandas NaN (which is *truthy*, so ``str(nan)`` would leak the
    literal ``"nan"`` into ``silk_url`` and trigger a bogus ``https://nan`` fetch).
    """
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):  # non-scalar; fall through to str()
        pass
    s = str(v).strip()
    return s or None


def _distance_str(first: pd.Series, mode: str) -> str:
    dist = _float(first.get("distance"))
    if mode == "fr":
        return f"{int(dist)}m" if dist else "?"
    # UK: prefer yards + furlongs
    yards = _float(first.get("distanceYards"))
    if yards and yards > 0:
        f = f" ({dist:.1f}f)" if dist else ""
        return f"{int(yards)}y{f}"
    return f"{dist:g}f" if dist else "?"


def build_section(
    df: pd.DataFrame,
    jurisdiction: str,
    title: Optional[str] = None,
) -> Section:
    """Group a ratings DataFrame into a Section of Races/Runners."""
    conf = JURISDICTIONS[jurisdiction]
    rating_col = _rating_col(df, jurisdiction)
    time_col = conf["time_col"]

    df = df.copy()
    df["_sort_pos"] = df["positionOfficial"].where(
        df["positionOfficial"].notna() & (df["positionOfficial"] > 0), other=9999
    )
    df = df.sort_values(["courseName", "raceNumber", "_sort_pos"])

    races: List[Race] = []
    for (course, race_num), race_df in df.groupby(
        ["courseName", "raceNumber"], sort=True
    ):
        first = race_df.iloc[0]
        field_size = int((race_df["positionOfficial"] > 0).sum()) or len(race_df)
        race = Race(
            course=str(course),
            race_number=_int(race_num) or 0,
            distance_str=_distance_str(first, conf["distance"]),
            race_name=(str(first.get("race_name")) if first.get("race_name") else None),
            going=(str(first.get("going")) if pd.notna(first.get("going")) else None),
            surface=(
                str(first.get("raceSurfaceName"))
                if pd.notna(first.get("raceSurfaceName"))
                else None
            ),
            race_class=(
                str(first.get("raceClass")) if pd.notna(first.get("raceClass")) else None
            ),
            going_allowance=_float(first.get("going_allowance")),
            median_or=_int(first.get("medianOR")) if conf["show_or"] else None,
            max_or=_int(first.get("maxOR")) if conf["show_or"] else None,
        )
        for _, r in race_df.iterrows():
            fig = _float(r.get(rating_col))
            beaten = _float(r.get("distanceCumulative"))
            race.runners.append(
                Runner(
                    horse=str(r.get("horseName", "?")),
                    pos=_pos(r.get("positionOfficial")),
                    field_size=field_size,
                    age=_int(r.get("horseAge")),
                    weight=(str(_int(r.get("weightCarried"))) if _int(r.get("weightCarried")) is not None else None),
                    official_rating=(
                        _int(r.get("officialRating"))
                        if conf["show_or"] and _int(r.get("officialRating"))
                        else None
                    ),
                    beaten=(beaten if beaten and beaten > 0 else None),
                    time=(f"{_float(r.get(time_col)):.2f}" if _float(r.get(time_col)) else None),
                    figure=(fig if fig is not None and fig >= 0 else None),
                    silk_url=_silk_url(r.get("silk_url")),
                )
            )
        races.append(race)

    return Section(title=title, races=races)


def _race_time_str(v) -> Optional[str]:
    """Normalise a raw off-time (HRB e.g. "5.30.") to a display string "5:30"."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    s = str(v).strip().rstrip(".")
    if not s:
        return None
    return s.replace(".", ":")


def top_performers(df: pd.DataFrame, jurisdiction: str, n: int = 10) -> List[TopPerformer]:
    rating_col = _rating_col(df, jurisdiction)
    rated = df[df[rating_col].notna() & (df[rating_col] >= 0)]
    top = rated.nlargest(n, rating_col)
    out: List[TopPerformer] = []
    for i, (_, r) in enumerate(top.iterrows(), 1):
        out.append(
            TopPerformer(
                rank=i,
                horse=str(r.get("horseName", "?")),
                course=str(r.get("courseName", "")),
                race_number=_int(r.get("raceNumber")) or 0,
                figure=float(r[rating_col]),
                pos=_pos(r.get("positionOfficial")),
                race_time=_race_time_str(r.get("raceTime")),
                race_name=(str(r.get("race_name")) if r.get("race_name") and pd.notna(r.get("race_name")) else None),
                silk_url=_silk_url(r.get("silk_url")),
            )
        )
    return out


def _human_date(target_date: str) -> str:
    try:
        d = datetime.strptime(target_date, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return str(target_date)
    return d.strftime("%A %d %B %Y")


def build_live_context(
    df: pd.DataFrame,
    jurisdiction: str,
    target_date: str,
    title: str,
    run_time: Optional[str] = None,
    accuracy_panel: Optional[dict] = None,
) -> ReportContext:
    """Single-jurisdiction live report (used by the UK and France emails)."""
    rating_col = _rating_col(df, jurisdiction)
    section = build_section(df, jurisdiction)
    rated = df[df[rating_col].notna() & (df[rating_col] >= 0)]
    n_races = df["race_id"].nunique() if "race_id" in df.columns else section.race_count
    return ReportContext(
        title=title,
        date_str=_human_date(target_date),
        subtitle_facts=[f"{len(rated)} runners", f"{n_races} races"],
        sections=[section],
        top_performers=top_performers(df, jurisdiction),
        accuracy_panel=accuracy_panel,
        footer_lines=[
            f"{len(rated)} runners rated across {n_races} races.",
            "Figures on the Timeform scale; silks © Racing Post.",
        ],
        run_time=run_time,
    )
