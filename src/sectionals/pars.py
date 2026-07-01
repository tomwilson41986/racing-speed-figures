"""Sectional and stride pars per (track, distance band, going group).

Pars are robust medians recomputed from the whole accumulating store each day,
so they sharpen as more data arrives. Every par row carries its own data count
``n`` and a sample-size tier (per STANDARD_TIMES_METHODOLOGY.md): production
(n>=50), provisional (20-49), insufficient (<20). The email shows ``n`` next to
the par so partial coverage is transparent.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from . import store
from .normalize import band_label

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
OUT_DIR = ROOT / "output" / "sectionals"

TIER_PRODUCTION = 50
TIER_PROVISIONAL = 20

KEY = ["track_norm", "distance_band", "going_group"]


def _tier(n: int) -> str:
    if n >= TIER_PRODUCTION:
        return "production"
    if n >= TIER_PROVISIONAL:
        return "provisional"
    return "insufficient"


def _runners_df(session) -> pd.DataFrame:
    rows = store.all_runners(session)
    return pd.DataFrame([
        {
            "track": r.track, "track_norm": r.track_norm, "country": r.country,
            "distance_band": r.distance_band, "going_group": r.going_group,
            "finishing_speed_pct": r.finishing_speed_pct,
            "final_furlong_s": r.final_furlong_s,
            "overall_time_s": r.overall_time_s,
            "stride_length_m": r.stride_length_m,
        }
        for r in rows
    ])


def compute_pars(session) -> pd.DataFrame:
    """Return one row per (track, band, going) with pars + counts + tier."""
    df = _runners_df(session)
    if df.empty:
        return pd.DataFrame(
            columns=KEY + ["track", "n", "n_fsp", "fsp_par", "fsp_sd",
                           "final_furlong_par", "n_stride", "stride_par", "tier", "band_label"]
        )
    df = df.dropna(subset=["distance_band", "going_group"])
    out = []
    for (tn, band, gg), g in df.groupby(KEY, dropna=True):
        fsp = g["finishing_speed_pct"].dropna()
        stride = g["stride_length_m"].dropna()
        ff = g["final_furlong_s"].dropna()
        n_fsp = int(len(fsp))
        out.append({
            "track_norm": tn,
            "track": g["track"].iloc[0],
            "distance_band": int(band),
            "band_label": band_label(int(band)),
            "going_group": gg,
            "n": int(len(g)),
            "n_fsp": n_fsp,
            "fsp_par": float(fsp.median()) if n_fsp else None,
            "fsp_sd": float(fsp.std(ddof=0)) if n_fsp > 1 else None,
            "final_furlong_par": float(ff.median()) if len(ff) else None,
            "n_stride": int(len(stride)),
            "stride_par": float(stride.median()) if len(stride) else None,
            "tier": _tier(n_fsp),
        })
    return pd.DataFrame(out).sort_values(["track_norm", "distance_band", "going_group"])


def par_lookup(pars: pd.DataFrame, track_norm: str, band: Optional[int],
               going_group: Optional[str]) -> Optional[dict]:
    """Fetch the par for a runner, relaxing going then distance if needed."""
    if pars is None or pars.empty or band is None:
        return None
    t = pars[pars["track_norm"] == track_norm]
    if t.empty:
        return None
    # exact
    for cond in [
        (t["distance_band"] == band) & (t["going_group"] == going_group),
        (t["distance_band"] == band),                                   # any going
        (t["distance_band"].sub(band).abs() <= 100),                    # +/-100m
    ]:
        m = t[cond]
        m = m[m["n_fsp"] > 0]
        if not m.empty:
            # prefer the largest sample
            return m.sort_values("n_fsp", ascending=False).iloc[0].to_dict()
    return None


def export(pars: pd.DataFrame, date: str) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dated = OUT_DIR / f"pars_{date}.csv"
    latest_csv = OUT_DIR / "pars_latest.csv"
    latest_pkl = OUT_DIR / "pars_latest.pkl"
    pars.to_csv(dated, index=False)
    pars.to_csv(latest_csv, index=False)
    pars.to_pickle(latest_pkl)
    log.info("Wrote %d par rows -> %s", len(pars), latest_csv)
    return latest_csv


def load_latest() -> Optional[pd.DataFrame]:
    p = OUT_DIR / "pars_latest.pkl"
    if p.exists():
        return pd.read_pickle(p)
    return None
