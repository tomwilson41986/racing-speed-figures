"""Match Timeform TFigs to our figures and score the day.

Pure/offline: takes a parsed TFig DataFrame and joins it to the day's ratings
index (``src.sectionals.matching``), producing a :class:`DayRecon`.  Metric
definitions mirror ``src.reporting.accuracy.compute_from_figures`` so the numbers
line up with the existing in-email accuracy panel.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np
import pandas as pd

from src.sectionals.matching import RatingsIndex, load_ratings_index

from .models import DayRecon, ReconRow

log = logging.getLogger(__name__)

# Absolute-diff (lb) above which a matched runner is always flagged an outlier.
OUTLIER_LB = 12.0
# Robust z-score multiplier for the MAD-based outlier test.
OUTLIER_Z = 3.5
_MAD_TO_SIGMA = 1.4826


def _metrics(diffs: np.ndarray, our: np.ndarray, tf: np.ndarray) -> dict:
    """corr / MAE / bias / within±10 for a set of matched pairs."""
    n = len(diffs)
    if n == 0:
        return {"corr": None, "mae": None, "bias": None, "within_10": None}
    corr = None
    if n >= 2 and np.std(our) > 0 and np.std(tf) > 0:
        corr = float(np.corrcoef(our, tf)[0, 1])
    return {
        "corr": corr,
        "mae": float(np.abs(diffs).mean()),
        "bias": float(diffs.mean()),
        "within_10": float((np.abs(diffs) <= 10).mean() * 100),
    }


def _flag_outliers(rows: List[ReconRow]) -> None:
    """Set ``outlier`` on rows using a robust MAD test plus a hard lb cap."""
    if not rows:
        return
    diffs = np.array([r.diff for r in rows], dtype=float)
    med = float(np.median(diffs))
    mad = float(np.median(np.abs(diffs - med)))
    sigma = mad * _MAD_TO_SIGMA
    for r in rows:
        robust = sigma > 0 and abs(r.diff - med) > OUTLIER_Z * sigma
        r.outlier = bool(robust or abs(r.diff) > OUTLIER_LB)


def match_rows(tf_df: pd.DataFrame, index: RatingsIndex) -> List[ReconRow]:
    """Join parsed TFig rows to our GB/IRE figures via the ratings index."""
    rows: List[ReconRow] = []
    for _, tr in tf_df.iterrows():
        tfig = tr.get("tfig")
        course = str(tr.get("course", ""))
        horse = str(tr.get("horse", ""))
        if pd.isna(tfig) or not horse:
            continue
        hit = index.match(course, horse)
        if hit is None or hit.get("figure") is None:
            continue
        if hit.get("country") != "GB/IRE":
            continue  # Timeform TFigs are UK/IRE; skip any France match
        our_fig = float(hit["figure"])
        tf_val = float(tfig)
        rows.append(ReconRow(
            course=hit.get("course") or course,
            horse=hit.get("horse") or horse,
            our_fig=our_fig,
            tfig=tf_val,
            diff=our_fig - tf_val,
            finish_pos=tr.get("finish_pos") if not pd.isna(tr.get("finish_pos")) else hit.get("pos"),
            country="GB/IRE",
        ))
    return rows


def _per_course(rows: List[ReconRow]) -> dict:
    out: dict = {}
    by: dict[str, List[ReconRow]] = {}
    for r in rows:
        by.setdefault(r.course, []).append(r)
    for course, rs in by.items():
        diffs = np.array([r.diff for r in rs], dtype=float)
        out[course] = {
            "n": len(rs),
            "mae": round(float(np.abs(diffs).mean()), 2),
            "bias": round(float(diffs.mean()), 2),
        }
    return out


def reconcile(date: str, tf_df: pd.DataFrame,
              index: Optional[RatingsIndex] = None) -> DayRecon:
    """Reconcile a day's parsed TFigs against our figures."""
    if index is None:
        index = load_ratings_index(date)
    n_tf = int(tf_df["tfig"].notna().sum()) if "tfig" in tf_df.columns else 0
    rows = match_rows(tf_df, index)
    _flag_outliers(rows)

    diffs = np.array([r.diff for r in rows], dtype=float)
    our = np.array([r.our_fig for r in rows], dtype=float)
    tf = np.array([r.tfig for r in rows], dtype=float)
    m = _metrics(diffs, our, tf)

    day = DayRecon(
        date=date,
        n_tf=n_tf,
        n_matched=len(rows),
        n_unmatched=max(0, n_tf - len(rows)),
        corr=m["corr"],
        mae=m["mae"],
        bias=m["bias"],
        within_10=m["within_10"],
        per_course=_per_course(rows),
        rows=rows,
    )
    log.info("Recon %s: matched %d/%d  MAE=%s bias=%s corr=%s",
             date, day.n_matched, day.n_tf,
             None if m["mae"] is None else round(m["mae"], 2),
             None if m["bias"] is None else round(m["bias"], 2),
             None if m["corr"] is None else round(m["corr"], 3))
    return day
