"""Rolling bias/scale correction of our figures toward Timeform, with guardrails.

The correction is computed from the trailing ``window_days`` of ``history.csv``
and persisted to ``correction.json``.  It is **report-only by default**: ``apply``
is only set true when every guardrail passes, and even then ``live_ratings.py``
applies it only when the master env switch ``TIMEFORM_CORRECTION=1`` is set.  This
keeps a feedback loop onto an already well-calibrated model safe and reversible.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CorrectionConfig:
    window_days: int = 14       # trailing days pooled
    min_days: int = 7           # need at least this many days present
    min_sample: int = 200       # need at least this many matched runners pooled
    bias_threshold: float = 3.0 # ignore |bias| below this (noise), lb
    bias_cap: float = 6.0       # clip applied bias to ±cap, lb
    scale_lo: float = 0.92      # scale clamp
    scale_hi: float = 1.08
    scale_deadband: float = 0.05  # apply scale only if |scale-1| exceeds this


def compute_correction(history: pd.DataFrame,
                       cfg: CorrectionConfig = CorrectionConfig(),
                       as_of: Optional[str] = None) -> dict:
    """Compute the rolling correction dict from a history frame.

    ``history`` must have columns ``date``, ``n_matched``, ``bias`` (as written by
    :func:`src.timeform.store.write_day`).  Returns a JSON-serialisable dict.
    """
    result = {
        "as_of": as_of or datetime.date.today().isoformat(),
        "window_days": cfg.window_days,
        "days_used": 0,
        "n_matched": 0,
        "bias_hat": None,
        "scale_hat": 1.0,
        "apply": False,
        "bias_applied": 0.0,
        "scale_applied": 1.0,
        "cap": cfg.bias_cap,
        "min_sample": cfg.min_sample,
        "reason": "",
    }
    if history is None or len(history) == 0:
        result["reason"] = "no history"
        return result

    df = history.copy()
    df = df[df["date"].notna()].sort_values("date")
    df = df.tail(cfg.window_days)
    df = df[df["bias"].notna() & df["n_matched"].notna()]
    df = df[df["n_matched"] > 0]
    if len(df) == 0:
        result["reason"] = "no usable rows in window"
        return result

    n = float(df["n_matched"].sum())
    weights = df["n_matched"].astype(float).to_numpy()
    biases = df["bias"].astype(float).to_numpy()
    bias_hat = float(np.average(biases, weights=weights))

    result["days_used"] = int(len(df))
    result["n_matched"] = int(n)
    result["bias_hat"] = round(bias_hat, 3)

    # Guardrails — all must pass to apply anything.
    if len(df) < cfg.min_days:
        result["reason"] = f"only {len(df)} days (< {cfg.min_days})"
        return result
    if n < cfg.min_sample:
        result["reason"] = f"only {int(n)} matched (< {cfg.min_sample})"
        return result
    if abs(bias_hat) <= cfg.bias_threshold:
        result["reason"] = f"|bias| {abs(bias_hat):.2f} <= {cfg.bias_threshold} (noise)"
        return result

    bias_applied = float(np.clip(bias_hat, -cfg.bias_cap, cfg.bias_cap))
    result["apply"] = True
    result["bias_applied"] = round(bias_applied, 3)
    result["scale_applied"] = 1.0  # scale left at 1.0 until enough data justifies it
    result["reason"] = "guardrails passed"
    log.info("Correction: apply bias %.2f lb over %d days / %d runners",
             bias_applied, len(df), int(n))
    return result
