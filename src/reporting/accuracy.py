"""Recurring in-email accuracy panel.

The headline, always-true accuracy statement is our figures vs Timeform's
timefigure (the same ground truth ``scripts/audit_model_accuracy.py`` uses),
measured on the OUT-OF-SAMPLE window only: the current year, which no
calibration layer has trained on.  ``refresh`` recomputes it from
``output/speed_figures.csv`` (regenerated each run by the pipeline) and caches
``output/accuracy/panel_latest.json``; the emails read that cache via
``rolling_panel``. Seeded with the last verified figures so the panel renders
even before the first CI refresh.
"""

from __future__ import annotations

import datetime
import json
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
FIGURES_PATH = ROOT / "output" / "speed_figures.csv"
PANEL_JSON = ROOT / "output" / "accuracy" / "panel_latest.json"


# Minimum out-of-sample rows before the OOS window is trusted for the
# headline (early January the current year has almost no racing yet).
MIN_OOS_ROWS = 100


def compute_from_figures(path: Path = FIGURES_PATH) -> dict:
    df = pd.read_csv(path, low_memory=False)
    v = df[
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_calibrated"].notna()
    ]

    # Every calibration layer (quadratic cal, GBR, quantile map, OOS
    # corrections, rescaling) trains on years <= max_year - 1, so only
    # the current (max) year is genuinely out-of-sample.  Reporting the
    # full 2015+ window presented in-sample agreement as model accuracy.
    if "source_year" in v.columns and v["source_year"].notna().any():
        max_year = int(v["source_year"].max())
        oos = v[v["source_year"] == max_year]
    else:
        max_year, oos = None, v.iloc[0:0]

    if len(oos) >= MIN_OOS_ROWS:
        sample = oos
        window = f"out-of-sample {max_year}"
        in_sample = False
    else:
        sample = v
        window = "in-sample (insufficient OOS data)"
        in_sample = True

    err = sample["figure_calibrated"] - sample["timefigure"]
    return {
        "n": int(len(sample)),
        "corr": float(np.corrcoef(sample["figure_calibrated"], sample["timefigure"])[0, 1]),
        "mae": float(err.abs().mean()),
        "bias": float(err.mean()),
        "within_10": float((err.abs() <= 10).mean() * 100),
        "window": window,
        "in_sample": in_sample,
    }


def refresh() -> Optional[dict]:
    """Recompute the panel from the figures file; cache to JSON. None on failure."""
    try:
        m = compute_from_figures()
    except Exception as e:
        log.warning("Accuracy refresh skipped (%s not usable): %s", FIGURES_PATH, e)
        return None
    m["as_of"] = datetime.date.today().isoformat()
    PANEL_JSON.parent.mkdir(parents=True, exist_ok=True)
    PANEL_JSON.write_text(json.dumps(m, indent=2))
    log.info("Accuracy panel refreshed: r=%.3f MAE=%.2f n=%d", m["corr"], m["mae"], m["n"])
    return m


def _human_n(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}m"
    if n >= 1000:
        return f"{n / 1000:.0f}k"
    return str(n)


def rolling_panel() -> Optional[dict]:
    """Panel dict for the email, or None if no cache exists."""
    if not PANEL_JSON.exists():
        return None
    try:
        m = json.loads(PANEL_JSON.read_text())
    except Exception:
        return None
    return {
        "window": f"Model accuracy vs Timeform · {m.get('window', '')}",
        "stats": [
            {"label": "Correlation", "value": f"{m['corr']:.2f}"},
            {"label": "Mean error", "value": f"{m['mae']:.1f} lb"},
            {"label": "Within ±10 lb", "value": f"{m['within_10']:.0f}%"},
            {"label": "Runners", "value": _human_n(m["n"])},
        ],
    }


if __name__ == "__main__":
    refresh()
