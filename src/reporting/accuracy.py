"""Recurring in-email accuracy panel.

The headline, always-true accuracy statement is our figures vs Timeform's
timefigure (the same ground truth ``scripts/audit_model_accuracy.py`` uses).
``refresh`` recomputes it from ``output/speed_figures.csv`` (regenerated each run
by the pipeline) and caches ``output/accuracy/panel_latest.json``; the emails
read that cache via ``rolling_panel``. Seeded with the last verified figures so
the panel renders even before the first CI refresh.
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


def compute_from_figures(path: Path = FIGURES_PATH) -> dict:
    df = pd.read_csv(path, low_memory=False)
    v = df[
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_calibrated"].notna()
    ]
    err = v["figure_calibrated"] - v["timefigure"]
    return {
        "n": int(len(v)),
        "corr": float(np.corrcoef(v["figure_calibrated"], v["timefigure"])[0, 1]),
        "mae": float(err.abs().mean()),
        "bias": float(err.mean()),
        "within_10": float((err.abs() <= 10).mean() * 100),
    }


def refresh(window: str = "2015–2026") -> Optional[dict]:
    """Recompute the panel from the figures file; cache to JSON. None on failure."""
    try:
        m = compute_from_figures()
    except Exception as e:
        log.warning("Accuracy refresh skipped (%s not usable): %s", FIGURES_PATH, e)
        return None
    m["window"] = window
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
