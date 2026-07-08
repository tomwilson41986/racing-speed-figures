"""Persistence for the Timeform reconciliation: per-day CSV, rolling history,
correction JSON and the rendered reports.

Committed outputs live under ``output/timeform_recon/`` (small text files);
``raw/`` holds dumped HTML/screenshots/JSON and is gitignored (CI artifact only).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .models import DayRecon, RawCapture

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
RECON_DIR = ROOT / "output" / "timeform_recon"
RAW_DIR = RECON_DIR / "raw"
HISTORY_CSV = RECON_DIR / "history.csv"
CORRECTION_JSON = RECON_DIR / "correction.json"

HISTORY_COLS = ["date", "n_tf", "n_matched", "n_unmatched", "corr", "mae",
                "bias", "within_10", "n_outliers"]


def day_csv_path(date: str) -> Path:
    return RECON_DIR / f"{date}.csv"


def report_md_path(date: str) -> Path:
    return RECON_DIR / f"{date}.md"


def report_html_path(date: str) -> Path:
    return RECON_DIR / f"{date}.html"


def write_day(day: DayRecon) -> Path:
    """Write the per-runner CSV and upsert the aggregate row into history.csv."""
    RECON_DIR.mkdir(parents=True, exist_ok=True)
    rows_df = pd.DataFrame(
        [
            {
                "date": day.date,
                "course": r.course,
                "horse": r.horse,
                "finish_pos": r.finish_pos,
                "our_fig": round(r.our_fig, 1),
                "tfig": round(r.tfig, 1),
                "diff": round(r.diff, 1),
                "outlier": r.outlier,
            }
            for r in day.rows
        ],
        columns=["date", "course", "horse", "finish_pos", "our_fig", "tfig",
                 "diff", "outlier"],
    ).sort_values(["course", "finish_pos"], na_position="last")
    path = day_csv_path(day.date)
    rows_df.to_csv(path, index=False)
    log.info("Wrote per-runner recon: %s (%d rows)", path, len(rows_df))
    _upsert_history(day)
    return path


def _upsert_history(day: DayRecon) -> None:
    rec = day.summary_record()
    df = load_history()
    df = df[df["date"] != day.date]  # idempotent: drop any existing row for the date
    df = pd.concat([df, pd.DataFrame([rec])], ignore_index=True)
    df = df.sort_values("date").reset_index(drop=True)
    df.to_csv(HISTORY_CSV, index=False)
    log.info("Updated history.csv (%d days)", len(df))


def load_history() -> pd.DataFrame:
    if HISTORY_CSV.exists():
        try:
            df = pd.read_csv(HISTORY_CSV)
            for c in HISTORY_COLS:
                if c not in df.columns:
                    df[c] = pd.NA
            return df[HISTORY_COLS]
        except Exception as e:  # pragma: no cover
            log.warning("history.csv unreadable (%s); starting fresh", e)
    return pd.DataFrame(columns=HISTORY_COLS)


def write_correction(corr: dict) -> Path:
    RECON_DIR.mkdir(parents=True, exist_ok=True)
    CORRECTION_JSON.write_text(json.dumps(corr, indent=2))
    log.info("Wrote correction.json (apply=%s, bias_applied=%s)",
             corr.get("apply"), corr.get("bias_applied"))
    return CORRECTION_JSON


def read_raw_capture(date: str) -> Optional[RawCapture]:
    """Rebuild a RawCapture from previously dumped ``raw/<date>/`` files.

    Lets us re-run parsing/reconciliation offline against a captured artifact
    (e.g. after the first CI run) without hitting the site again.
    """
    day_dir = RAW_DIR / date
    if not day_dir.exists():
        return None
    cap = RawCapture(date=date)
    index = day_dir / "index.html"
    if index.exists():
        cap.index_html = index.read_text(encoding="utf-8", errors="ignore")
    for html_file in sorted(day_dir.glob("*.html")):
        if html_file.name == "index.html":
            continue
        course = html_file.stem.replace("_", " ").title()
        cap.page_htmls[course] = html_file.read_text(encoding="utf-8", errors="ignore")
    for pf in sorted(day_dir.glob("payload_*.json")):
        body = pf.read_text(encoding="utf-8", errors="ignore")
        if body.strip():
            cap.json_payloads.append(body)
    return cap


def load_correction() -> Optional[dict]:
    if not CORRECTION_JSON.exists():
        return None
    try:
        return json.loads(CORRECTION_JSON.read_text())
    except Exception:  # pragma: no cover
        return None
