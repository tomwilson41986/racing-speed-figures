"""Dataclasses shared across the Timeform reconciliation pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MeetingRef:
    """A meeting listed on the ``results/yesterday`` index page."""

    course: str
    date: str
    result_url: str
    country: Optional[str] = None


@dataclass
class RaceResultRow:
    """One runner parsed from a Timeform race result (HTML or JSON)."""

    course: str
    horse: str
    tfig: Optional[float] = None
    finish_pos: Optional[int] = None
    race_time: Optional[str] = None
    race_no: Optional[int] = None
    source: str = "html"  # "html" | "json" — which parse path produced this row


@dataclass
class ReconRow:
    """A Timeform runner matched to one of our figures."""

    course: str
    horse: str
    our_fig: float
    tfig: float
    diff: float  # our_fig - tfig  (positive => we rate higher than Timeform)
    finish_pos: Optional[int] = None
    country: str = "GB/IRE"
    outlier: bool = False


@dataclass
class RawCapture:
    """Raw material dumped from one Timeform fetch (before parsing)."""

    date: str
    index_html: str = ""
    page_htmls: Dict[str, str] = field(default_factory=dict)   # course -> html
    json_payloads: List[str] = field(default_factory=list)     # sniffed XHR bodies


@dataclass
class DayRecon:
    """Full reconciliation for one race date."""

    date: str
    n_tf: int              # TFig rows parsed
    n_matched: int         # matched to one of our figures
    n_unmatched: int
    corr: Optional[float]
    mae: Optional[float]
    bias: Optional[float]  # mean(our_fig - tfig)
    within_10: Optional[float]
    per_course: Dict[str, dict] = field(default_factory=dict)
    rows: List[ReconRow] = field(default_factory=list)

    @property
    def n_outliers(self) -> int:
        return sum(1 for r in self.rows if r.outlier)

    def summary_record(self) -> dict:
        """One flat row for ``history.csv``."""
        return {
            "date": self.date,
            "n_tf": self.n_tf,
            "n_matched": self.n_matched,
            "n_unmatched": self.n_unmatched,
            "corr": _round(self.corr, 4),
            "mae": _round(self.mae, 3),
            "bias": _round(self.bias, 3),
            "within_10": _round(self.within_10, 2),
            "n_outliers": self.n_outliers,
        }


def _round(x: Optional[float], n: int) -> Optional[float]:
    return round(x, n) if x is not None else None
