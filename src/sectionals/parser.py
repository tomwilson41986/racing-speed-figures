"""Parse At The Races sectional PDFs into structured per-runner records.

The exact ATR layout varies by track/country, so this is a defensive extractor:
it pulls the raw text (and tables) and applies heuristics to recover, per runner,
the finishing-speed %, overall time, final-furlong time, per-furlong splits and
stride data where present. Every parse keeps the raw text so the heuristics can
be calibrated against real PDFs (use ``dump_text`` / the CLI ``dump`` command).

NOTE: the regexes below are a starting point and MUST be validated against the
real PDFs once Drive access (the service account) is in place — see
``docs``/the plan. Unrecognised layouts yield an empty runner list plus the raw
text and an ``error`` marker rather than crashing.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from .normalize import distance_band_m, distance_to_m, going_group, normalise_name

log = logging.getLogger(__name__)

# ── regex building blocks ─────────────────────────────────────────────
_TIME = r"(?:\d{1,2}:)?\d{1,2}\.\d{1,2}"           # 1:11.20 or 23.45
_PCT = r"\d{2,3}(?:\.\d)?\s*%"                       # 104.2%
_DIST_F = re.compile(r"(\d(?:\.\d)?)\s*f\b", re.I)
_DIST_M = re.compile(r"(\d{3,4})\s*m\b", re.I)
_GOING = re.compile(r"going[:\s]+([A-Za-z /-]+)", re.I)
_FSP_LABEL = re.compile(r"(?:finish(?:ing)?\s*speed|fsp)[^0-9]{0,12}(" + _PCT + ")", re.I)
_STRIDE = re.compile(r"stride[^0-9]{0,12}(\d(?:\.\d{1,2})?)\s*m", re.I)


@dataclass
class RunnerSectional:
    horse: str
    finish_pos: Optional[int] = None
    overall_time_s: Optional[float] = None
    final_furlong_s: Optional[float] = None
    finishing_speed_pct: Optional[float] = None
    stride_length_m: Optional[float] = None
    stride_freq: Optional[float] = None
    splits: List[float] = field(default_factory=list)


@dataclass
class RaceSectionals:
    track: str
    race_date: str
    race_time: Optional[str]
    distance_m: Optional[float] = None
    going: Optional[str] = None
    runners: List[RunnerSectional] = field(default_factory=list)
    raw_text: str = ""
    error: Optional[str] = None

    @property
    def race_key(self) -> str:
        return f"{self.race_date}_{self.track}_{self.race_time or '?'}".replace(" ", "")


# ── filename → context ────────────────────────────────────────────────
_FN_TIME = re.compile(r"(\d{1,2})[_:. ](\d{2})")
_FN_DATE = re.compile(r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})")


def parse_filename(name: str, folder_date: Optional[str] = None,
                   folder_track: Optional[str] = None) -> dict:
    """Recover (race_time, track, race_date) from an ATR PDF filename.

    Filenames look like:
      '13_15 _ Doncaster _ Friday 26th June 2026 _ At The Races.pdf'
      'Doncaster Racing Results _ 26th June 2026 13_15.pdf'
    Folder date/track (from the Drive tree) are used as fallbacks.
    """
    base = re.sub(r"\.pdf$", "", name, flags=re.I)
    time = None
    m = _FN_TIME.search(base)
    if m:
        time = f"{int(m.group(1)):02d}{m.group(2)}"
    date = folder_date
    md = _FN_DATE.search(base)
    if md:
        try:
            date = datetime.strptime(
                f"{int(md.group(1))} {md.group(2)} {md.group(3)}", "%d %B %Y"
            ).strftime("%Y-%m-%d")
        except ValueError:
            pass
    track = folder_track
    if track is None:
        # 'HH_MM _ Course _ Weekday DD Month YYYY _ At The Races' — split on the
        # ' _ ' delimiter (not the bare underscore inside the time).
        parts = [p.strip() for p in re.split(r"\s+[_|]\s+", base) if p.strip()]
        if len(parts) >= 2:
            track = parts[1]
    return {"race_time": time, "track": track, "race_date": date}


# ── time helpers ──────────────────────────────────────────────────────
def to_seconds(token: str) -> Optional[float]:
    token = token.strip()
    try:
        if ":" in token:
            m, s = token.split(":")
            return int(m) * 60 + float(s)
        return float(token)
    except (ValueError, TypeError):
        return None


def _pct_to_float(token: str) -> Optional[float]:
    try:
        return float(re.sub(r"[^0-9.]", "", token))
    except ValueError:
        return None


def extract_text(pdf_path: str) -> str:
    import pdfplumber  # lazy: heavy dep, only needed for parsing
    parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            parts.append(page.extract_text() or "")
    return "\n".join(parts)


def dump_text(pdf_path: str) -> str:
    """Debug helper: return raw extracted text (for heuristic calibration)."""
    return extract_text(pdf_path)


def _race_meta(text: str, distance_unit: str) -> dict:
    dist_m = None
    mm = _DIST_M.search(text)
    mf = _DIST_F.search(text)
    if distance_unit == "m" and mm:
        dist_m = distance_to_m(mm.group(1), "m")
    elif mf:
        dist_m = distance_to_m(mf.group(1), "f")
    elif mm:
        dist_m = distance_to_m(mm.group(1), "m")
    going = None
    g = _GOING.search(text)
    if g:
        going = g.group(1).strip().rstrip(".")
    return {"distance_m": dist_m, "going": going}


def _runner_lines(text: str) -> List[RunnerSectional]:
    """Heuristic: one runner per line that carries a name + a finishing-speed %.

    Calibrate against real PDFs; falls back to empty if nothing matches.
    """
    runners: List[RunnerSectional] = []
    for line in text.splitlines():
        if not _FSP_LABEL.search(line) and "%" not in line:
            continue
        # leading position + name: '1 SOME HORSE ...'
        m = re.match(r"\s*(\d{1,2})?\s*([A-Z][A-Za-z'\- ]{2,30}?)\s{2,}", line)
        if not m:
            continue
        name = m.group(2).strip()
        if len(name) < 3:
            continue
        fsp = None
        fm = _FSP_LABEL.search(line) or re.search("(" + _PCT + ")", line)
        if fm:
            fsp = _pct_to_float(fm.group(1))
        times = [to_seconds(t) for t in re.findall(_TIME, line)]
        times = [t for t in times if t]
        stride = None
        sm = _STRIDE.search(line)
        if sm:
            try:
                stride = float(sm.group(1))
            except ValueError:
                stride = None
        runners.append(RunnerSectional(
            horse=name,
            finish_pos=int(m.group(1)) if m.group(1) else None,
            overall_time_s=max(times) if times else None,
            final_furlong_s=min(times) if times else None,
            finishing_speed_pct=fsp,
            stride_length_m=stride,
            splits=times,
        ))
    return runners


def parse_pdf(pdf_path: str, *, track: str, race_date: str,
              race_time: Optional[str] = None,
              distance_unit: str = "f") -> RaceSectionals:
    """Parse one ATR sectional PDF into a RaceSectionals record."""
    result = RaceSectionals(track=track, race_date=race_date, race_time=race_time)
    try:
        text = extract_text(pdf_path)
    except Exception as e:  # pragma: no cover - corrupt/locked PDF
        result.error = f"extract failed: {e}"
        return result
    result.raw_text = text
    meta = _race_meta(text, distance_unit)
    result.distance_m = meta["distance_m"]
    result.going = meta["going"]
    result.runners = _runner_lines(text)
    if not result.runners:
        result.error = "no runner sectionals recognised (calibrate parser vs real PDF)"
    return result


def to_store_rows(rs: RaceSectionals, source_file: str, country: Optional[str] = None) -> List[dict]:
    """Flatten a RaceSectionals into upsert dicts for store.upsert_runner."""
    import json
    band = distance_band_m(rs.distance_m)
    gg = going_group(rs.going) if rs.going else None
    rows = []
    for r in rs.runners:
        rows.append({
            "race_date": rs.race_date,
            "track": rs.track,
            "track_norm": None,           # store fills from track
            "country": country,
            "race_time": rs.race_time,
            "race_key": rs.race_key,
            "distance_m": rs.distance_m,
            "distance_band": band,
            "going": rs.going,
            "going_group": gg,
            "horse": r.horse,
            "horse_norm": normalise_name(r.horse),
            "finish_pos": r.finish_pos,
            "overall_time_s": r.overall_time_s,
            "final_furlong_s": r.final_furlong_s,
            "finishing_speed_pct": r.finishing_speed_pct,
            "stride_length_m": r.stride_length_m,
            "stride_freq": r.stride_freq,
            "splits_json": json.dumps(r.splits) if r.splits else None,
            "source_file": source_file,
        })
    return rows
