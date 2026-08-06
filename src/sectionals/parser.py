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

from .normalize import (
    distance_band_m,
    distance_to_m,
    going_group,
    normalise_name,
    normalise_track,
)

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


# ── coordinate extraction (the layout the real PDFs actually use) ─────
# The saved files are browser print-outs of the ATR race page, one per tab, so
# page.extract_text() interleaves columns into unusable soup — which is why the
# line heuristics above never matched anything.  The *Sectional Times* tab is a
# clean coordinate table though: a header row
#
#   Pos | SilkHorse | Start-5f | 5f-4f | 4f-3f | 3f-2f | 2f-1f | 1f-Finish | Finish
#
# and, per runner, a row of split times aligned under those headers with the
# position, cloth number and horse name on the following rows a couple of points
# lower.  Everything below keys off that header.
_SPLIT_HDR = re.compile(r"^(?:Start-(\d{1,2})f|(\d{1,2})f-(?:\d{1,2}f|Finish))$", re.I)
_NUM = re.compile(r"^\d{1,2}\.\d{1,2}$")
_CLOTH = re.compile(r"^(\d{1,2})\.$")

#: A split time must sit within this many points of its header column.
_COL_TOL = 6.0
#: Rows belonging to one runner sit within this many points of the split row.
_BLOCK_SPAN = 4.0


def _words(pdf_path: str) -> List[dict]:
    import pdfplumber  # lazy: heavy dep, only needed for parsing

    out: List[dict] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            out.extend(page.extract_words(extra_attrs=["size"]))
    return out


def _split_columns(words: List[dict]):
    """Return (header_top, [x0 per split column], furlongs) or None."""
    by_top = {}
    for w in words:
        by_top.setdefault(round(w["top"], 1), []).append(w)
    for top in sorted(by_top):
        row = sorted(by_top[top], key=lambda w: w["x0"])
        hdrs = [w for w in row if _SPLIT_HDR.match(w["text"])]
        if len(hdrs) < 3:
            continue
        first = _SPLIT_HDR.match(hdrs[0]["text"])
        # 'Start-5f' means five furlong markers remain, so the race is 6f.
        start = first.group(1)
        furlongs = int(start) + 1 if start else len(hdrs)
        return top, [w["x0"] for w in hdrs], furlongs
    return None


def _runner_blocks(pdf_path: str) -> List[RunnerSectional]:
    """Recover per-runner sectionals from the Sectional Times tab."""
    words = _words(pdf_path)
    cols = _split_columns(words)
    if not cols:
        return []
    hdr_top, xs, _ = cols

    by_top = {}
    for w in words:
        if w["top"] > hdr_top + 1:
            by_top.setdefault(round(w["top"], 1), []).append(w)

    runners: List[RunnerSectional] = []
    for top in sorted(by_top):
        row = sorted(by_top[top], key=lambda w: w["x0"])
        splits = []
        for x in xs:
            hit = [w for w in row
                   if _NUM.match(w["text"]) and abs(w["x0"] - x) <= _COL_TOL]
            splits.append(to_seconds(hit[0]["text"]) if hit else None)
        if sum(s is not None for s in splits) < len(xs) - 1:
            continue  # not a split row

        block = [w for t, ws in by_top.items() if 0 < t - top <= _BLOCK_SPAN
                 for w in ws]
        pos = next((int(w["text"]) for w in block
                    if w["x0"] < xs[0] - 45 and w["text"].isdigit()), None)
        name_words, cloth = [], None
        for w in sorted((w for w in block if xs[0] - 45 <= w["x0"] < xs[0] - 2),
                        key=lambda w: w["x0"]):
            m = _CLOTH.match(w["text"])
            if m and cloth is None:
                cloth = int(m.group(1))
                continue
            if w["text"].startswith("(") or not re.match(r"^[A-Za-z'()\-]", w["text"]):
                continue
            name_words.append(w["text"])
        name = " ".join(name_words).strip()
        if not name:
            continue

        known = [s for s in splits if s is not None]
        overall = sum(known) if len(known) == len(xs) else None
        final_f = splits[-1]
        # Finishing speed: how the closing furlong compares with the horse's own
        # average furlong. Above 100% = finished faster than it averaged.
        fsp = None
        if overall and final_f:
            fsp = round((overall / len(xs)) / final_f * 100, 2)
        runners.append(RunnerSectional(
            horse=name,
            finish_pos=pos,
            overall_time_s=overall,
            final_furlong_s=final_f,
            finishing_speed_pct=fsp,
            splits=known,
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

    # Coordinates first — the print-outs defeat line-based extraction.
    try:
        result.runners = _runner_blocks(pdf_path)
    except Exception as e:  # pragma: no cover - defensive
        log.warning("Coordinate parse failed for %s: %s", pdf_path, e)
        result.runners = []
    if result.runners and result.distance_m is None:
        cols = _split_columns(_words(pdf_path))
        if cols:
            result.distance_m = distance_to_m(str(cols[2]), "f")
    if not result.runners:
        result.runners = _runner_lines(text)
    if not result.runners:
        result.error = "no runner sectionals recognised"
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
            # Must be filled here: upsert_runner uses setdefault, which does
            # nothing when the key is present-but-None, and the column is NOT
            # NULL. Never surfaced before because the parser returned no rows.
            "track_norm": normalise_track(rs.track),
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
