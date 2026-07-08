"""Pure parsers for Timeform results pages — no network, fully unit-testable.

These functions turn saved HTML / JSON strings into structured rows.  They are
deliberately defensive with several fallback strategies because the exact
Timeform markup is not known at authoring time (the site is unreachable from the
dev sandbox); the first CI capture seeds fixtures against which the selectors are
finalised.  Keep this module free of any Playwright / network imports so the
whole thing runs under pytest offline.
"""

from __future__ import annotations

import json
import logging
import re
from typing import List, Optional, Union
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from .models import MeetingRef, RaceResultRow, RawCapture

log = logging.getLogger(__name__)

SITE_BASE = "https://www.timeform.com"

# A Timeform time figure is an integer, optionally annotated: trailing "+"
# (ran to at least this), a small letter ("p" promising, "d" disappointing), or
# a "§"/"?" reliability mark.  We extract the leading (optionally signed) number.
_TFIG_RE = re.compile(r"^\s*([+-]?\d{1,3})\s*[+\-–pdsx?§]*\s*$", re.I)
# Header labels that denote the time-figure column.
_TFIG_HEADERS = ("tfig", "timefigure", "time figure", "tf", "timeform figure")
# Finish position like "1", "2nd", "10", or non-finishers we skip.
_POS_RE = re.compile(r"^\s*(\d{1,2})")
_NON_FINISH = {"pu", "p", "f", "ur", "bd", "su", "ro", "rr", "co", "vo", "dsq",
               "nr", "-", "—", ""}


def parse_tfig(text: Optional[str]) -> Optional[float]:
    """Extract a numeric TFig from a cell's text, or None if not a figure."""
    if text is None:
        return None
    s = str(text).strip()
    if not s or s in {"-", "—", "–", "N/A", "na"}:
        return None
    m = _TFIG_RE.match(s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    # Last resort: a bare run of digits somewhere in the cell.
    m2 = re.search(r"([+-]?\d{1,3})", s)
    return float(m2.group(1)) if m2 else None


def parse_finish_pos(text: Optional[str]) -> Optional[int]:
    """Extract a numeric finishing position, or None for non-finishers."""
    if text is None:
        return None
    s = str(text).strip().lower()
    if s in _NON_FINISH:
        return None
    m = _POS_RE.match(s)
    return int(m.group(1)) if m else None


# ─────────────────────────────────────────────────────────────────────
# Yesterday index → meeting result URLs
# ─────────────────────────────────────────────────────────────────────
def parse_yesterday_index(html: str, date: Optional[str] = None) -> List[MeetingRef]:
    """Find each meeting's result-page URL from the ``results/yesterday`` page.

    Timeform result links look like ``/horse-racing/results/<date>/<course>/...``.
    We collect the distinct meeting-level links (deepest path kept per course).
    """
    soup = BeautifulSoup(html, "lxml")
    meetings: dict[str, MeetingRef] = {}
    for a in soup.select('a[href*="/horse-racing/results/"], a[href*="/results/"]'):
        href = a.get("href") or ""
        if not href or href.endswith("/results/yesterday"):
            continue
        path = href.split("?", 1)[0].rstrip("/")
        # Expect .../results/<date>/<course>[/<race>...]; need at least a course.
        parts = [p for p in path.split("/") if p]
        if "results" not in parts:
            continue
        i = parts.index("results")
        tail = parts[i + 1:]
        if len(tail) < 2:
            continue  # only a date, no course
        course = tail[1].replace("-", " ").strip().title()
        if not course:
            continue
        url = urljoin(SITE_BASE + "/", href.lstrip("/"))
        key = course.lower()
        # Prefer the meeting root (shortest path) as the canonical result URL.
        if key not in meetings or len(url) < len(meetings[key].result_url):
            meetings[key] = MeetingRef(course=course, date=date or "", result_url=url)
    result = list(meetings.values())
    log.info("Parsed %d meetings from yesterday index", len(result))
    return result


# ─────────────────────────────────────────────────────────────────────
# Race result HTML → runner rows (with TFig)
# ─────────────────────────────────────────────────────────────────────
def _header_tfig_column(table) -> Optional[int]:
    """Index of the TFig column from the table header, or None."""
    header = table.find("thead") or table
    for tr in header.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        for idx, c in enumerate(cells):
            label = c.get_text(" ", strip=True).lower()
            if any(h == label or label.startswith(h) for h in _TFIG_HEADERS):
                return idx
    return None


def _cell_text(cells, idx: int) -> Optional[str]:
    if idx is None or idx < 0 or idx >= len(cells):
        return None
    return cells[idx].get_text(" ", strip=True)


def _horse_from_row(row_el) -> Optional[str]:
    """Best-effort horse name from a result row."""
    a = row_el.select_one('a[href*="/horse/"], a[href*="/horses/"], a.rp-horse, '
                          'a[data-horse], .horse a, td.horse a')
    if a and a.get_text(strip=True):
        return a.get_text(strip=True)
    el = row_el.select_one('.horse, [data-horse], td.rp-horse')
    if el and el.get_text(strip=True):
        # Strip a leading draw/number in parentheses if present.
        return re.sub(r"\s*\(\d+\)\s*$", "", el.get_text(" ", strip=True))
    return None


def parse_race_result_html(html: str, course_hint: Optional[str] = None) -> List[RaceResultRow]:
    """Parse runner rows (horse, finish pos, TFig) from a race-result page.

    Strategy, in order of preference:
      1. header-driven table: locate the TFig column by its header label;
      2. ``data-*`` attributes on cells (``[data-tfig]`` / ``data-column=tfig``);
      3. a cell with a tfig-ish class (``.tfig`` / ``.time-figure``).
    """
    soup = BeautifulSoup(html, "lxml")
    rows: List[RaceResultRow] = []

    course = course_hint or _page_course(soup) or ""

    for table in soup.find_all("table"):
        tfig_col = _header_tfig_column(table)
        body = table.find("tbody") or table
        trs = body.find_all("tr")
        for tr in trs:
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            horse = _horse_from_row(tr)
            if not horse:
                continue

            # (1) header-driven column
            tfig_text = _cell_text(cells, tfig_col) if tfig_col is not None else None
            # (2) data attributes
            if tfig_text is None:
                dc = tr.select_one("[data-tfig], td[data-column='tfig'], "
                                   "td[data-col='tfig'], [data-timefigure]")
                if dc is not None:
                    tfig_text = dc.get("data-tfig") or dc.get("data-timefigure") \
                        or dc.get_text(" ", strip=True)
            # (3) class-based
            if tfig_text is None:
                cc = tr.select_one("td.tfig, td.time-figure, .tfig, .time-figure, "
                                   ".rp-timefigure")
                if cc is not None:
                    tfig_text = cc.get_text(" ", strip=True)

            tfig = parse_tfig(tfig_text)
            pos_el = tr.select_one("td.position, .rp-position, [data-position], td.pos")
            pos_text = pos_el.get_text(" ", strip=True) if pos_el else (
                cells[0].get_text(" ", strip=True) if cells else None)
            rows.append(RaceResultRow(
                course=course, horse=horse, tfig=tfig,
                finish_pos=parse_finish_pos(pos_text), source="html",
            ))

    # De-dup identical (course, horse) rows, preferring one with a TFig.
    return _dedup_rows(rows)


def _page_course(soup) -> Optional[str]:
    """Best-effort meeting/course name from a result page header."""
    for sel in ("h1", ".rp-course", "[data-course]", ".meeting-title", "title"):
        el = soup.select_one(sel)
        if el:
            txt = el.get("data-course") if el.has_attr("data-course") else el.get_text(" ", strip=True)
            if txt:
                # Take the first token group before a date/separator.
                return re.split(r"\s+[-–|]\s+", txt.strip())[0].strip()
    return None


# ─────────────────────────────────────────────────────────────────────
# JSON payload (preferred if the SPA exposes one)
# ─────────────────────────────────────────────────────────────────────
_JSON_TFIG_KEYS = ("tfig", "timefigure", "timeFigure", "tf", "timeformFigure")
_JSON_HORSE_KEYS = ("horse", "horseName", "name", "runner", "runnerName")
_JSON_POS_KEYS = ("position", "finishPos", "finishingPosition", "pos", "place")
_JSON_COURSE_KEYS = ("course", "courseName", "meeting", "track", "venue")


def _first_key(d: dict, keys) -> Optional[object]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def parse_results_payload(obj: Union[dict, list, str],
                          course_hint: Optional[str] = None) -> List[RaceResultRow]:
    """Parse runner rows from a Timeform JSON results payload (best-effort).

    Recursively walks the object looking for runner-shaped dicts that carry a
    horse name and a time-figure key.
    """
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except (ValueError, TypeError):
            return []
    rows: List[RaceResultRow] = []
    _walk_json(obj, rows, course_hint or "")
    return _dedup_rows(rows)


def _walk_json(node, rows: List[RaceResultRow], course_ctx: str):
    if isinstance(node, dict):
        course = _first_key(node, _JSON_COURSE_KEYS)
        course = str(course) if course else course_ctx
        horse = _first_key(node, _JSON_HORSE_KEYS)
        tfig_val = _first_key(node, _JSON_TFIG_KEYS)
        if horse and tfig_val is not None and not isinstance(horse, (dict, list)):
            rows.append(RaceResultRow(
                course=course,
                horse=str(horse),
                tfig=parse_tfig(str(tfig_val)),
                finish_pos=parse_finish_pos(str(_first_key(node, _JSON_POS_KEYS) or "")),
                source="json",
            ))
        for v in node.values():
            _walk_json(v, rows, course)
    elif isinstance(node, list):
        for v in node:
            _walk_json(v, rows, course_ctx)


# ─────────────────────────────────────────────────────────────────────
# Assembly
# ─────────────────────────────────────────────────────────────────────
def _dedup_rows(rows: List[RaceResultRow]) -> List[RaceResultRow]:
    best: dict[tuple, RaceResultRow] = {}
    for r in rows:
        key = (r.course.strip().lower(), r.horse.strip().lower())
        cur = best.get(key)
        if cur is None or (cur.tfig is None and r.tfig is not None):
            best[key] = r
    return list(best.values())


def build_results_df(capture: RawCapture) -> pd.DataFrame:
    """Turn a RawCapture into a TFig DataFrame, preferring a JSON feed.

    JSON (if the SPA exposed one) is far more robust than HTML selectors, so we
    try it first and fall back to per-meeting HTML parsing only if JSON yielded
    nothing.
    """
    rows: List[RaceResultRow] = []
    for payload in capture.json_payloads:
        rows.extend(parse_results_payload(payload))
    if not rows:
        for course, html in capture.page_htmls.items():
            rows.extend(parse_race_result_html(html, course_hint=course))
    df = rows_to_df(_dedup_rows(rows))
    log.info("Assembled %d TFig rows (%s)", len(df),
             "json" if capture.json_payloads and len(df) else "html")
    return df


def rows_to_df(rows: List[RaceResultRow]) -> pd.DataFrame:
    """RaceResultRow list → DataFrame with stable columns."""
    return pd.DataFrame(
        [
            {
                "course": r.course,
                "horse": r.horse,
                "tfig": r.tfig,
                "finish_pos": r.finish_pos,
                "race_no": r.race_no,
                "race_time": r.race_time,
                "source": r.source,
            }
            for r in rows
        ],
        columns=["course", "horse", "tfig", "finish_pos", "race_no", "race_time", "source"],
    )
