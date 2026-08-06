"""Google-Drive-backed source of Timeform TFigs.

Replaces the (dead) Playwright scrape of timeform.com.  The site's sign-in form
is behind an image CAPTCHA, so an automated login can never complete from CI —
but the account owner already saves Timeform *Race Result* PDFs to the same
Google Drive folder the sectionals pipeline reads, and those PDFs carry the TFR
and Tfig columns the reconciliation needs.

Drive layout (identical to sectionals)::

    <root>/<DD.MM.YY>/<Track>/<files>

Three unrelated document families share each track folder; **only the first is
Timeform**:

===  ==================================================  ==================
 #   filename shape                                       source
===  ==================================================  ==================
 1   ``Race Result HH_MM COURSE Weekday DD Month.pdf``     Timeform  ← wanted
 2   ``<Course> Racing Results _ <date> HH_MM.pdf``        Racing TV
 3   ``HH_MM _ <Course> _ <date> _ At The Races*.pdf``     At The Races
===  ==================================================  ==================

Everything about authentication, root-folder resolution and ``DD.MM.YY``
matching is reused verbatim from :mod:`src.sectionals.drive_client` — this
module adds only the family filter (which ``drive_client.iter_race_pdfs``
deliberately lacks: it yields *every* PDF under the date folder) and the
PDF → ``tf_df`` assembly.

Public entry point::

    tf_df = build_results_df_from_drive("2026-08-03")

which returns exactly the frame ``reconcile.match_rows`` expects.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from ..sectionals.drive_client import (
    PDF_MIME,
    SECTIONALS_ROOT_ID,
    date_folder_name,
    find_date_folder,
    get_reader,
)
from . import tfig_pdf
from .tfig_pdf import PdfRace

log = logging.getLogger(__name__)

__all__ = [
    "TimeformPdf",
    "get_reader",          # re-exported from sectionals.drive_client
    "SECTIONALS_ROOT_ID",
    "is_timeform_result_pdf",
    "parse_timeform_filename",
    "iter_timeform_pdfs",
    "fetch_pdf_bytes",
    "fetch_races",
    "build_results_df_from_drive",
]


# ─────────────────────────────────────────────────────────────────────
# Filename filter
# ─────────────────────────────────────────────────────────────────────
# Timeform: "Race Result 15_24 RIPON Monday 3 August.pdf".  The time separator
# varies with whatever sanitising the download path applied ('_', '.', '-',
# ':'), and Drive/Chrome duplicate-suffix variants append junk before the
# extension ("… August2.pdf", "… August (1).pdf", "… August-3.pdf").
_MONTHS = (
    "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    "aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
)
_WEEKDAYS = (
    "mon(?:day)?|tue(?:s(?:day)?)?|wed(?:nes(?:day)?)?|thu(?:r(?:s(?:day)?)?)?|"
    "fri(?:day)?|sat(?:ur(?:day)?)?|sun(?:day)?"
)

#: Strict shape — also yields the off-time and the course token.
_TF_STRICT_RE = re.compile(
    r"^\s*race\s*results?\s+"                      # 'Race Result'
    r"(\d{1,2})\s*[._:\-]\s*(\d{2})\s+"            # HH_MM
    r"(.+?)\s+"                                    # COURSE (non-greedy)
    rf"(?:{_WEEKDAYS})\s+"                         # Weekday
    r"(\d{1,2})(?:st|nd|rd|th)?\s+"                # DD
    rf"({_MONTHS})(?![a-z])"                       # Month ('August2' → 'August')
    r"[\s\d()\[\]_.\-]*$",                         # duplicate-suffix junk
    re.IGNORECASE,
)

#: Lenient shape — a Timeform result PDF whose tail we do not recognise.  Still
#: rejects both other families, which never begin with 'Race Result'.
_TF_LOOSE_RE = re.compile(r"^\s*race\s*results?\b", re.IGNORECASE)

#: Time anywhere in the stem, for the lenient path.
_TIME_RE = re.compile(r"\b(\d{1,2})\s*[._:\-]\s*(\d{2})\b")

#: Belt-and-braces negative guard.  Neither family starts with 'Race Result'
#: (Racing TV leads with the course, At The Races with the off-time), so this
#: only fires if one is ever renamed — but the requirement is to exclude them
#: *explicitly*, and an explicit rule is testable.
_EXCLUDE_RE = re.compile(r"racing\s*results?|at\s*the\s*races|racingtv|attheraces",
                         re.IGNORECASE)


#: A trailing extension that is *not* .pdf disqualifies the file. Names with no
#: extension at all are kept — Drive files can carry the PDF mime type without
#: one, and ``iter_timeform_pdfs`` gates on mime type as well.
_NON_PDF_EXT_RE = re.compile(r"\.(?!pdf$)[a-z0-9]{1,5}$", re.IGNORECASE)


def _stem(name: str) -> Optional[str]:
    """Filename minus a trailing '.pdf', or ``None`` if it is not a PDF."""
    s = (name or "").strip()
    if s.lower().endswith(".pdf"):
        return s[:-4].strip()
    return None if _NON_PDF_EXT_RE.search(s) else s


def parse_timeform_filename(name: str) -> Optional[Dict[str, Any]]:
    """Parse a Timeform *Race Result* filename, or ``None`` if it is not one.

    Returns ``{"race_time": "15:24", "course": "RIPON", "day": 3,
    "month": "August", "strict": True}``.  On the lenient path (leading
    ``Race Result`` but an unrecognised tail) ``course``/``day``/``month`` may
    be ``None`` and ``strict`` is ``False``; the course is then recovered from
    the PDF's own banner, which is authoritative anyway.
    """
    stem = _stem(name)
    if not stem:
        return None
    if _EXCLUDE_RE.search(stem):
        return None

    m = _TF_STRICT_RE.match(stem)
    if m:
        hh, mm, course, day, month = m.groups()
        return {
            "race_time": f"{int(hh):02d}:{mm}",
            "course": course.strip(" _-"),
            "day": int(day),
            "month": month,
            "strict": True,
        }

    if not _TF_LOOSE_RE.match(stem):
        return None
    t = _TIME_RE.search(stem)
    return {
        "race_time": f"{int(t.group(1)):02d}:{t.group(2)}" if t else None,
        "course": None,
        "day": None,
        "month": None,
        "strict": False,
    }


def is_timeform_result_pdf(name: str) -> bool:
    """True when ``name`` belongs to the Timeform *Race Result* family."""
    return parse_timeform_filename(name) is not None


# ─────────────────────────────────────────────────────────────────────
# Drive walk
# ─────────────────────────────────────────────────────────────────────
@dataclass
class TimeformPdf:
    """One selected Drive file, with what the filename told us about it."""

    file_id: str
    name: str
    course: Optional[str] = None        # from the filename, may be None
    folder_track: Optional[str] = None  # <Track> folder it sits in
    race_time: Optional[str] = None
    relpath: Optional[str] = None
    meta: Optional[dict] = None

    @property
    def course_hint(self) -> Optional[str]:
        """Best course guess when the PDF banner is missing."""
        return self.course or self.folder_track


def iter_timeform_pdfs(reader, date: str,
                       root_id: str = SECTIONALS_ROOT_ID) -> Iterator[TimeformPdf]:
    """Yield the Timeform *Race Result* PDFs under ``<root>/<DD.MM.YY>/``.

    Mirrors ``drive_client.iter_race_pdfs`` but adds the family filter, and
    takes the track from the containing folder rather than falling back to the
    *date* folder's name (which that function does, silently returning
    ``'3.08.26'`` as the track for a PDF sitting loose in the date folder).
    """
    day = find_date_folder(reader, date, root_id)
    if not day:
        log.warning("Timeform/Drive: no folder for %s (%s) under %s",
                    date, date_folder_name(date), root_id)
        return

    n_pdf = n_hit = 0
    for meta in reader.list_folder(day["id"], recursive=True):
        name = meta.get("name", "") or ""
        if not (meta.get("mimeType") == PDF_MIME or name.lower().endswith(".pdf")):
            continue
        n_pdf += 1
        parsed = parse_timeform_filename(name)
        if parsed is None:
            log.debug("Timeform/Drive: skipping non-Timeform PDF %r", name)
            continue
        n_hit += 1
        rel = meta.get("relpath", name)
        yield TimeformPdf(
            file_id=meta["id"],
            name=name,
            course=parsed["course"],
            folder_track=rel.split("/")[0] if "/" in rel else None,
            race_time=parsed["race_time"],
            relpath=rel,
            meta=meta,
        )
    log.info("Timeform/Drive %s: %d Timeform PDFs of %d PDFs in %s",
             date, n_hit, n_pdf, day.get("name"))


def fetch_pdf_bytes(reader, item: TimeformPdf) -> bytes:
    """Download one PDF's bytes (no disk round-trip: pdfplumber reads BytesIO)."""
    return reader.download_file(item.file_id)


# ─────────────────────────────────────────────────────────────────────
# Assembly
# ─────────────────────────────────────────────────────────────────────
def fetch_races(date: str, reader=None,
                root_id: str = SECTIONALS_ROOT_ID) -> List[PdfRace]:
    """Download and parse every Timeform result PDF for ``date``.

    Best-effort per file: a download error or an unparseable PDF is logged and
    skipped, so one bad file cannot abort the day.
    """
    if reader is None:
        reader = get_reader()

    items = list(iter_timeform_pdfs(reader, date, root_id))
    races: List[PdfRace] = []
    n_fail = 0
    for item in items:
        try:
            raw = fetch_pdf_bytes(reader, item)
        except Exception as exc:  # network / permissions / quota
            n_fail += 1
            log.warning("Timeform/Drive: download failed for %s (%s: %s)",
                        item.name, type(exc).__name__, exc)
            continue
        # parse_result_pdf never raises: it returns ok=False on any failure.
        race = tfig_pdf.parse_result_pdf(raw)
        race.source_file = item.name
        if not race.ok:
            n_fail += 1
            log.warning("Timeform/Drive: unparseable %s (%s)", item.name, race.error)
            continue
        # The PDF banner is authoritative; fall back to the filename/folder.
        if not race.course and item.course_hint:
            race.course = item.course_hint
        if not race.race_time and item.race_time:
            race.race_time = item.race_time
        races.append(race)

    n_runners = sum(len(r.runners) for r in races)
    n_tfig = sum(1 for r in races for x in r.runners if x.tfig is not None)
    log.info("Timeform/Drive %s: %d files found, %d parsed, %d failed, "
             "%d runners, %d with Tfig",
             date, len(items), len(races), n_fail, n_runners, n_tfig)
    # Surfaced to the caller (see build_results_df_from_drive) so a PARTIAL day
    # cannot masquerade as a clean one. Dropping races silently would still give
    # a 100% match rate on the survivors while feeding a skewed MAE/bias into
    # the rolling correction.
    fetch_races.last_stats = {
        "n_found": len(items), "n_parsed": len(races), "n_failed": n_fail,
        "n_runners": n_runners, "n_tfig": n_tfig,
    }
    return races


def build_results_df_from_drive(date: str, reader=None,
                                root_id: str = SECTIONALS_ROOT_ID) -> pd.DataFrame:
    """``date`` → the ``tf_df`` frame ``reconcile.match_rows`` expects.

    Columns ``course, horse, tfig, finish_pos, race_no, race_time, source``
    with ``tfig`` a float (never an annotated string — ``reconcile.py:74``
    calls a bare ``float()`` on it) and ``course`` the full course name from
    the PDF banner.  Returns a correctly shaped empty frame when the day's
    Drive folder is missing or holds no Timeform PDFs.
    """
    fetch_races.last_stats = {}
    df = tfig_pdf.races_to_df(fetch_races(date, reader=reader, root_id=root_id))
    # Carry the fetch stats with the frame so the CLI can refuse to publish a
    # silently-partial day (``df.attrs`` survives the frame construction here).
    df.attrs["fetch_stats"] = dict(getattr(fetch_races, "last_stats", {}) or {})
    log.info("Timeform/Drive %s: %d Tfig rows", date, len(df))
    return df
