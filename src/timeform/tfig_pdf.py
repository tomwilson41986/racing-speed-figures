"""Parse Timeform "Race Result" PDFs for TFR / Tfig.

Why this module exists
----------------------
Scraping timeform.com from CI is a dead end (image CAPTCHA on login).  The
account owner instead saves the Timeform *Race Result* PDFs to Google Drive
every day, and those PDFs carry the two rating columns we care about — ``TFR``
(the performance rating) and ``Tfig`` (the *time figure*, which is what
``reconcile.match_rows`` consumes as ``tfig``).

Parsing approach
----------------
The PDF table is an HTML auto-layout print, so:

* ``page.extract_text()`` **interleaves** the columns (horse name and pedigree
  overlap) and is not reliably parseable.  We use
  ``page.extract_words(use_text_flow=True, extra_attrs=["fontname", "size"])``
  instead — ``use_text_flow=True`` is essential, without it pdfplumber
  re-sorts glyphs globally and shreds words into single characters because the
  interleaved columns share a near-identical ``top``.
* **x-coordinates are not stable across PDFs** (the horse-name column width is
  content driven, so everything to its right shifts by up to ~10pt).  Nothing
  is keyed off a hardcoded x.  Columns are identified by *font + size*, and the
  TFR/Tfig split is derived per document by clustering the x-centres of the
  bold rating tokens, cross-checked against the ``TFR`` header token.

Font / size taxonomy that drives the parse (all pages A4, one race per file):

===== ============================ ==========================================
size  fontname                     meaning
===== ============================ ==========================================
4.31  SourceSansPro-Bold           finishing position
3.37  SourceSansPro-SemiBold       course banner
3.00  SourceSansPro-Bold           ``N.`` cloth anchor + horse name (+ tab row)
2.81  SourceSansPro-Regular        ``(draw)``
2.62  SourceSansPro-Bold           **TFR and Tfig**
2.62  SourceSansPro-Regular        btn / jockey / trainer / SP / commentary
2.25  Regular + Italic             pedigree line
1.87  Helvetica-Bold               rating-delta glyph (excluded by fontname)
===== ============================ ==========================================

Robustness contract
-------------------
``parse_result_pdf`` never raises: a malformed / non-Timeform / unreadable PDF
is logged and yields an *empty* :class:`PdfRace` so one bad file cannot kill a
whole day's reconciliation.
"""

from __future__ import annotations

import io
import logging
import os
import re
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import pandas as pd

from . import results as results_mod
from .models import RaceResultRow

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# Font / size constants
# ─────────────────────────────────────────────────────────────────────
BOLD = "SourceSansPro-Bold"
SEMIBOLD = "SourceSansPro-SemiBold"
REGULAR = "SourceSansPro-Regular"
ITALIC = "SourceSansPro-Italic"

S_NAME = 3.00     # horse name / cloth anchor / section tabs
S_POS = 4.31      # finishing position
S_BODY = 2.62     # table body text *and* TFR/Tfig (discriminated by weight)
S_PED = 2.25      # pedigree
S_DRAW = 2.81     # (draw)
S_COURSE = 3.37   # course banner

_SIZE_TOL = 0.06

_NUMDOT_RE = re.compile(r"^\d+\.$")
# A Timeform rating cell: 1-3 digits with an optional annotation suffix.
# Only "+" occurs in the reference sample but Timeform also uses p/P/? .
_RATING_RE = re.compile(r"^(\d{1,3})([+pP?§]?)$")
_TIME_RE = re.compile(r"^\d{2}:\d{2}$")
_CARD_TIMES_RE = re.compile(r"(?:\d{2}:\d{2})+$")

MONTHS = ("January February March April May June July August September "
          "October November December").split()
_MONTH_NUM = {m: i + 1 for i, m in enumerate(MONTHS)}

# Table terminator: the tab row that follows the last runner block.
_TAB_TOKENS = ("Report", "Past", "Winners", "Replay")

# Tokens that mark the column-header row.  Header tokens merge unpredictably
# ("TFR" vs "TFRTfigJockey", "Age"/"Wgt"/"ISP" vs "AgeWgtISP"), so always match
# with startswith.  Deliberately excludes "Age"/"Wgt"/"Jockey"/"Trainer": those
# words also occur as *metadata labels* one line higher ("Age : 3yo+"), which
# would drag the header boundary up and swallow the metadata row.
_HEADER_PREFIXES = ("Horse", "Pos", "TFR", "Pedigree", "Btn", "(Draw)")


# ─────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────
@dataclass
class PdfRunner:
    """One runner row parsed out of a Timeform result PDF."""

    horse: str
    tfig: Optional[float] = None          # the *time figure* — what recon uses
    tfr: Optional[float] = None           # the performance rating
    finish_pos: Optional[int] = None
    tfr_flag: Optional[str] = None        # "+" / "p" / "?" annotation on TFR
    pos_raw: Optional[str] = None         # "1", "PU", "F" … as printed
    cloth: Optional[int] = None
    draw: Optional[int] = None
    btn: Optional[str] = None
    jockey: Optional[str] = None
    trainer: Optional[str] = None
    pedigree: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PdfRace:
    """One race (== one Timeform result PDF)."""

    course: str = ""
    race_time: Optional[str] = None       # "15:24"
    race_date: Optional[str] = None       # ISO "2026-08-03"
    race_name: Optional[str] = None
    race_class: Optional[int] = None
    race_no: Optional[int] = None         # position on the card, 1-based
    distance: Optional[str] = None
    going: Optional[str] = None
    surface: Optional[str] = None
    race_type: Optional[str] = None
    age: Optional[str] = None
    rated: Optional[str] = None
    winning_time: Optional[str] = None
    non_runners: Optional[str] = None
    all_ran: Optional[int] = None
    card_times: List[str] = field(default_factory=list)
    source_file: Optional[str] = None
    ok: bool = False                      # False => parse produced nothing usable
    error: Optional[str] = None
    runners: List[PdfRunner] = field(default_factory=list)

    # -- convenience ---------------------------------------------------
    @property
    def n_runners(self) -> int:
        return len(self.runners)

    @property
    def n_tfig(self) -> int:
        return sum(1 for r in self.runners if r.tfig is not None)

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────
# Low-level word helpers
# ─────────────────────────────────────────────────────────────────────
def _font(w: dict) -> str:
    """Font name with the PDF subset prefix (``ABCDEF+``) stripped."""
    return str(w.get("fontname", "")).split("+")[-1]


def _is_bold(w: dict) -> bool:
    # Exact match, so Helvetica-Bold (delta glyph) and SemiBold cannot leak in.
    return _font(w) == BOLD


def _is_reg(w: dict) -> bool:
    return _font(w) == REGULAR


def _sz(w: dict, size: float, tol: float = _SIZE_TOL) -> bool:
    try:
        return abs(float(w.get("size", 0.0)) - size) < tol
    except (TypeError, ValueError):
        return False


def _centre(w: dict) -> float:
    return (float(w["x0"]) + float(w["x1"])) / 2.0


def extract_words(src: "PdfSource") -> List[dict]:
    """All words of the document, sorted top-then-left, using doctop as ``top``.

    ``use_text_flow=True`` is what keeps cells intact; ``doctop`` makes the
    single ``top`` axis continuous across pages so a table spilling onto page 2
    parses identically.
    """
    import pdfplumber  # local import: keeps module importable without pdfplumber

    words: List[dict] = []
    opener = _as_pdfplumber_arg(src)
    with pdfplumber.open(opener) as pdf:
        for page in pdf.pages:
            for w in page.extract_words(use_text_flow=True,
                                        extra_attrs=["fontname", "size"]):
                w["top"] = w["doctop"]
                words.append(w)
    words.sort(key=lambda w: (round(w["top"], 1), w["x0"]))
    return words


PdfSource = Union[str, "os.PathLike[str]", bytes, bytearray, io.IOBase]


def _as_pdfplumber_arg(src: PdfSource):
    if isinstance(src, (bytes, bytearray)):
        return io.BytesIO(bytes(src))
    return src


def _source_name(src: PdfSource) -> Optional[str]:
    if isinstance(src, (str, os.PathLike)):
        return os.path.basename(os.fspath(src))
    name = getattr(src, "name", None)
    return os.path.basename(str(name)) if name else None


def _lines(words: Sequence[dict], tol: float = 1.0) -> List[List[dict]]:
    """Group words into physical lines by ``top`` proximity."""
    out: List[List[dict]] = []
    cur: List[dict] = []
    for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
        if cur and w["top"] - cur[-1]["top"] > tol:
            out.append(cur)
            cur = []
        cur.append(w)
    if cur:
        out.append(cur)
    return out


def _join(words: Sequence[dict]) -> str:
    """Join words left→right, inserting a space only for a real x-gap.

    Rendered spaces are ~0.5pt gaps; intra-word kerning is ~0.00pt, so a 0.30pt
    threshold cleanly separates the two.
    """
    ws = sorted(words, key=lambda w: w["x0"])
    parts: List[str] = []
    for i, w in enumerate(ws):
        if i and (w["x0"] - ws[i - 1]["x1"]) > 0.30:
            parts.append(" ")
        parts.append(w["text"])
    return "".join(parts).strip()


def _cluster(values: Sequence[float], gap: float = 1.0) -> List[List[float]]:
    """1-D clustering: split a sorted sequence wherever the gap exceeds *gap*."""
    out: List[List[float]] = []
    for v in sorted(values):
        if out and v - out[-1][-1] <= gap:
            out[-1].append(v)
        else:
            out.append([v])
    return out


# ─────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────
def _header_top(words: Sequence[dict]) -> float:
    """``top`` of the column-header row (the table body starts below it)."""
    cands = [w["top"] for w in words
             if w["top"] < 100
             and any(str(w["text"]).startswith(p) for p in _HEADER_PREFIXES)]
    return min(cands) if cands else 79.0


def parse_header(words: Sequence[dict]) -> Dict[str, Any]:
    """Course banner, race time/date, and the ``Label : Value`` metadata rows."""
    meta: Dict[str, Any] = {}

    # Meeting banner (SemiBold 3.37): course name + the concatenated card times.
    banner = [w for w in words if _sz(w, S_COURSE) and _font(w) == SEMIBOLD]
    if banner:
        top = min(w["top"] for w in banner)
        toks = [w for w in banner if abs(w["top"] - top) < 1.0]
        name_toks = [w["text"] for w in toks
                     if not _CARD_TIMES_RE.fullmatch(w["text"])]
        if name_toks:
            meta["course"] = " ".join(name_toks).strip().title()
        time_toks = [w["text"] for w in toks if _CARD_TIMES_RE.fullmatch(w["text"])]
        if time_toks:
            meta["card_times"] = re.findall(r"\d{2}:\d{2}", time_toks[0])

    hdr_top = _header_top(words)
    head = [w for w in words if 60 < w["top"] < hdr_top - 0.5]

    for line in _lines(head):
        toks = sorted(line, key=lambda w: w["x0"])
        texts = [w["text"] for w in toks]

        # "15:24 Monday 03 August 2026"
        if (len(texts) >= 5 and _TIME_RE.fullmatch(texts[0])
                and texts[3] in _MONTH_NUM):
            meta["race_time"] = texts[0]
            try:
                meta["race_date"] = "%04d-%02d-%02d" % (
                    int(texts[4]), _MONTH_NUM[texts[3]], int(texts[2]))
            except (ValueError, KeyError):  # pragma: no cover - defensive
                pass
            continue

        # "Label : <bold value>" pairs, plus a leading bold run = race title.
        i = 0
        label: List[dict] = []
        title: List[dict] = []
        while i < len(toks):
            w = toks[i]
            if w["text"] == ":":
                j = i + 1
                val: List[dict] = []
                while j < len(toks) and _is_bold(toks[j]):
                    val.append(toks[j])
                    j += 1
                key = " ".join(x["text"] for x in label).strip()
                if key:
                    meta[key] = _join(val)
                label = []
                i = j
                continue
            if _is_reg(w):
                label.append(w)
            elif _is_bold(w) and not label:
                title.append(w)
            i += 1

        # The title is the leading bold run of the *first* header line that has
        # one; later lines only ever contribute Label : Value pairs.
        if title and "race_name" not in meta:
            t = _join(title)
            m = re.search(r"\((\d)\)\s*$", t)   # trailing "(5)" == race class
            if m:
                meta["race_class"] = int(m.group(1))
                t = t[:m.start()].strip()
            meta["race_name"] = t

    return meta


# ─────────────────────────────────────────────────────────────────────
# Runner rows
# ─────────────────────────────────────────────────────────────────────
def _rating_band(words: Sequence[dict], name_x0: float) -> tuple:
    """(lo_x, hi_x) x-window that contains the TFR + Tfig cells.

    Derived from the header when it is legible, otherwise a permissive window
    to the right of the horse-name column.  Self-clustering (below) does the
    real work; this band only keeps far-away bold body text out.
    """
    tfr_hdr = [w for w in words if w["top"] < 100 and str(w["text"]).startswith("TFR")]
    trn_hdr = [w for w in words if w["top"] < 100 and str(w["text"]).startswith("Trainer")]
    lo = (tfr_hdr[0]["x0"] - 2.0) if tfr_hdr else (name_x0 + 40.0)
    hi = (trn_hdr[0]["x0"] - 0.5) if trn_hdr else float("inf")
    if hi <= lo:  # pragma: no cover - defensive against a garbled header
        hi = float("inf")
    return lo, hi


def _assign_ratings(runners: List[PdfRunner], raw: List[List[dict]],
                    tfr_hdr_x0: Optional[float]) -> None:
    """Decide which bold token is TFR and which is Tfig, per document.

    Two independent derivations, cross-checked:

    1. **Self-clustering** (preferred, header-free): the x-centres of every
       bold 2.62 rating token in the table fall into exactly two tight
       clusters; the left one is TFR, the right one Tfig.
    2. **Header band**: the ``TFR`` header token's ``x0`` sits 2.22pt left of
       the TFR data-column centre.

    Magnitude is deliberately *not* used — TFR < Tfig genuinely occurs.
    """
    centres = [_centre(w) for row in raw for w in row]
    clusters = _cluster(centres, gap=1.0)

    c_tfr = c_fig = None
    if len(clusters) >= 2:
        if len(clusters) > 2:  # pragma: no cover - not seen in any sample
            log.warning("Expected 2 rating columns, found %d — using the two "
                        "most populated", len(clusters))
        pair = sorted(sorted(clusters, key=len, reverse=True)[:2],
                      key=lambda c: c[0])
        c_tfr = sum(pair[0]) / len(pair[0])
        c_fig = sum(pair[1]) / len(pair[1])
        if len(pair[0]) != len(pair[1]):
            log.debug("TFR/Tfig clusters have unequal membership: %d vs %d",
                      len(pair[0]), len(pair[1]))
    elif len(clusters) == 1:
        # A whole column is absent for this race — work out which one we have
        # from the header rather than silently mapping TFR into Tfig.
        c = sum(clusters[0]) / len(clusters[0])
        if tfr_hdr_x0 is not None and abs(c - (tfr_hdr_x0 + 2.22)) >= 3.0:
            c_fig = c
        else:
            c_tfr = c
        log.warning("Only one rating column found (centre %.2f, TFR header x0 "
                    "%s) — mapping it to %s", c,
                    "?" if tfr_hdr_x0 is None else "%.2f" % tfr_hdr_x0,
                    "TFR" if c_tfr is not None else "Tfig")

    for runner, row in zip(runners, raw):
        toks = sorted(row, key=_centre)
        if len(toks) >= 2:
            _set_tfr(runner, toks[0])
            _set_tfig(runner, toks[1])
        elif len(toks) == 1:
            c = _centre(toks[0])
            d_tfr = abs(c - c_tfr) if c_tfr is not None else float("inf")
            d_fig = abs(c - c_fig) if c_fig is not None else float("inf")
            if d_fig < d_tfr:
                _set_tfig(runner, toks[0])
            else:
                _set_tfr(runner, toks[0])
        # len == 0: tailed-off runner with neither figure; both stay None.


def _set_tfr(runner: PdfRunner, w: dict) -> None:
    m = _RATING_RE.match(str(w["text"]))
    if m:
        runner.tfr = float(m.group(1))
        runner.tfr_flag = m.group(2) or None


def _set_tfig(runner: PdfRunner, w: dict) -> None:
    m = _RATING_RE.match(str(w["text"]))
    if m:
        runner.tfig = float(m.group(1))


def parse_runners(words: Sequence[dict]) -> List[PdfRunner]:
    """Split the table into per-runner blocks and pull each field out."""
    anchors = sorted(
        [w for w in words
         if _sz(w, S_NAME) and _is_bold(w) and _NUMDOT_RE.match(str(w["text"]))],
        key=lambda w: w["top"])
    if not anchors:
        return []

    name_x0 = min(a["x0"] for a in anchors)

    # The table ends at the "Report / Past Winners / Race Replay" tab row.
    ends = [w["top"] for w in words
            if _sz(w, S_NAME) and _is_bold(w) and str(w["text"]) in _TAB_TOKENS
            and w["top"] > anchors[-1]["top"]]
    tail = min(ends) if ends else float("inf")
    bounds = [a["top"] for a in anchors] + [tail]

    lo_x, hi_x = _rating_band(words, name_x0)
    tfr_hdr = [w for w in words if w["top"] < 100 and str(w["text"]).startswith("TFR")]
    tfr_hdr_x0 = tfr_hdr[0]["x0"] if tfr_hdr else None

    age_hdr = [w["x0"] for w in words
               if w["top"] < 100 and w["x0"] > 340
               and (str(w["text"]).startswith("Age")
                    or str(w["text"]).startswith("(Equip)"))]
    age_x = (min(age_hdr) - 1.0) if age_hdr else float("inf")

    runners: List[PdfRunner] = []
    raw_ratings: List[List[dict]] = []

    for i, anchor in enumerate(anchors):
        lo, hi = bounds[i] - 1.0, bounds[i + 1] - 1.0
        blk = [w for w in words if lo <= w["top"] < hi]
        a_top = anchor["top"]

        # 1. cloth number + horse name: bold 3.00 on the anchor's own line.
        #    The pedigree can never be confused with it (size 2.25, lower top).
        nm = sorted([w for w in blk if _sz(w, S_NAME) and _is_bold(w)
                     and abs(w["top"] - a_top) < 1.0],
                    key=lambda w: w["x0"])
        if not nm:
            continue
        cloth = None
        if _NUMDOT_RE.match(str(nm[0]["text"])):
            try:
                cloth = int(str(nm[0]["text"]).rstrip("."))
            except ValueError:  # pragma: no cover - defensive
                cloth = None
            nm = nm[1:]
        horse = _join(nm)
        if not horse:
            continue

        runner = PdfRunner(horse=horse, cloth=cloth)

        # 2. finishing position: the outsized (4.31) bold token, left column.
        big = [w for w in blk if float(w.get("size", 0)) > 4.0]
        if big:
            runner.pos_raw = str(big[0]["text"]).strip()
            runner.finish_pos = results_mod.parse_finish_pos(runner.pos_raw)

        # 3. TFR / Tfig candidates — resolved to columns after the loop.
        raw_ratings.append(sorted(
            [w for w in blk if _sz(w, S_BODY) and _is_bold(w)
             and lo_x <= w["x0"] <= hi_x
             and _RATING_RE.match(str(w["text"]))],
            key=_centre))

        # 4. draw
        dr = [w for w in blk if _sz(w, S_DRAW)
              and re.fullmatch(r"\(\d+\)", str(w["text"]))]
        if dr:
            runner.draw = int(str(dr[0]["text"])[1:-1])

        # 5. beaten distance: regular body text right-aligned against the name
        #    column.  The top window matters — the commentary also starts at
        #    the far-left margin and runs through the same x band.
        btn = [w for w in blk if _is_reg(w) and _sz(w, S_BODY)
               and w["x1"] <= name_x0 - 0.5 and w["top"] < a_top + 5.0]
        runner.btn = _join(btn) or None

        # 6. pedigree — group into physical lines *before* joining, else a
        #    two-line pedigree comes out scrambled.
        ped = [w for w in blk if _sz(w, S_PED) and _font(w) in (REGULAR, ITALIC)]
        if ped:
            runner.pedigree = " ".join(_join(ln) for ln in _lines(ped)).strip() or None

        # 7. jockey (offset ~0) / trainer (offset ~+5), right of the ratings.
        jk = [w for w in blk if _is_reg(w) and _sz(w, S_BODY)
              and hi_x <= w["x0"] < age_x and abs(w["top"] - a_top) < 1.5]
        runner.jockey = _join(jk) or None
        tr = [w for w in blk if _is_reg(w) and _sz(w, S_BODY)
              and hi_x <= w["x0"] < age_x
              and a_top + 3.5 < w["top"] < a_top + 7.0]
        runner.trainer = _join(tr) or None

        runners.append(runner)

    _assign_ratings(runners, raw_ratings, tfr_hdr_x0)
    return runners


# ─────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────
def parse_footer(words: Sequence[dict], after_top: float) -> Dict[str, Any]:
    foot = [w for w in words if w["top"] > after_top]
    txt = " ".join(_join(ln) for ln in _lines(foot))
    out: Dict[str, Any] = {}

    m = re.search(r"\bAll (\d+) ran\b", txt)
    out["all_ran"] = int(m.group(1)) if m else None
    m = re.search(r"Non Runners?:\s*(.*?)\s*"
                  r"(?:Race Distance Adjustment|Winning Owner|Time:|$)", txt)
    out["non_runners"] = m.group(1).rstrip(".") if m and m.group(1) else None
    m = re.search(r"Time:\s*((?:\d+m\s*)?[\d.]+s)", txt)
    out["winning_time"] = m.group(1) if m else None
    return out


# ─────────────────────────────────────────────────────────────────────
# Public entry points
# ─────────────────────────────────────────────────────────────────────
def parse_result_pdf(src: PdfSource) -> PdfRace:
    """Parse one Timeform *Race Result* PDF.

    Accepts a path, ``bytes``, or an open binary file object.  **Never raises**
    — on any failure it logs and returns a :class:`PdfRace` with ``ok=False``,
    an ``error`` string and no runners, so a single bad file cannot abort a
    day's run.
    """
    name = None
    try:
        name = _source_name(src)
    except Exception:  # pragma: no cover - paranoid
        pass

    race = PdfRace(source_file=name)
    try:
        words = extract_words(src)
    except Exception as exc:
        log.warning("Timeform PDF %s: could not read (%s: %s)",
                    name, type(exc).__name__, exc)
        race.error = f"{type(exc).__name__}: {exc}"
        return race

    if not words:
        log.warning("Timeform PDF %s: no text words extracted", name)
        race.error = "no words extracted"
        return race

    try:
        meta = parse_header(words)
        runners = parse_runners(words)

        anchor_tops = [w["top"] for w in words
                       if _sz(w, S_NAME) and _is_bold(w)
                       and _NUMDOT_RE.match(str(w["text"]))]
        meta.update(parse_footer(words, max(anchor_tops) if anchor_tops else 0.0))
    except Exception as exc:  # pragma: no cover - belt and braces
        log.warning("Timeform PDF %s: parse failed (%s: %s)",
                    name, type(exc).__name__, exc)
        race.error = f"{type(exc).__name__}: {exc}"
        return race

    race.course = str(meta.get("course") or "").strip()
    race.race_time = meta.get("race_time")
    race.race_date = meta.get("race_date")
    race.race_name = meta.get("race_name")
    race.race_class = meta.get("race_class")
    race.distance = meta.get("Distance")
    race.going = meta.get("Going")
    race.surface = meta.get("Surface")
    race.race_type = meta.get("Race Type")
    race.age = meta.get("Age")
    race.rated = meta.get("Rated")
    race.winning_time = meta.get("winning_time")
    race.non_runners = meta.get("non_runners")
    race.all_ran = meta.get("all_ran")
    race.card_times = list(meta.get("card_times") or [])
    race.runners = runners

    # Race number = index of this race's off-time within the meeting's card.
    if race.race_time and race.race_time in race.card_times:
        race.race_no = race.card_times.index(race.race_time) + 1

    if not runners:
        log.warning("Timeform PDF %s: no runner rows found "
                    "(not a Timeform result PDF?)", name)
        race.error = race.error or "no runner rows"
        return race
    if not race.course:
        # The course string only matters for disambiguating horses that ran
        # twice in a day; the matcher has a unique-horse fallback.
        log.warning("Timeform PDF %s: no course banner found", name)

    race.ok = True
    log.info("Timeform PDF %s: %s %s — %d runners, %d with Tfig",
             name, race.course or "?", race.race_time or "?",
             race.n_runners, race.n_tfig)
    return race


_APOSTROPHE_RE = re.compile("['’ʼ‘`]")


def match_name(horse: str) -> str:
    """Horse name as a *match key* for ``RatingsIndex.match``.

    Timeform prints apostrophes (``HARRY'S POP``); our ratings feed elides them
    (``Harrys Pop`` — verified: 0 of 4497 rows in ``output/uk_audit/*`` contain
    one).  ``normalise_name`` turns punctuation into a *space*, so the two
    spellings normalise to ``HARRY S POP`` vs ``HARRYS POP`` and never join.
    Deleting the apostrophe here closes that gap; it cannot break a match that
    would otherwise succeed, because the ratings side has none to lose.

    :attr:`PdfRunner.horse` keeps the verbatim Timeform spelling — this
    substitution applies only to the frame handed to ``reconcile``, whose
    output takes its ``horse`` string from the ratings row anyway.
    """
    return _APOSTROPHE_RE.sub("", horse or "").strip()


def race_to_rows(race: PdfRace) -> List[RaceResultRow]:
    """One parsed race → the ``RaceResultRow`` list the recon pipeline speaks.

    Only runners that carry a Tfig are emitted: ``reconcile`` counts every
    non-null ``tfig`` in ``n_tf``, so emitting placeholder rows for tailed-off
    runners with no figure would corrupt the match-rate signal.
    """
    rows: List[RaceResultRow] = []
    for r in race.runners:
        if r.tfig is None or not r.horse:
            continue
        rows.append(RaceResultRow(
            course=race.course,
            horse=match_name(r.horse),
            tfig=float(r.tfig),
            finish_pos=r.finish_pos,
            race_time=race.race_time,
            race_no=race.race_no,
            source="pdf",
        ))
    return rows


def races_to_df(races: Iterable[PdfRace]) -> pd.DataFrame:
    """Many parsed races → the ``tf_df`` frame ``reconcile.match_rows`` eats.

    Columns are exactly those of :func:`src.timeform.results.rows_to_df`
    (``course, horse, tfig, finish_pos, race_no, race_time, source``); ``tfig``
    is always a float (never an annotated string — ``reconcile.py`` calls a
    bare ``float()`` on it), ``course`` is the full course name from the PDF
    banner (``"Ripon"``, ``"Nottingham"``), and ``horse`` keeps its country
    suffix because the downstream matcher strips it.
    """
    rows: List[RaceResultRow] = []
    for race in races:
        rows.extend(race_to_rows(race))
    df = results_mod.rows_to_df(results_mod._dedup_rows(rows))
    # Guarantee the dtypes the contract promises even for an empty frame.
    df["tfig"] = pd.to_numeric(df["tfig"], errors="coerce").astype("float64")
    df["finish_pos"] = pd.to_numeric(df["finish_pos"], errors="coerce").astype("Int64")
    df["course"] = df["course"].astype("string").fillna("").astype(str)
    df["horse"] = df["horse"].astype("string").fillna("").astype(str)
    return df


def build_results_df_from_pdfs(sources: Iterable[PdfSource]) -> pd.DataFrame:
    """Parse a batch of Timeform result PDFs into one ``tf_df``.

    Bad files are skipped with a warning rather than aborting the batch.
    """
    races = [parse_result_pdf(s) for s in sources]
    ok = [r for r in races if r.ok]
    if len(ok) != len(races):
        log.warning("Timeform PDFs: %d of %d parsed", len(ok), len(races))
    df = races_to_df(ok)
    log.info("Timeform PDFs: %d races → %d Tfig rows", len(ok), len(df))
    return df
