"""House-style design tokens for every email this project sends.

Single source of truth for colours, typography and the semantic swatch
families defined in the Email Style Guide. All values are plain data so they
can be injected into Jinja templates (see ``render.py``) and reused by the
plaintext renderer.

The guide's rating tiles are scored 0-100; our speed figures sit on the
Timeform ~40-130 scale, so ``figure_swatch`` re-maps the band thresholds to
the figure range. Retune the thresholds here in one place.
"""

from __future__ import annotations

from typing import Optional

# ── Page & surface ────────────────────────────────────────────────────
PAGE_BG = "#f4f4f1"        # warm paper grey, on <body>
CARD_BG = "#ffffff"        # every card / table
CARD_BORDER = "#e3e3de"    # 1px hairline around cards
ROW_DIVIDER = "#efefe9"    # border-top between table rows

# ── Text hierarchy (warm-neutral greys) ───────────────────────────────
INK = "#1a1a1a"            # headings, horse names, key values
BODY_DARK = "#444444"
BODY = "#555555"
MUTED = "#666666"          # section headers, subtitles
LABEL = "#888888"          # eyebrow labels, <th>, meta
FAINT = "#999999"          # small-sample rows, tertiary meta
FOOTNOTE = "#aaaaaa"       # closing source/footer note
HAIRLINE_TEXT = "#bbbbbb"  # separators

# ── Accent ────────────────────────────────────────────────────────────
LINK = "#1a5fb4"           # all hyperlinks; no underline, weight 600

# ── Typography ────────────────────────────────────────────────────────
FONT_STACK = "-apple-system,Segoe UI,Helvetica,Arial,sans-serif"

# ── Silk display (see silks.py for the render pipeline) ───────────────
SILK_RENDER_W = 80         # render at 2x for retina
SILK_RENDER_H = 57
SILK_DISPLAY_W = 56
SILK_DISPLAY_H = 40
SILK_RADIUS = 4

# ── Semantic swatch families: (background, foreground, label) ─────────
SWATCH_STRONG = {"bg": "#cce5cc", "fg": "#1f4a1f", "label": "#3c6c3c"}   # green
SWATCH_HIGH = {"bg": "#e3f0e3", "fg": "#2d5b2d", "label": "#557a55"}     # mid-green
SWATCH_MID = {"bg": "#f5efde", "fg": "#6a5a30", "label": "#8a7a4f"}      # amber
SWATCH_LOW = {"bg": "#ececec", "fg": "#555555", "label": "#888888"}      # neutral
SWATCH_EMPTY = {"bg": "#f0f0ec", "fg": "#666666", "label": "#888888"}    # paper
SWATCH_SLATE = {"bg": "#e2e6ea", "fg": "#3d4750", "label": "#3d4750"}    # slate

# Podium accents (finishing position only)
PODIUM = {
    1: {"bg": "#fce99c", "fg": "#7a5a00"},
    2: {"bg": "#e2e6ea", "fg": "#3d4750"},
    3: {"bg": "#f0d9bf", "fg": "#7a4a1a"},
}
PODIUM_OTHER = {"bg": "#ececec", "fg": "#444444"}


# ── Figure -> swatch (Timeform ~40-130 scale) ─────────────────────────
# Guide tiles use >=90 / >=70 / >=50 on a 0-100 scale. Speed figures spread
# wider, so anchor the bands to meaningful class levels:
#   >=115 top-class (Group), >=100 useful/handicap-good, >=85 fair, else modest.
FIG_BAND_STRONG = 115.0
FIG_BAND_HIGH = 100.0
FIG_BAND_MID = 85.0


def figure_swatch(fig: Optional[float]) -> dict:
    """Swatch family for a speed figure value."""
    if fig is None:
        return SWATCH_EMPTY
    try:
        f = float(fig)
    except (TypeError, ValueError):
        return SWATCH_EMPTY
    if f >= FIG_BAND_STRONG:
        return SWATCH_STRONG
    if f >= FIG_BAND_HIGH:
        return SWATCH_HIGH
    if f >= FIG_BAND_MID:
        return SWATCH_MID
    return SWATCH_LOW


def pos_swatch(pos: Optional[int]) -> dict:
    """Podium swatch for a finishing position (gold/silver/bronze/other)."""
    if pos is None:
        return PODIUM_OTHER
    try:
        p = int(pos)
    except (TypeError, ValueError):
        return PODIUM_OTHER
    return PODIUM.get(p, PODIUM_OTHER)


# ── Uplift / downgrade (day-after sectional report) ───────────────────
def uplift_swatch(uplift: Optional[float]) -> dict:
    """Green for a meaningful upgrade, amber for a downgrade, neutral else."""
    if uplift is None:
        return SWATCH_EMPTY
    try:
        u = float(uplift)
    except (TypeError, ValueError):
        return SWATCH_EMPTY
    if u >= 3.0:
        return SWATCH_STRONG
    if u >= 1.0:
        return SWATCH_HIGH
    if u <= -1.0:
        return SWATCH_MID
    return SWATCH_LOW


def status_swatch(kind: Optional[str]) -> dict:
    """won -> green, placed -> amber, ran -> neutral, else paper."""
    k = (kind or "").strip().lower()
    if k == "won":
        return SWATCH_STRONG
    if k == "placed":
        return SWATCH_MID
    if k == "ran":
        return SWATCH_LOW
    return SWATCH_EMPTY


def as_dict() -> dict:
    """All tokens as a dict, for injection into the Jinja environment."""
    return {
        "PAGE_BG": PAGE_BG,
        "CARD_BG": CARD_BG,
        "CARD_BORDER": CARD_BORDER,
        "ROW_DIVIDER": ROW_DIVIDER,
        "INK": INK,
        "BODY_DARK": BODY_DARK,
        "BODY": BODY,
        "MUTED": MUTED,
        "LABEL": LABEL,
        "FAINT": FAINT,
        "FOOTNOTE": FOOTNOTE,
        "HAIRLINE_TEXT": HAIRLINE_TEXT,
        "LINK": LINK,
        "FONT_STACK": FONT_STACK,
        "SILK_DISPLAY_W": SILK_DISPLAY_W,
        "SILK_DISPLAY_H": SILK_DISPLAY_H,
        "SILK_RADIUS": SILK_RADIUS,
    }
