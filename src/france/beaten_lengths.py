"""
French beaten-length parser.

PMU data provides the distance from the *previous horse* (not cumulative
from the winner).  This module parses the French text format and computes
cumulative beaten lengths for each race, mirroring the ``distanceCumulative``
column in the UK Timeform pipeline.
"""

import re
from typing import Optional

import numpy as np
import pandas as pd


# ── Pattern dispatch table ──
# Order matters: more specific patterns first.  Each entry is
# (compiled regex, numeric value in lengths).
_BL_PATTERNS: list[tuple[re.Pattern, float]] = [
    # Short head / nose variants
    (re.compile(r"^(?:Courte\s+)?Tte$|^Cte?\s*Tte$|^C\.?T\.?$", re.I), 0.1),
    (re.compile(r"^Courte\s+T[eêè]te$", re.I), 0.05),
    (re.compile(r"^Nez$", re.I), 0.03),
    # Neck
    (re.compile(r"^Enc(?:olure)?$|^Enk$|^Nk$", re.I), 0.25),
    # "Loin" = far (large margin)
    (re.compile(r"^Loin$", re.I), 30.0),
    # "Tête" / "Tte" on its own (head)
    (re.compile(r"^T[eêè]te$|^Tte$", re.I), 0.1),
]

# Numeric patterns: "3/4 L", "1 L 1/2", "2 L 3/4", "5", "1/2", etc.
_NUMERIC_RE = re.compile(
    r"^(\d+)?\s*(?:L\s*)?(\d+/\d+)?(?:\s*L)?$", re.I
)

_FRACTION_MAP = {
    "1/4": 0.25,
    "1/2": 0.5,
    "3/4": 0.75,
    "1/3": 0.33,
    "2/3": 0.67,
}


def parse_beaten_length(text: Optional[str]) -> float:
    """
    Parse a single French beaten-length string to a numeric value (lengths).

    Returns 0.0 for the winner (empty/None text).
    Returns ``np.nan`` if the text cannot be parsed.
    """
    if text is None or str(text).strip() == "":
        return 0.0

    text = str(text).strip()

    # Check named patterns first
    for pattern, value in _BL_PATTERNS:
        if pattern.match(text):
            return value

    # Try numeric pattern: "N L F" or "N" or "F" or "N L"
    m = _NUMERIC_RE.match(text)
    if m:
        whole = int(m.group(1)) if m.group(1) else 0
        frac = _FRACTION_MAP.get(m.group(2), 0.0) if m.group(2) else 0.0
        result = whole + frac
        if result > 0:
            return result

    # Last resort: try float parse
    try:
        val = float(text)
        if val >= 0:
            return val
    except (ValueError, TypeError):
        pass

    return np.nan


def compute_cumulative_bl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert per-horse beaten-length margins to cumulative beaten lengths.

    Expects the DataFrame to have columns:
      - ``race_id``: unique race identifier
      - ``positionOfficial``: finish position (1 = winner)
      - ``beaten_lengths_raw``: raw French BL text (per-horse margin)

    Returns a copy of the DataFrame with a new ``distanceCumulative`` column
    containing the cumulative beaten lengths from the winner.

    Logic mirrors how the UK pipeline uses ``distanceCumulative``:
    winner = 0, second = margin from winner, third = sum of margins, etc.
    """
    df = df.copy()

    # Parse individual margins
    df["_bl_individual"] = df["beaten_lengths_raw"].apply(parse_beaten_length)

    # Winner always has 0 cumulative BL
    df.loc[df["positionOfficial"] == 1, "_bl_individual"] = 0.0

    # Sort by race and finish position, then cumsum within each race
    df = df.sort_values(["race_id", "positionOfficial"])

    def _cumsum_race(group):
        vals = group["_bl_individual"].values.copy()
        # First runner (winner) = 0, rest are cumulative
        cum = np.zeros(len(vals))
        running = 0.0
        for i in range(len(vals)):
            if i == 0:
                cum[i] = 0.0
            else:
                margin = vals[i] if not np.isnan(vals[i]) else 0.0
                running += margin
                cum[i] = running
        return pd.Series(cum, index=group.index)

    df["distanceCumulative"] = df.groupby("race_id", group_keys=False).apply(
        _cumsum_race
    )

    df.drop(columns=["_bl_individual"], inplace=True)
    return df
