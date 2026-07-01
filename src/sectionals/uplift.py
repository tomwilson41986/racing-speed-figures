"""Sectional uplift / downgrade of the original live rating.

A horse that finishes markedly faster than the sectional par for the track,
distance and going kept galloping (often off a slow pace) and is worth more than
the bare result; one that finished slower than par was likely flattered. We turn
the finishing-speed deviation from par into a pounds adjustment, capped and
tier-gated by how much data backs the par.
"""

from __future__ import annotations

from typing import Optional, Tuple

# Sensitivity: ~0.75 lb per 1 percentage-point of finishing-speed deviation.
K_LB_PER_PCT = 0.75
CAP_PRODUCTION = 6.0
CAP_PROVISIONAL = 3.0      # softer cap when the par is only provisional
NEUTRAL_BAND = 1.0         # |uplift| below this is treated as "in line with par"


def compute_uplift(finishing_speed_pct: Optional[float],
                   par: Optional[dict]) -> Tuple[Optional[float], dict]:
    """Return (uplift_lbs, meta). None uplift means 'no verdict' (no data)."""
    if finishing_speed_pct is None or not par or par.get("fsp_par") is None:
        return None, {}
    tier = par.get("tier", "insufficient")
    if tier == "insufficient":
        return None, {"tier": tier, "reason": "insufficient par sample"}
    cap = CAP_PROVISIONAL if tier == "provisional" else CAP_PRODUCTION
    dev = finishing_speed_pct - par["fsp_par"]          # percentage points
    raw = K_LB_PER_PCT * dev
    uplift = max(-cap, min(cap, raw))
    meta = {
        "dev_pct": dev,
        "cap": cap,
        "tier": tier,
        "n": par.get("n_fsp"),
        "fsp": finishing_speed_pct,
        "fsp_par": par["fsp_par"],
    }
    return round(uplift, 1), meta


def is_interesting(uplift: Optional[float]) -> bool:
    """Flag eye-catchers (big upgrades) and clearly flattered horses."""
    return uplift is not None and abs(uplift) >= 3.0


def apply(original_figure: Optional[float], uplift: Optional[float]) -> Optional[float]:
    if original_figure is None:
        return None
    if uplift is None or abs(uplift) < NEUTRAL_BAND:
        return original_figure
    return round(original_figure + uplift, 1)
