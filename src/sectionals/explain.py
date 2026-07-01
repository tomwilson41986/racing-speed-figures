"""Deterministic natural-language reasons for each sectional adjustment.

Reproducible and auditable (no model call in the CI path); the same numbers
always produce the same sentence. An optional LLM narration hook can be added
later behind a flag.
"""

from __future__ import annotations

from typing import Optional


def _n_phrase(n: Optional[int]) -> str:
    return f"n={n}" if n else "n=?"


def explain(uplift: Optional[float], meta: dict,
            final_furlong_s: Optional[float] = None) -> Optional[str]:
    """One-sentence reason for the uplift/downgrade, or None when no verdict."""
    if uplift is None:
        return None
    fsp = meta.get("fsp")
    par = meta.get("fsp_par")
    n = meta.get("n")
    if fsp is None or par is None:
        return None

    core = f"finishing speed {fsp:.1f}% vs {par:.1f}% par ({_n_phrase(n)})"
    ff = f", closed final furlong in {final_furlong_s:.2f}s" if final_furlong_s else ""

    if uplift >= 3.0:
        verdict = "kept galloping strongly — value above the bare result"
    elif uplift >= 1.0:
        verdict = "finished a shade better than par"
    elif uplift <= -3.0:
        verdict = "faded well below par — likely flattered by the run of the race"
    elif uplift <= -1.0:
        verdict = "finished a touch below par"
    else:
        verdict = "in line with sectional par"

    sign = f"{uplift:+.0f} lb" if abs(uplift) >= 1.0 else "no change"
    prov = " (provisional par)" if meta.get("tier") == "provisional" else ""
    return f"{core}{ff} — {verdict}; {sign}{prov}."
