"""Assemble and send ONE combined live email: UK, Ireland and France.

The two rating engines stay separate (different scales/units); this module
reads the two ratings CSVs they already write, splits UK vs Ireland via the
pipeline's ``IRE_COURSES`` set, renders France as its own section, and sends a
single house-style email. Run at ~10pm UK on the race day.

Usage:
    python -m src.reporting.combined_live [--date YYYY-MM-DD] [--no-email]
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

from . import context as report_context
from . import emailer, render
from .models import ReportContext, TopPerformer

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent
UK_CSV = ROOT / "data" / "live" / "ratings_{date}.csv"
FR_CSV = ROOT / "data" / "france_live" / "ratings_{date}.csv"

# Distribution list (kept in sync with the UK pipeline).
try:
    from src.live_ratings import RECIPIENTS
except Exception:  # pragma: no cover - fallback if UK module not importable
    RECIPIENTS = [
        "racingsquared@gmail.com",
        "tom.biggs@blandfordbloodstock.com",
        "fred@blandfordbloodstock.com",
        "stuart@blandfordbloodstock.com",
        "richard@blandfordbloodstock.com",
    ]

# Irish course names (for the UK/IRE split). Imported from the pipeline so the
# two never drift; small hard-coded fallback keeps this importable standalone.
try:
    from src.speed_figures import IRE_COURSES
except Exception:  # pragma: no cover
    try:
        from speed_figures import IRE_COURSES
    except Exception:
        IRE_COURSES = set()


def _recipients() -> List[str]:
    """Recipient list, overridable via RATINGS_RECIPIENTS (comma-separated).

    Set RATINGS_RECIPIENTS in the workflow to restrict who receives the email
    (e.g. to racingsquared@gmail.com while verifying); unset it to use the full
    distribution list.
    """
    override = os.environ.get("RATINGS_RECIPIENTS", "").strip()
    if override:
        return [r.strip() for r in override.split(",") if r.strip()]
    return RECIPIENTS


def _read(path_tmpl: Path, date: str) -> Optional[pd.DataFrame]:
    p = Path(str(path_tmpl).format(date=date))
    if p.exists():
        try:
            df = pd.read_csv(p)
            if len(df):
                return df
        except Exception as e:  # pragma: no cover
            log.warning("Failed to read %s: %s", p, e)
    return None


def _combined_top(uk_df, fr_df, n: int = 10) -> List[TopPerformer]:
    tps: List[TopPerformer] = []
    if uk_df is not None:
        tps += report_context.top_performers(uk_df, "uk", n)
    if fr_df is not None:
        tps += report_context.top_performers(fr_df, "fr", n)
    tps.sort(key=lambda t: t.figure, reverse=True)
    tps = tps[:n]
    for i, t in enumerate(tps, 1):
        t.rank = i
    return tps


def build_combined_context(
    uk_df: Optional[pd.DataFrame],
    fr_df: Optional[pd.DataFrame],
    target_date: str,
    run_time: Optional[str] = None,
    accuracy_panel: Optional[dict] = None,
) -> ReportContext:
    sections = []
    if uk_df is not None:
        gb = uk_df[~uk_df["courseName"].isin(IRE_COURSES)]
        ire = uk_df[uk_df["courseName"].isin(IRE_COURSES)]
        if len(gb):
            sections.append(report_context.build_section(gb, "uk", title="United Kingdom"))
        if len(ire):
            sections.append(report_context.build_section(ire, "uk", title="Ireland"))
    if fr_df is not None:
        sections.append(report_context.build_section(fr_df, "fr", title="France"))

    n_runners = sum(s.runner_count for s in sections)
    n_races = sum(s.race_count for s in sections)
    countries = [s.title for s in sections]

    return ReportContext(
        title="Live Speed Ratings",
        date_str=report_context._human_date(target_date),
        subtitle_facts=[f"{n_runners} runners", f"{n_races} races",
                        " · ".join(countries) if countries else "no racing"],
        sections=sections,
        top_performers=_combined_top(uk_df, fr_df),
        accuracy_panel=accuracy_panel,
        footer_lines=[
            f"{n_runners} runners rated across {n_races} races.",
            "Figures on the Timeform scale. UK/IRE via HorseRaceBase, "
            "France via PMU. Silks © Racing Post.",
        ],
        run_time=(f"{run_time} UK" if run_time else None),
    )


def run(target_date: Optional[str] = None, send: bool = True) -> ReportContext:
    if target_date is None:
        target_date = datetime.now(timezone.utc).date().isoformat()
    run_time = datetime.now(timezone.utc).strftime("%H:%M")

    uk_df = _read(UK_CSV, target_date)
    fr_df = _read(FR_CSV, target_date)
    log.info(
        "Combined live %s: UK/IRE rows=%s, FR rows=%s",
        target_date, 0 if uk_df is None else len(uk_df), 0 if fr_df is None else len(fr_df),
    )

    # Silks by jurisdiction, each from its own feed:
    #   UK/IRE — Racing Post public racecards (rp-assets SVG). Best-effort: RP's
    #     WAF blocks datacenter IPs, so this is empty from CI unless RP_SILKS_PROXY
    #     is set. Env-gated with RP_SILKS=0.
    #   France — PMU casaques (urlCasaque), from the same first-party API the
    #     ratings use, so it works from CI with no proxy. Env-gated with FR_SILKS=0.
    # Cards render silk-optional either way.
    if uk_df is not None and os.environ.get("RP_SILKS", "1") != "0":
        try:
            from . import rp_silks
            # Prefer the morning prefetch (full, un-throttled); fall back to live.
            uk_df = rp_silks.enrich_silks(uk_df, target_date, rp_silks.silk_map_for(target_date))
        except Exception as e:  # never let silks break the email
            log.warning("RP silks enrichment skipped: %s", e)
    if fr_df is not None and os.environ.get("FR_SILKS", "1") != "0":
        try:
            from src.france import silks as fr_silks
            fr_df = fr_silks.enrich_silks(fr_df, fr_silks.fetch_silk_map(target_date))
        except Exception as e:  # never let silks break the email
            log.warning("PMU silks enrichment skipped: %s", e)

    accuracy_panel = None
    try:
        from . import accuracy as accuracy_mod
        accuracy_panel = accuracy_mod.rolling_panel()
    except Exception as e:  # accuracy panel is best-effort
        log.info("No accuracy panel: %s", e)

    ctx = build_combined_context(uk_df, fr_df, target_date, run_time, accuracy_panel)

    out_dir = ROOT / "data" / "live"
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / f"combined_{target_date}.html"
    html_path.write_text(render.render_html(ctx), encoding="utf-8")
    log.info("Saved combined HTML preview: %s", html_path)

    n = sum(s.runner_count for s in ctx.sections)
    subj_date = _subject_date(target_date)
    subject = f"Live Speed Ratings — {subj_date} ({n} runners)"

    recipients = _recipients()
    log.info("Sending combined email to %s", recipients)
    emailer.send_report(
        ctx, subject, recipients, dry_run=not send, save_html_path=str(html_path)
    )
    return ctx


def _subject_date(target_date: str) -> str:
    try:
        return datetime.strptime(target_date, "%Y-%m-%d").strftime("%a %d %b %Y")
    except ValueError:
        return target_date


def main():
    ap = argparse.ArgumentParser(description="Combined UK/IRE/FR live ratings email")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--no-email", action="store_true", help="Render only, do not send")
    args = ap.parse_args()
    run(args.date, send=not args.no_email)


if __name__ == "__main__":
    main()
