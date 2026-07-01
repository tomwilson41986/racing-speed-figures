"""Assemble and send the day-after SECTIONAL email.

Joins the day's parsed sectionals to the original live ratings, applies the
uplift/downgrade, explains each one, headlines the eye-catchers, links the At
The Races replay, and appends the updated sectional/stride pars (with counts).
Renders through the shared house-style engine (src/reporting/).
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.reporting import emailer, render
from src.reporting.models import Race, ReportContext, Runner, Section, TopPerformer

from . import matching, pars as pars_mod, store, uplift as uplift_mod, video
from .explain import explain

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent

try:
    from src.live_ratings import RECIPIENTS
except Exception:  # pragma: no cover
    RECIPIENTS = ["racingsquared@gmail.com"]


def _distance_str(distance, unit) -> str:
    try:
        d = float(distance)
    except (TypeError, ValueError):
        return "?"
    return f"{int(d)}m" if unit == "m" else f"{d:g}f"


def _human_date(date: str) -> str:
    try:
        return datetime.strptime(date, "%Y-%m-%d").strftime("%A %d %B %Y")
    except ValueError:
        return date


def build_day_after_context(date: str, session=None) -> ReportContext:
    session = session or store.get_session()
    index = matching.load_ratings_index(date)
    pars = pars_mod.load_latest()
    runners = store.runners_for_date(session, date)

    # race_key -> {meta, runners:[]}
    races = defaultdict(lambda: {"meta": None, "runners": []})
    eyecatchers = []
    matched = 0
    unmatched = 0
    today_tracks = set()

    for sr in runners:
        today_tracks.add(sr.track_norm)
        rating = matching.match(index, sr.track, sr.horse)
        if not rating:
            unmatched += 1          # sectional with no rating to adjust — skip
            continue
        par = pars_mod.par_lookup(pars, sr.track_norm, sr.distance_band, sr.going_group) if pars is not None else None
        up, meta = uplift_mod.compute_uplift(sr.finishing_speed_pct, par)
        reason = explain(up, meta, sr.final_furlong_s)

        matched += 1
        original = rating.get("figure")
        adjusted = uplift_mod.apply(original, up)
        country = rating["country"]
        unit = rating["unit"]
        course = rating["course"]
        race_no = rating.get("race_number") or 0
        dist = _distance_str(rating.get("distance"), unit)
        going = rating.get("going")
        surface = rating.get("surface")
        rclass = rating.get("race_class")

        key = (country, course, race_no)
        r = races[key]
        if r["meta"] is None:
            r["meta"] = {
                "country": country, "course": course, "race_no": race_no,
                "dist": dist, "going": going, "surface": surface, "rclass": rclass,
                "video": video.race_replay_url(sr.track, date, sr.race_time),
            }
        r["runners"].append(Runner(
            horse=sr.horse,
            pos=sr.finish_pos,
            figure=adjusted if adjusted is not None else original,
            figure_original=original,
            uplift=up,
            reason=reason,
        ))
        if up is not None and uplift_mod.is_interesting(up) and adjusted is not None:
            eyecatchers.append((up, TopPerformer(
                rank=0, horse=sr.horse, course=course, race_number=race_no,
                figure=adjusted, pos=sr.finish_pos, note=reason,
            )))

    # sections grouped by country
    by_country = defaultdict(list)
    for (country, course, race_no), r in races.items():
        m = r["meta"]
        race = Race(
            course=course, race_number=race_no, distance_str=m["dist"],
            going=m["going"], surface=m["surface"], race_class=m["rclass"],
            video_url=m["video"],
            runners=sorted(r["runners"], key=lambda x: (x.pos is None, x.pos or 999)),
        )
        by_country[country].append(race)

    order = ["GB/IRE", "FR"]
    titles = {"GB/IRE": "UK & Ireland", "FR": "France"}
    sections = []
    for country in order + [c for c in by_country if c not in order]:
        if country in by_country:
            races_sorted = sorted(by_country[country], key=lambda r: (r.course, r.race_number))
            sections.append(Section(title=titles.get(country, country), races=races_sorted))

    # eye-catchers board (biggest absolute moves first)
    eyecatchers.sort(key=lambda t: abs(t[0]), reverse=True)
    top = [tp for _, tp in eyecatchers[:10]]
    for i, tp in enumerate(top, 1):
        tp.rank = i

    par_tables = _build_par_tables(pars, today_tracks)

    n_up = sum(1 for _, tp in eyecatchers if _ > 0)
    n_down = sum(1 for _, tp in eyecatchers if _ < 0)
    return ReportContext(
        title="Sectional Upgrades & Downgrades",
        date_str=_human_date(date),
        subtitle_facts=[f"{matched} runners matched", f"{n_up} upgrades", f"{n_down} downgrades"],
        sections=sections,
        top_performers=top,
        par_tables=par_tables,
        footer_lines=[
            f"{matched} runners matched to ratings; {unmatched} sectionals had "
            "no matching rating.",
            "Sectional adjustments from finishing-speed vs par (per track, "
            "distance & going); capped and tier-gated by data count.",
            "Sectionals from At The Races. Figures on the Timeform scale.",
        ],
    )


def _build_par_tables(pars, today_tracks) -> list:
    if pars is None or pars.empty:
        return []
    df = pars[pars["track_norm"].isin(today_tracks)] if today_tracks else pars
    if df.empty:
        df = pars
    headers = ["Track · distance · going", "Fin.speed par", "Stride par", "n"]
    rows = []
    for _, r in df.sort_values(["track", "distance_band", "going_group"]).iterrows():
        fsp = f"{r['fsp_par']:.1f}%" if pd.notna(r.get("fsp_par")) else "—"
        stride = f"{r['stride_par']:.2f}m" if pd.notna(r.get("stride_par")) else "—"
        label = f"{r['track']} · {r['band_label']} · {r['going_group']}"
        rows.append({
            "cells": [label, fsp, stride, f"n={int(r['n_fsp'])}"],
            "faint": r.get("tier") != "production",
        })
    return [{
        "title": "Sectional & stride pars (updated — small samples italicised)",
        "headers": headers,
        "rows": rows,
    }]


def run(date: str, send: bool = True) -> ReportContext:
    ctx = build_day_after_context(date)
    out_dir = ROOT / "output" / "sectionals"
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / f"sectional_report_{date}.html"
    html_path.write_text(render.render_html(ctx), encoding="utf-8")
    log.info("Saved sectional report HTML: %s", html_path)

    subj_date = datetime.strptime(date, "%Y-%m-%d").strftime("%a %d %b %Y") if _isdate(date) else date
    n = sum(s.runner_count for s in ctx.sections)
    subject = f"Sectional Upgrades & Downgrades — {subj_date} ({n} runners)"
    emailer.send_report(ctx, subject, RECIPIENTS, dry_run=not send, save_html_path=str(html_path))
    return ctx


def _isdate(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def main():
    ap = argparse.ArgumentParser(description="Day-after sectional upgrades/downgrades email")
    ap.add_argument("--date", required=True, help="Race date YYYY-MM-DD (yesterday)")
    ap.add_argument("--no-email", action="store_true")
    args = ap.parse_args()
    run(args.date, send=not args.no_email)


if __name__ == "__main__":
    main()
