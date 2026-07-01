"""Plaintext companion for every email (the multipart/alternative twin)."""

from __future__ import annotations

from typing import List

from .models import ReportContext

_RULE = "─" * 60  # ─ x 60


def _fig(value) -> str:
    return f"{value:.0f}" if value is not None else "-"


def render_text(ctx: ReportContext) -> str:
    lines: List[str] = []
    lines.append(ctx.title)
    facts = " · ".join([ctx.date_str, *ctx.subtitle_facts])
    lines.append(facts)
    lines.append("")

    if ctx.top_performers:
        lines.append(f"═══ TOP PERFORMERS · {len(ctx.top_performers)} ═══")
        for tp in ctx.top_performers:
            pos = f"Pos {tp.pos}" if tp.pos else "  - "
            lines.append(
                f"{tp.rank:>2}. {tp.horse:<22} {tp.course:<14} R{tp.race_number}"
                f"  {pos}  Fig {_fig(tp.figure)}"
            )
        lines.append("")

    for section in ctx.sections:
        if section.title:
            lines.append(
                f"═══ {section.title.upper()} · "
                f"{section.race_count} races ═══"
            )
            lines.append("")
        for race in section.races:
            lines.append(_RULE)
            meta = " · ".join(
                [x for x in [race.distance_str, race.going, race.surface] if x]
            )
            lines.append(f"{race.course} — Race {race.race_number}   {meta}")
            if race.video_url:
                lines.append(f"    Replay: {race.video_url}")
            for r in race.runners:
                pos = f"{r.pos}/{r.field_size}" if r.pos else "-"
                extra = ""
                if r.uplift is not None:
                    extra = f"  ({r.figure_original:.0f} {r.uplift:+.0f})"
                line = f"► {pos:<6} {r.horse:<22} Fig {_fig(r.figure)}{extra}"
                lines.append(line)
                if r.reason:
                    lines.append(f"    {r.reason}")
            lines.append("")

    if ctx.footer_lines:
        lines.append(_RULE)
        for fl in ctx.footer_lines:
            lines.append(fl)
    return "\n".join(lines)
