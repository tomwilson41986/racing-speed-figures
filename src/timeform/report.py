"""Render the daily reconciliation report (markdown + HTML) and email it.

Reuses the existing SMTP transport in ``src.reporting.emailer.send_message`` — we
build a small standalone ``EmailMessage`` (plaintext + inline-CSS HTML) rather
than the house-style ``ReportContext`` email, since this is a compact ops report.
"""

from __future__ import annotations

import logging
import os
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import List, Optional

import pandas as pd

from .models import DayRecon

log = logging.getLogger(__name__)

DEFAULT_RECIPIENT = "racingsquared@gmail.com"


def _recipients() -> List[str]:
    override = os.environ.get("RATINGS_RECIPIENTS", "").strip()
    if override:
        return [e.strip() for e in override.split(",") if e.strip()]
    return [DEFAULT_RECIPIENT]


def _fmt(x: Optional[float], nd: int = 1, suffix: str = "") -> str:
    return "—" if x is None else f"{x:.{nd}f}{suffix}"


def _trend_table(history: pd.DataFrame, n: int = 14) -> List[dict]:
    if history is None or len(history) == 0:
        return []
    return history.sort_values("date").tail(n).to_dict("records")


def render_markdown(day: DayRecon, correction: dict,
                    history: Optional[pd.DataFrame] = None) -> str:
    lines: List[str] = []
    lines.append(f"# Timeform reconciliation — {day.date}\n")
    lines.append("How our UK/IRE speed figures compared to Timeform's TFigs for "
                 "the previous day's racing.\n")
    lines.append("## Headline\n")
    lines.append(f"- Matched runners: **{day.n_matched}** of {day.n_tf} TFig rows "
                 f"({day.n_unmatched} unmatched)")
    lines.append(f"- Correlation: **{_fmt(day.corr, 3)}**")
    lines.append(f"- Mean abs error: **{_fmt(day.mae, 1, ' lb')}**")
    lines.append(f"- Bias (ours − Timeform): **{_fmt(day.bias, 1, ' lb')}**")
    lines.append(f"- Within ±10 lb: **{_fmt(day.within_10, 0, '%')}**")
    lines.append(f"- Outliers flagged: **{day.n_outliers}**\n")

    outliers = sorted([r for r in day.rows if r.outlier],
                      key=lambda r: abs(r.diff), reverse=True)[:20]
    if outliers:
        lines.append("## Biggest discrepancies\n")
        lines.append("| Course | Horse | Pos | Ours | TFig | Diff |")
        lines.append("|---|---|---:|---:|---:|---:|")
        for r in outliers:
            pos = "" if r.finish_pos is None else int(r.finish_pos)
            lines.append(f"| {r.course} | {r.horse} | {pos} | {r.our_fig:.0f} | "
                         f"{r.tfig:.0f} | {r.diff:+.0f} |")
        lines.append("")

    lines.append("## Suggested correction\n")
    if correction.get("apply"):
        lines.append(f"- **Would apply** a bias of **{correction['bias_applied']:+.1f} lb** "
                     f"(scale {correction['scale_applied']:.3f}) — {correction['reason']}.")
    else:
        lines.append(f"- No correction applied — {correction.get('reason', 'n/a')}.")
    lines.append(f"- Rolling estimate: bias_hat {_fmt(correction.get('bias_hat'), 2, ' lb')} "
                 f"over {correction.get('days_used', 0)} days / "
                 f"{correction.get('n_matched', 0)} runners.")
    lines.append("- Auto-apply to live figures requires `TIMEFORM_CORRECTION=1` "
                 "*and* `apply=true` (off by default).\n")

    trend = _trend_table(history)
    if trend:
        lines.append("## Recent trend\n")
        lines.append("| Date | Matched | Corr | MAE | Bias | ±10 |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for row in trend:
            lines.append(
                f"| {row['date']} | {_int(row.get('n_matched'))} | "
                f"{_num(row.get('corr'), 3)} | {_num(row.get('mae'), 1)} | "
                f"{_num(row.get('bias'), 1)} | {_num(row.get('within_10'), 0)}% |")
        lines.append("")
    return "\n".join(lines)


def _int(x) -> str:
    try:
        return str(int(x))
    except (TypeError, ValueError):
        return "—"


def _num(x, nd) -> str:
    try:
        if pd.isna(x):
            return "—"
        return f"{float(x):.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def render_html(day: DayRecon, correction: dict,
                history: Optional[pd.DataFrame] = None) -> str:
    """Compact inline-CSS HTML version for the email body."""
    def stat(label, value):
        return (f'<td style="padding:6px 14px;text-align:center;">'
                f'<div style="font-size:20px;font-weight:700;color:#0b3d2e;">{value}</div>'
                f'<div style="font-size:11px;color:#666;text-transform:uppercase;'
                f'letter-spacing:.04em;">{label}</div></td>')

    head = (
        f'<div style="font-family:Arial,Helvetica,sans-serif;color:#222;max-width:720px;">'
        f'<h2 style="color:#0b3d2e;margin:0 0 4px;">Timeform reconciliation — {day.date}</h2>'
        f'<p style="color:#555;margin:0 0 12px;font-size:13px;">Our UK/IRE speed '
        f'figures vs Timeform TFigs for the previous day.</p>'
        f'<table style="border-collapse:collapse;background:#f4f7f5;border-radius:8px;'
        f'margin-bottom:16px;"><tr>'
        + stat("Matched", f"{day.n_matched}")
        + stat("Correlation", _fmt(day.corr, 3))
        + stat("MAE", _fmt(day.mae, 1))
        + stat("Bias", _fmt(day.bias, 1))
        + stat("±10 lb", _fmt(day.within_10, 0, "%"))
        + stat("Outliers", f"{day.n_outliers}")
        + '</tr></table>'
    )

    outliers = sorted([r for r in day.rows if r.outlier],
                      key=lambda r: abs(r.diff), reverse=True)[:20]
    out_html = ""
    if outliers:
        trs = "".join(
            f'<tr><td style="padding:4px 10px;border-top:1px solid #eee;">{r.course}</td>'
            f'<td style="padding:4px 10px;border-top:1px solid #eee;">{r.horse}</td>'
            f'<td style="padding:4px 10px;border-top:1px solid #eee;text-align:right;">'
            f'{"" if r.finish_pos is None else int(r.finish_pos)}</td>'
            f'<td style="padding:4px 10px;border-top:1px solid #eee;text-align:right;">{r.our_fig:.0f}</td>'
            f'<td style="padding:4px 10px;border-top:1px solid #eee;text-align:right;">{r.tfig:.0f}</td>'
            f'<td style="padding:4px 10px;border-top:1px solid #eee;text-align:right;'
            f'font-weight:700;color:{"#b00" if abs(r.diff) > 15 else "#333"};">{r.diff:+.0f}</td></tr>'
            for r in outliers)
        out_html = (
            '<h3 style="color:#0b3d2e;margin:16px 0 6px;">Biggest discrepancies</h3>'
            '<table style="border-collapse:collapse;font-size:13px;width:100%;">'
            '<tr style="text-align:left;color:#666;font-size:11px;text-transform:uppercase;">'
            '<th style="padding:4px 10px;">Course</th><th style="padding:4px 10px;">Horse</th>'
            '<th style="padding:4px 10px;text-align:right;">Pos</th>'
            '<th style="padding:4px 10px;text-align:right;">Ours</th>'
            '<th style="padding:4px 10px;text-align:right;">TFig</th>'
            '<th style="padding:4px 10px;text-align:right;">Diff</th></tr>'
            f'{trs}</table>')

    if correction.get("apply"):
        corr_html = (f'<p style="font-size:13px;"><b>Suggested correction:</b> would apply '
                     f'{correction["bias_applied"]:+.1f} lb — {correction["reason"]} '
                     f'(off unless <code>TIMEFORM_CORRECTION=1</code>).</p>')
    else:
        corr_html = (f'<p style="font-size:13px;color:#555;"><b>Suggested correction:</b> '
                     f'none — {correction.get("reason", "n/a")}. Rolling bias '
                     f'{_fmt(correction.get("bias_hat"), 2)} lb over '
                     f'{correction.get("days_used", 0)} days.</p>')

    return f'{head}{out_html}{corr_html}</div>'


def build_email(day: DayRecon, correction: dict, recipients: List[str],
                history: Optional[pd.DataFrame] = None,
                sender: Optional[str] = None) -> EmailMessage:
    sender = sender or os.environ.get("SMTP_USER") or DEFAULT_RECIPIENT
    msg = EmailMessage()
    msg["Subject"] = f"Timeform reconciliation — {day.date}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="racingspeedfigures")
    msg.set_content(render_markdown(day, correction, history))
    msg.add_alternative(render_html(day, correction, history), subtype="html")
    return msg


def send_email(day: DayRecon, correction: dict,
               history: Optional[pd.DataFrame] = None,
               dry_run: bool = False) -> bool:
    recipients = _recipients()
    msg = build_email(day, correction, recipients, history)
    if dry_run:
        log.info("Dry run: not sending recon email. To=%s", recipients)
        return True
    from src.reporting import emailer  # lazy: keeps the report/CLI import light
    return emailer.send_message(msg, recipients)
