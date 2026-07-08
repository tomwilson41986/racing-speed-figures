"""CLI for the Timeform TFig reconciliation.

Usage:
    python -m src.timeform.cli capture-html --date yesterday   # login + dump raw only
    python -m src.timeform.cli run          --date yesterday   # fetch + reconcile + email
    python -m src.timeform.cli reconcile    --date 2026-07-07 --from-raw   # reprocess a capture
    python -m src.timeform.cli report       --date 2026-07-07  # re-render report from stored csv
"""

from __future__ import annotations

import datetime
import logging
import sys

import click

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # pragma: no cover
    pass

from . import report as report_mod
from . import results as results_mod
from . import store
from .correction import compute_correction
from .reconcile import reconcile


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _resolve_date(value: str) -> str:
    v = (value or "yesterday").strip().lower()
    today = datetime.datetime.now(datetime.timezone.utc).date()
    if v in ("yesterday", "y"):
        return (today - datetime.timedelta(days=1)).isoformat()
    if v in ("today", "t"):
        return today.isoformat()
    # Validate an explicit YYYY-MM-DD.
    datetime.date.fromisoformat(v)
    return v


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable DEBUG logging.")
@click.pass_context
def cli(ctx, verbose):
    """Timeform TFig reconciliation."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)


def _fetch_capture(date: str, headless: bool):
    from .client import TimeformClient  # local import: network layer, CI only
    client = TimeformClient(headless=headless)
    click.echo("Logging in to Timeform...")
    if not client.login():
        click.echo("Login failed. Check TIMEFORM_USER / TIMEFORM_PASS.", err=True)
        sys.exit(1)
    click.echo("Login OK. Capturing yesterday's results...")
    try:
        return client.capture_yesterday(date)
    finally:
        client.close()


@cli.command("capture-html")
@click.option("--date", default="yesterday", help="yesterday | today | YYYY-MM-DD")
@click.option("--headful", is_flag=True, help="Show the browser (debugging).")
def capture_html(date, headful):
    """Log in and dump raw HTML/JSON only (for first-run selector refinement)."""
    d = _resolve_date(date)
    capture = _fetch_capture(d, headless=not headful)
    click.echo(f"Captured {len(capture.page_htmls)} meeting pages, "
               f"{len(capture.json_payloads)} JSON payloads → {store.RAW_DIR / d}")


@cli.command()
@click.option("--date", default="yesterday", help="yesterday | today | YYYY-MM-DD")
@click.option("--from-raw", is_flag=True, help="Reprocess a previously captured raw dump.")
@click.option("--no-email", is_flag=True, help="Do not send the email.")
@click.option("--headful", is_flag=True, help="Show the browser (debugging).")
@click.pass_context
def run(ctx, date, from_raw, no_email, headful):
    """Fetch (or reprocess) yesterday's TFigs, reconcile, store, and email."""
    d = _resolve_date(date)
    if from_raw:
        capture = store.read_raw_capture(d)
        if capture is None:
            click.echo(f"No raw capture at {store.RAW_DIR / d}", err=True)
            sys.exit(1)
    else:
        capture = _fetch_capture(d, headless=not headful)

    tf_df = results_mod.build_results_df(capture)
    if tf_df.empty:
        click.echo("No TFig rows parsed — check the raw dump / selectors.", err=True)

    day = reconcile(d, tf_df)
    store.write_day(day)
    correction = compute_correction(store.load_history())
    store.write_correction(correction)

    md = report_mod.render_markdown(day, correction, store.load_history())
    store.report_md_path(d).write_text(md, encoding="utf-8")
    store.report_html_path(d).write_text(
        report_mod.render_html(day, correction, store.load_history()), encoding="utf-8")

    report_mod.send_email(day, correction, store.load_history(), dry_run=no_email)
    click.echo(f"Recon {d}: matched {day.n_matched}/{day.n_tf}, "
               f"MAE={day.mae}, bias={day.bias}, outliers={day.n_outliers}")


@cli.command()
@click.option("--date", default="yesterday", help="yesterday | today | YYYY-MM-DD")
@click.option("--no-email", is_flag=True, help="Do not send the email.")
def reconcile_cmd(date, no_email):
    """Reconcile from an existing raw capture (alias for `run --from-raw`)."""
    d = _resolve_date(date)
    capture = store.read_raw_capture(d)
    if capture is None:
        click.echo(f"No raw capture at {store.RAW_DIR / d}", err=True)
        sys.exit(1)
    tf_df = results_mod.build_results_df(capture)
    day = reconcile(d, tf_df)
    store.write_day(day)
    correction = compute_correction(store.load_history())
    store.write_correction(correction)
    store.report_md_path(d).write_text(
        report_mod.render_markdown(day, correction, store.load_history()), encoding="utf-8")
    report_mod.send_email(day, correction, store.load_history(), dry_run=no_email)
    click.echo(f"Recon {d}: matched {day.n_matched}/{day.n_tf}")


# Register with a hyphenated name.
cli.add_command(reconcile_cmd, name="reconcile")


@cli.command()
@click.option("--date", required=True, help="YYYY-MM-DD (must have a stored {date}.csv).")
def report(date):
    """Print the stored markdown report for a date."""
    d = _resolve_date(date)
    path = store.report_md_path(d)
    if not path.exists():
        click.echo(f"No report at {path}", err=True)
        sys.exit(1)
    click.echo(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    cli()
