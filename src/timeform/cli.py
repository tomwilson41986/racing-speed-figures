"""CLI for the Timeform TFig reconciliation.

Usage:
    python -m src.timeform.cli run          --date yesterday   # Drive PDFs → reconcile → email
    python -m src.timeform.cli list-pdfs    --date yesterday   # show which Drive PDFs are selected
    python -m src.timeform.cli run          --date yesterday --source scrape   # legacy (dead)
    python -m src.timeform.cli capture-html --date yesterday   # login + dump raw only (legacy)
    python -m src.timeform.cli reconcile    --date 2026-07-07  # reprocess a stored raw capture
    python -m src.timeform.cli report       --date 2026-07-07  # re-render report from stored csv

Sources (``--source``):
    drive   Timeform "Race Result" PDFs from Google Drive  (DEFAULT)
    scrape  Playwright login + scrape of timeform.com      (DEAD — image CAPTCHA)
    raw     a previously captured ``output/timeform_recon/raw/{date}/`` dump
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


def _we_rated_races(date: str) -> bool:
    """Did OUR pipeline produce UK/IRE figures for ``date``?

    Used to tell a genuine no-racing day (nothing to reconcile — fine) apart
    from a broken one (we rated races, so the Timeform PDFs should be there).
    """
    # Reuse the ratings-location patterns the matcher itself uses, so this can
    # never drift from where the figures are actually written.
    from pathlib import Path

    from src.sectionals.matching import UK_AUDIT, UK_CSV

    return any(Path(str(tmpl).format(date=date)).exists()
               for tmpl in (UK_CSV, UK_AUDIT))


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable DEBUG logging.")
@click.pass_context
def cli(ctx, verbose):
    """Timeform TFig reconciliation."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)


SOURCES = ("drive", "scrape", "raw")


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


def _build_tf_df(date: str, source: str, headful: bool = False):
    """Build the ``tf_df`` frame ``reconcile`` eats, from the chosen source.

    This is the one seam every source plugs into: whatever produces the frame,
    everything downstream (reconcile → store → correction → report → email) is
    source-agnostic.
    """
    if source == "drive":
        # Local import: pulls in the Google client libs only when actually used.
        from . import drive_source
        click.echo(f"Reading Timeform result PDFs from Google Drive for {date}...")
        try:
            return drive_source.build_results_df_from_drive(date)
        except (ImportError, RuntimeError) as exc:
            # Missing GDRIVE_SA_JSON, or google-api-python-client not installed.
            click.echo(f"Drive source unavailable: {exc}", err=True)
            click.echo("Set GDRIVE_SA_JSON (service-account JSON or a path to it) "
                       "and install requirements.txt.", err=True)
            sys.exit(1)

    if source == "raw":
        capture = store.read_raw_capture(date)
        if capture is None:
            click.echo(f"No raw capture at {store.RAW_DIR / date}", err=True)
            sys.exit(1)
        return results_mod.build_results_df(capture)

    if source == "scrape":
        click.echo("WARNING: the scrape source is dead (timeform.com sign-in is "
                   "behind an image CAPTCHA). Use --source drive.", err=True)
        capture = _fetch_capture(date, headless=not headful)
        return results_mod.build_results_df(capture)

    raise click.BadParameter(f"unknown source {source!r}")


@cli.command("capture-html")
@click.option("--date", default="yesterday", help="yesterday | today | YYYY-MM-DD")
@click.option("--headful", is_flag=True, help="Show the browser (debugging).")
def capture_html(date, headful):
    """Log in and dump raw HTML/JSON only (for first-run selector refinement)."""
    d = _resolve_date(date)
    capture = _fetch_capture(d, headless=not headful)
    click.echo(f"Captured {len(capture.page_htmls)} meeting pages, "
               f"{len(capture.json_payloads)} JSON payloads → {store.RAW_DIR / d}")


@cli.command("list-pdfs")
@click.option("--date", default="yesterday", help="yesterday | today | YYYY-MM-DD")
def list_pdfs(date):
    """List the Timeform result PDFs the Drive source would use for a date."""
    from .drive_source import get_reader, iter_timeform_pdfs
    d = _resolve_date(date)
    n = 0
    for item in iter_timeform_pdfs(get_reader(), d):
        n += 1
        click.echo(f"{item.race_time or '?':>5}  {item.course_hint or '?':<20} {item.name}")
    click.echo(f"{n} Timeform result PDF(s) for {d}")


@cli.command()
@click.option("--date", default="yesterday", help="yesterday | today | YYYY-MM-DD")
@click.option("--source", type=click.Choice(SOURCES), default="drive",
              show_default=True,
              help="drive = Timeform PDFs on Google Drive; scrape = legacy "
                   "Playwright login (dead); raw = a stored raw capture.")
@click.option("--from-raw", is_flag=True,
              help="Deprecated alias for --source raw.")
@click.option("--no-email", is_flag=True, help="Do not send the email.")
@click.option("--headful", is_flag=True, help="Show the browser (--source scrape only).")
@click.option("--allow-partial", is_flag=True,
              help="Publish the day even if some Timeform PDFs failed to "
                   "download/parse (default: refuse, to keep the correction "
                   "from being computed on a biased subset).")
@click.pass_context
def run(ctx, date, source, from_raw, no_email, headful, allow_partial):
    """Fetch (or reprocess) yesterday's TFigs, reconcile, store, and email."""
    d = _resolve_date(date)
    if from_raw:
        source = "raw"
    tf_df = _build_tf_df(d, source, headful=headful)

    # ── Anti-silent-success guards ────────────────────────────────────
    # This pipeline has a history of reporting green while doing nothing, and
    # its output feeds a correction applied to client-facing figures. A day is
    # published only if it is COMPLETE; anything else exits non-zero so the run
    # goes red and can simply be re-run once the cause is fixed.
    stats = tf_df.attrs.get("fetch_stats") or {}
    if tf_df.empty:
        click.echo(f"No TFig rows parsed from source={source}.", err=True)
        if source == "drive":
            if _we_rated_races(d):
                # We produced figures for this date, so there WAS racing and the
                # PDFs should exist: missing folder, un-uploaded PDFs, or the
                # service account lost access.
                click.echo(f"We rated races on {d} but found no Timeform PDFs — "
                           "check the Drive folder (`list-pdfs --date ...`).", err=True)
                sys.exit(1)
            # No PDFs and no figures of our own: a genuine no-racing day.
            click.echo(f"No racing rated by us on {d} either — nothing to "
                       "reconcile (not an error).")
            return
        sys.exit(1)

    n_failed = int(stats.get("n_failed") or 0)
    if n_failed and not allow_partial:
        # Publishing a subset would report a flattering ~100% match rate on the
        # survivors while feeding a skewed MAE/bias into the rolling correction.
        click.echo(
            f"{n_failed} of {stats.get('n_found')} Timeform PDFs failed to "
            f"download/parse for {d}. Refusing to publish a partial day "
            "(the correction would be computed from a biased subset). "
            "Re-run, or pass --allow-partial to accept it.", err=True)
        sys.exit(1)

    day = reconcile(d, tf_df)
    if day.n_matched == 0:
        # TFigs parsed but nothing joined to our figures — usually the day's
        # uk_audit CSV is missing/uncommitted, or a course-name join broke.
        click.echo(f"Parsed {day.n_tf} TFig rows for {d} but matched NONE to our "
                   "figures — not writing a hollow row. Check that our ratings "
                   f"for {d} exist and that course names line up.", err=True)
        sys.exit(1)
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
@click.option("--source", type=click.Choice(SOURCES), default="raw",
              show_default=True, help="Where to read the TFigs from.")
@click.option("--no-email", is_flag=True, help="Do not send the email.")
def reconcile_cmd(date, source, no_email):
    """Reconcile without re-fetching (defaults to a stored raw capture)."""
    d = _resolve_date(date)
    tf_df = _build_tf_df(d, source)
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
