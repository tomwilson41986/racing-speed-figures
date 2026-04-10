"""CLI for France Galop PDF downloader.

Usage:
    python -m src.france_galop.cli download-today
    python -m src.france_galop.cli download-date --date 2026-04-09
    python -m src.france_galop.cli download-range --start 2026-04-01 --end 2026-04-09
    python -m src.france_galop.cli status
    python -m src.france_galop.cli retry-failed
"""

import datetime
import logging
import os
import sys

import click
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import func, select

from src.france.database import get_engine, get_session, init_db
from .client import FranceGalopClient
from .downloader import FGPDFDownloader, PDF_BASE_DIR
from .models import FGMeetingPDF, FGRacePDF


def _setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.group()
@click.option("--db", default="sqlite:///france.db", help="Database connection string.")
@click.option("-v", "--verbose", is_flag=True, help="Enable DEBUG logging.")
@click.pass_context
def cli(ctx, db, verbose):
    """France Galop PDF downloader."""
    _setup_logging(verbose)
    engine = get_engine(db)
    init_db(engine)
    ctx.ensure_object(dict)
    ctx.obj["engine"] = engine


def _make_downloader(ctx, headless: bool = True) -> FGPDFDownloader:
    """Create an authenticated FranceGalopClient and FGPDFDownloader."""
    engine = ctx.obj["engine"]
    session = get_session(engine)

    client = FranceGalopClient(headless=headless)
    click.echo("Logging in to France Galop...")
    if not client.login():
        click.echo("Login failed. Check FG_EMAIL and FG_PASSWORD.", err=True)
        sys.exit(1)
    click.echo("Login successful.")

    downloader = FGPDFDownloader(
        client=client,
        db_session=session,
        s3_bucket=os.environ.get("S3_BUCKET"),
    )
    return downloader


@cli.command("download-today")
@click.option("--headless/--no-headless", default=True, help="Run browser headless.")
@click.pass_context
def download_today(ctx, headless):
    """Download sectional times PDFs for today's races."""
    downloader = _make_downloader(ctx, headless=headless)
    today = datetime.date.today()

    click.echo(f"Downloading PDFs for {today.isoformat()}...")
    stats = downloader.download_date(today)
    _print_stats(stats)


@cli.command("download-date")
@click.option(
    "--date", "target_date", required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Date to download (YYYY-MM-DD).",
)
@click.option("--headless/--no-headless", default=True, help="Run browser headless.")
@click.pass_context
def download_date(ctx, target_date, headless):
    """Download sectional times PDFs for a specific date."""
    downloader = _make_downloader(ctx, headless=headless)
    d = target_date.date()

    click.echo(f"Downloading PDFs for {d.isoformat()}...")
    stats = downloader.download_date(d)
    _print_stats(stats)


@cli.command("download-range")
@click.option(
    "--start", required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end", required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD).",
)
@click.option("--headless/--no-headless", default=True, help="Run browser headless.")
@click.pass_context
def download_range(ctx, start, end, headless):
    """Download sectional times PDFs for a date range."""
    downloader = _make_downloader(ctx, headless=headless)
    start_d = start.date()
    end_d = end.date()

    click.echo(f"Downloading PDFs: {start_d.isoformat()} → {end_d.isoformat()}")
    stats = downloader.download_date_range(start_d, end_d)
    click.echo(f"\nDates processed: {stats['dates_processed']}")
    _print_stats(stats)


@cli.command("status")
@click.pass_context
def status(ctx):
    """Show PDF download coverage statistics."""
    engine = ctx.obj["engine"]
    session = get_session(engine)

    try:
        n_meetings = session.execute(
            select(func.count(FGMeetingPDF.id))
        ).scalar() or 0
        n_pdfs = session.execute(
            select(func.count(FGRacePDF.id))
        ).scalar() or 0
        n_success = session.execute(
            select(func.count(FGRacePDF.id)).where(
                FGRacePDF.download_status == "success"
            )
        ).scalar() or 0
        n_failed = session.execute(
            select(func.count(FGRacePDF.id)).where(
                FGRacePDF.download_status == "failed"
            )
        ).scalar() or 0
        total_bytes = session.execute(
            select(func.sum(FGRacePDF.file_size_bytes)).where(
                FGRacePDF.download_status == "success"
            )
        ).scalar() or 0

        click.echo("=== France Galop PDF Download Status ===")
        click.echo(f"Meetings tracked:     {n_meetings:>6,}")
        click.echo(f"Total PDF records:    {n_pdfs:>6,}")
        click.echo(f"  Successful:         {n_success:>6,}")
        click.echo(f"  Failed:             {n_failed:>6,}")
        total_mb = total_bytes / (1024 * 1024)
        click.echo(f"Total download size:  {total_mb:>6.1f} MB")

        if n_meetings == 0:
            click.echo("\nNo data yet. Run 'download-today' to start.")
            return

        # Date range
        min_date = session.execute(
            select(func.min(FGMeetingPDF.race_date))
        ).scalar()
        max_date = session.execute(
            select(func.max(FGMeetingPDF.race_date))
        ).scalar()
        click.echo(f"\nDate range: {min_date} → {max_date}")

        # Recent downloads
        click.echo("\nRecent dates:")
        recent = session.execute(
            select(
                FGMeetingPDF.race_date,
                FGMeetingPDF.venue,
                FGMeetingPDF.pdfs_downloaded,
                FGMeetingPDF.races_checked,
            )
            .order_by(FGMeetingPDF.race_date.desc())
            .limit(10)
        ).all()
        for race_date, venue, pdfs, races in recent:
            click.echo(
                f"  {race_date}  {venue:20s}  "
                f"{races} races  {pdfs} PDFs"
            )

        # PDF type breakdown
        click.echo("\nPDF types:")
        type_counts = session.execute(
            select(
                FGRacePDF.pdf_type,
                func.count(FGRacePDF.id),
            )
            .group_by(FGRacePDF.pdf_type)
        ).all()
        for pdf_type, count in type_counts:
            click.echo(f"  {pdf_type:25s}  {count:>5,}")

    finally:
        session.close()


@cli.command("retry-failed")
@click.option("--headless/--no-headless", default=True, help="Run browser headless.")
@click.pass_context
def retry_failed(ctx, headless):
    """Retry all PDFs with download_status='failed'."""
    downloader = _make_downloader(ctx, headless=headless)

    click.echo("Retrying failed downloads...")
    stats = downloader.retry_failed()
    click.echo(f"Retried: {stats['retried']}")
    click.echo(f"Succeeded: {stats['succeeded']}")
    click.echo(f"Still failed: {stats['still_failed']}")


@cli.command("sync-s3")
@click.option(
    "--date", "target_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Sync PDFs for a specific date. Omit for all dates.",
)
@click.pass_context
def sync_s3(ctx, target_date):
    """Upload locally saved PDFs to S3."""
    engine = ctx.obj["engine"]
    session = get_session(engine)

    s3_bucket = os.environ.get("S3_BUCKET")
    if not s3_bucket:
        click.echo("Error: S3_BUCKET env var required.", err=True)
        sys.exit(1)

    # Create a minimal downloader (no auth needed for S3 sync)
    from .downloader import FGPDFDownloader

    class _DummyClient:
        pass

    downloader = FGPDFDownloader(
        client=_DummyClient(),  # type: ignore
        db_session=session,
        s3_bucket=s3_bucket,
    )

    if target_date:
        d = target_date.date()
        uploaded = downloader.sync_pdfs_to_s3(d)
        click.echo(f"Uploaded {uploaded} PDFs for {d.isoformat()}")
    else:
        # Sync all dates
        if not PDF_BASE_DIR.exists():
            click.echo("No local PDFs found.")
            return
        total = 0
        for date_dir in sorted(PDF_BASE_DIR.iterdir()):
            if date_dir.is_dir():
                try:
                    d = datetime.date.fromisoformat(date_dir.name)
                    uploaded = downloader.sync_pdfs_to_s3(d)
                    total += uploaded
                except ValueError:
                    continue
        click.echo(f"Total uploaded: {total} PDFs")


def _print_stats(stats: dict):
    """Print download stats summary."""
    click.echo()
    click.echo("=== Download Summary ===")
    click.echo(f"Meetings checked:  {stats.get('meetings_checked', 0)}")
    click.echo(f"Races checked:     {stats.get('races_checked', 0)}")
    click.echo(f"PDFs found:        {stats.get('pdfs_found', 0)}")
    click.echo(f"PDFs downloaded:   {stats.get('pdfs_downloaded', 0)}")
    click.echo(f"PDFs skipped:      {stats.get('pdfs_skipped', 0)}")
    click.echo(f"Errors:            {stats.get('errors', 0)}")


if __name__ == "__main__":
    cli()
