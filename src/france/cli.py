"""
CLI for French racing data ingestion and status.

Usage:
    python -m src.france.cli backfill --start 2015-01-01 --end 2026-03-09
    python -m src.france.cli ingest-today
    python -m src.france.cli status
"""

import datetime
import logging
import sys

import click
from sqlalchemy import func, select

from .database import (
    MeetingRow,
    RaceRow,
    RunnerRow,
    StandardTimeRow,
    get_engine,
    get_session,
    init_db,
)
from .backfill import backfill_date_range, ingest_day
from .pmu_client import PMUClient
from .s3_sync import S3Sync


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
    """French flat racing data pipeline."""
    _setup_logging(verbose)
    engine = get_engine(db)
    init_db(engine)
    ctx.ensure_object(dict)
    ctx.obj["engine"] = engine


@cli.command()
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Start date (YYYY-MM-DD).")
@click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="End date (YYYY-MM-DD).")
@click.option("--no-resume", is_flag=True, help="Ignore last ingested date, start fresh.")
@click.option("--s3-bucket", default=None, help="S3 bucket to sync DB to during backfill.")
@click.option("--s3-key", default="france.db", help="S3 object key for the DB file.")
@click.option("--s3-interval", default=300, type=int,
              help="Seconds between S3 uploads (default 300).")
@click.pass_context
def backfill(ctx, start, end, no_resume, s3_bucket, s3_key, s3_interval):
    """Backfill historical flat race results from PMU."""
    engine = ctx.obj["engine"]
    session = get_session(engine)
    client = PMUClient()

    start_d = start.date()
    end_d = end.date()
    total_days = (end_d - start_d).days + 1

    # Resolve DB file path from connection string
    db_url = str(engine.url)
    db_path = db_url.replace("sqlite:///", "") if "sqlite:///" in db_url else "france.db"

    # Set up S3 sync if bucket provided
    s3_sync = None
    if s3_bucket:
        s3_sync = S3Sync(bucket=s3_bucket, key=s3_key, db_path=db_path)
        click.echo(f"S3 sync: s3://{s3_bucket}/{s3_key} (every {s3_interval}s)")

    click.echo(f"Backfill: {start_d} → {end_d}  ({total_days} days)")
    click.echo(f"DB: {engine.url}")
    click.echo(f"Resume: {'off' if no_resume else 'on'}")
    click.echo()

    try:
        stats = backfill_date_range(
            start_date=start_d,
            end_date=end_d,
            session=session,
            client=client,
            resume=not no_resume,
            s3_sync=s3_sync,
            s3_sync_interval=s3_interval,
        )
    except KeyboardInterrupt:
        click.echo("\nInterrupted — progress has been committed up to the last complete day.")
        if s3_sync:
            click.echo("Uploading latest DB to S3 before exit...")
            s3_sync.upload()
        session.close()
        sys.exit(1)
    finally:
        session.close()

    click.echo()
    click.echo(f"Done.  Days processed: {stats['dates_processed']}  "
               f"Races ingested: {stats['races_ingested']}  "
               f"Errors: {stats['errors']}")


@cli.command("s3-upload")
@click.option("--s3-bucket", required=True, help="S3 bucket name.")
@click.option("--s3-key", default="france.db", help="S3 object key.")
@click.pass_context
def s3_upload(ctx, s3_bucket, s3_key):
    """Upload the current database to S3."""
    db_url = str(ctx.obj["engine"].url)
    db_path = db_url.replace("sqlite:///", "") if "sqlite:///" in db_url else "france.db"
    sync = S3Sync(bucket=s3_bucket, key=s3_key, db_path=db_path)
    if sync.upload():
        click.echo("Upload complete.")
    else:
        click.echo("Upload failed.", err=True)
        sys.exit(1)


@cli.command("s3-download")
@click.option("--s3-bucket", required=True, help="S3 bucket name.")
@click.option("--s3-key", default="france.db", help="S3 object key.")
@click.pass_context
def s3_download(ctx, s3_bucket, s3_key):
    """Download the database from S3."""
    db_url = str(ctx.obj["engine"].url)
    db_path = db_url.replace("sqlite:///", "") if "sqlite:///" in db_url else "france.db"
    sync = S3Sync(bucket=s3_bucket, key=s3_key, db_path=db_path)
    if sync.download():
        click.echo("Download complete.")
    else:
        click.echo("Download failed or no file on S3.", err=True)
        sys.exit(1)


@cli.command("ingest-today")
@click.pass_context
def ingest_today(ctx):
    """Ingest today's flat race results."""
    engine = ctx.obj["engine"]
    session = get_session(engine)
    client = PMUClient()
    today = datetime.date.today()

    click.echo(f"Ingesting {today.isoformat()} ...")
    try:
        n = ingest_day(client, session, today)
        click.echo(f"Done — {n} flat races ingested.")
    finally:
        session.close()


@cli.command()
@click.pass_context
def status(ctx):
    """Show database counts, date ranges, and gaps."""
    engine = ctx.obj["engine"]
    session = get_session(engine)

    try:
        # Counts
        n_meetings = session.execute(select(func.count(MeetingRow.id))).scalar() or 0
        n_races = session.execute(select(func.count(RaceRow.id))).scalar() or 0
        n_runners = session.execute(select(func.count(RunnerRow.id))).scalar() or 0
        n_std = session.execute(select(func.count(StandardTimeRow.id))).scalar() or 0

        click.echo("=== French Racing Database ===")
        click.echo(f"Meetings:       {n_meetings:>8,}")
        click.echo(f"Races (PLAT):   {n_races:>8,}")
        click.echo(f"Runners:        {n_runners:>8,}")
        click.echo(f"Standard times: {n_std:>8,}")

        if n_meetings == 0:
            click.echo("\nNo data yet.  Run 'backfill' to start ingesting.")
            return

        # Date range
        min_date = session.execute(
            select(func.min(MeetingRow.race_date))
        ).scalar()
        max_date = session.execute(
            select(func.max(MeetingRow.race_date))
        ).scalar()
        click.echo(f"\nDate range: {min_date} → {max_date}")

        # Distinct dates with data
        distinct_dates = session.execute(
            select(func.count(func.distinct(MeetingRow.race_date)))
        ).scalar() or 0
        total_span = (max_date - min_date).days + 1
        click.echo(f"Days with data: {distinct_dates}  (span: {total_span} calendar days)")

        # Top tracks by race count
        click.echo("\nTop 10 tracks:")
        track_counts = session.execute(
            select(
                MeetingRow.hippodrome_code,
                func.count(RaceRow.id).label("cnt"),
            )
            .join(RaceRow, RaceRow.meeting_id == MeetingRow.id)
            .group_by(MeetingRow.hippodrome_code)
            .order_by(func.count(RaceRow.id).desc())
            .limit(10)
        ).all()
        for code, cnt in track_counts:
            click.echo(f"  {code or '(unknown)':20s}  {cnt:>6,} races")

        # Gap detection: find months with no data in the span
        if total_span > 60:
            click.echo("\nMonthly coverage (dates with data per month):")
            monthly = session.execute(
                select(
                    func.strftime("%Y-%m", MeetingRow.race_date).label("month"),
                    func.count(func.distinct(MeetingRow.race_date)).label("days"),
                )
                .group_by("month")
                .order_by("month")
            ).all()
            for month, days in monthly[-12:]:  # show last 12 months
                bar = "#" * min(days, 40)
                click.echo(f"  {month}  {days:>3} days  {bar}")
            if len(monthly) > 12:
                click.echo(f"  ... ({len(monthly) - 12} earlier months omitted)")

    finally:
        session.close()


if __name__ == "__main__":
    cli()
