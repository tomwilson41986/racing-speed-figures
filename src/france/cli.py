"""
CLI for French racing data ingestion, speed figures, and status.

Usage:
    python -m src.france.cli backfill --start 2015-01-01 --end 2026-03-09
    python -m src.france.cli ingest-today
    python -m src.france.cli compute-figures
    python -m src.france.cli compute-figures --date 2026-03-15
    python -m src.france.cli figures-status
    python -m src.france.cli status
"""

import datetime
import logging
import os
import sys

import click
from dotenv import load_dotenv

load_dotenv()
from sqlalchemy import func, select

from .database import (
    DailyFigureRow,
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
@click.option("--s3-bucket", default=lambda: os.environ.get("S3_BUCKET"),
              help="S3 bucket to sync DB to during backfill (default: $S3_BUCKET).")
@click.option("--s3-key", default=lambda: os.environ.get("S3_KEY", "france.db"),
              help="S3 object key for the DB file (default: $S3_KEY or france.db).")
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
@click.option("--s3-bucket", default=lambda: os.environ.get("S3_BUCKET"),
              help="S3 bucket name (default: $S3_BUCKET).")
@click.option("--s3-key", default=lambda: os.environ.get("S3_KEY", "france.db"),
              help="S3 object key (default: $S3_KEY or france.db).")
@click.pass_context
def s3_upload(ctx, s3_bucket, s3_key):
    """Upload the current database to S3."""
    if not s3_bucket:
        click.echo("Error: --s3-bucket or S3_BUCKET env var required.", err=True)
        sys.exit(1)
    db_url = str(ctx.obj["engine"].url)
    db_path = db_url.replace("sqlite:///", "") if "sqlite:///" in db_url else "france.db"
    sync = S3Sync(bucket=s3_bucket, key=s3_key, db_path=db_path)
    if sync.upload():
        click.echo("Upload complete.")
    else:
        click.echo("Upload failed.", err=True)
        sys.exit(1)


@cli.command("s3-download")
@click.option("--s3-bucket", default=lambda: os.environ.get("S3_BUCKET"),
              help="S3 bucket name (default: $S3_BUCKET).")
@click.option("--s3-key", default=lambda: os.environ.get("S3_KEY", "france.db"),
              help="S3 object key (default: $S3_KEY or france.db).")
@click.pass_context
def s3_download(ctx, s3_bucket, s3_key):
    """Download the database from S3."""
    if not s3_bucket:
        click.echo("Error: --s3-bucket or S3_BUCKET env var required.", err=True)
        sys.exit(1)
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


@cli.command("build-artifacts")
@click.option("--s3-bucket", default=lambda: os.environ.get("S3_BUCKET", "frenchspeedfigures"),
              help="S3 bucket name (default: $S3_BUCKET or frenchspeedfigures).")
@click.option("--s3-key", default=lambda: os.environ.get("S3_KEY", "france.db"),
              help="S3 object key (default: $S3_KEY or france.db).")
@click.option("--output-dir", default=None,
              help="Output directory for artifacts (default: output/france).")
@click.option("--skip-download", is_flag=True,
              help="Skip S3 download (use existing local DB).")
@click.option("--min-races", type=int, default=None,
              help="Override minimum races for standard times.")
@click.pass_context
def build_artifacts(ctx, s3_bucket, s3_key, output_dir, skip_download, min_races):
    """Build artifact files (standard times, going allowances, LPL) from the database.

    Downloads france.db from S3 (unless --skip-download), runs the full pipeline,
    and saves lookup tables to output/france/ for use by live daily ratings.
    """
    from .speed_figures import run_pipeline, save_artifacts
    from . import constants as C

    engine = ctx.obj["engine"]

    # Download DB from S3 if requested
    if not skip_download:
        if not s3_bucket:
            click.echo("Error: --s3-bucket or S3_BUCKET env var required "
                        "(or use --skip-download).", err=True)
            sys.exit(1)
        db_url = str(engine.url)
        db_path = db_url.replace("sqlite:///", "") if "sqlite:///" in db_url else "france.db"
        sync = S3Sync(bucket=s3_bucket, key=s3_key, db_path=db_path)
        click.echo(f"Downloading s3://{s3_bucket}/{s3_key} ...")
        if not sync.download():
            click.echo("Download failed.", err=True)
            sys.exit(1)
        click.echo("Download complete.")
        # The S3 download replaced the DB file on disk, but the engine's
        # connection pool still holds open handles to the old (empty) file
        # created by init_db().  Re-create the engine so new sessions
        # connect to the freshly-downloaded database.
        engine.dispose()
        engine = get_engine(db_url)
        ctx.obj["engine"] = engine

    if min_races is not None:
        C.MIN_RACES_STANDARD_TIME = min_races
        click.echo(f"(Using --min-races={min_races})")

    session = get_session(engine)
    try:
        click.echo("Running full pipeline to compute artifacts...")
        result = run_pipeline(session, return_artifacts=True)

        if isinstance(result, dict):
            df = result["df"]
        else:
            click.echo("Error: pipeline returned no artifacts (empty data?).", err=True)
            sys.exit(1)

        if df.empty:
            click.echo("No data — cannot build artifacts.", err=True)
            sys.exit(1)

        paths = save_artifacts(
            std_dict=result["std_dict"],
            std_df=result["std_df"],
            lpl_dict=result["lpl_dict"],
            ga_dict=result["ga_dict"],
            ga_se_dict=result["ga_se_dict"],
            cal_params=result.get("cal_params"),
            empirical_ga_priors=result.get("empirical_ga_priors"),
            output_dir=output_dir,
        )

        click.echo()
        click.echo("=== Artifacts Built ===")
        click.echo(f"  Standard times:   {len(result['std_dict']):,} combos")
        click.echo(f"  LPL entries:      {len(result['lpl_dict']):,} combos")
        click.echo(f"  Going allowances: {len(result['ga_dict']):,} meetings")
        cal_p = result.get("cal_params", {})
        if cal_p:
            click.echo(f"  Calibration:      {len([k for k in cal_p if k != 'ga_coeff']):,} class params")
        click.echo()
        for label, path in paths.items():
            click.echo(f"  {label}: {path}")

    finally:
        session.close()


@cli.command("compute-figures")
@click.option("--date", "single_date", type=click.DateTime(formats=["%Y-%m-%d"]),
              default=None, help="Compute figures for a single date only.")
@click.option("--start", type=click.DateTime(formats=["%Y-%m-%d"]),
              default=None, help="Start date for figure computation.")
@click.option("--end", type=click.DateTime(formats=["%Y-%m-%d"]),
              default=None, help="End date for figure computation.")
@click.option("--no-persist", is_flag=True,
              help="Compute figures but don't write to DB.")
@click.option("--min-races", type=int, default=None,
              help="Override minimum races for standard times (default 20).")
@click.option("--s3-bucket", default=lambda: os.environ.get("S3_BUCKET", "frenchspeedfigures"),
              help="S3 bucket name (default: $S3_BUCKET or frenchspeedfigures).")
@click.option("--s3-key", default=lambda: os.environ.get("S3_KEY", "france.db"),
              help="S3 object key (default: $S3_KEY or france.db).")
@click.option("--skip-download", is_flag=True,
              help="Skip S3 download (use existing local DB).")
@click.pass_context
def compute_figures(ctx, single_date, start, end, no_persist, min_races,
                    s3_bucket, s3_key, skip_download):
    """Compute speed figures for French flat races.

    Downloads france.db from S3 by default to ensure standard times are
    generated from the latest complete dataset.  Use --skip-download to
    use an existing local DB instead.

    With no date options, computes figures for ALL data in the database.
    Use --date for a single day, or --start/--end for a range.
    """
    from .speed_figures import persist_figures, run_pipeline
    from . import constants as C

    engine = ctx.obj["engine"]

    # Download DB from S3 by default
    if not skip_download:
        if not s3_bucket:
            click.echo("Warning: no S3 bucket configured, using local DB. "
                        "Set S3_BUCKET or use --s3-bucket.", err=True)
        else:
            db_url = str(engine.url)
            db_path = db_url.replace("sqlite:///", "") if "sqlite:///" in db_url else "france.db"
            sync = S3Sync(bucket=s3_bucket, key=s3_key, db_path=db_path)
            click.echo(f"Downloading s3://{s3_bucket}/{s3_key} ...")
            if sync.download():
                click.echo("Download complete.")
                # Re-create engine so sessions connect to the fresh DB
                engine.dispose()
                engine = get_engine(db_url)
                ctx.obj["engine"] = engine
            else:
                click.echo("S3 download failed — falling back to local DB.", err=True)

    session = get_session(engine)

    # Allow overriding minimum races threshold for limited data
    if min_races is not None:
        C.MIN_RACES_STANDARD_TIME = min_races
        click.echo(f"(Using --min-races={min_races} for standard times)")

    start_d = single_date.date() if single_date else (start.date() if start else None)
    end_d = single_date.date() if single_date else (end.date() if end else None)

    if single_date:
        click.echo(f"Computing figures for {start_d} ...")
    elif start_d or end_d:
        click.echo(f"Computing figures: {start_d or '...'} → {end_d or '...'}")
    else:
        click.echo("Computing figures for ALL data ...")

    try:
        df = run_pipeline(session, start_date=start_d, end_date=end_d)

        if df.empty:
            click.echo("No data to compute figures for.")
            return

        has_fig = df["figure_final"].notna()
        click.echo(f"\nRunners with figures: {has_fig.sum():,} / {len(df):,}")

        if has_fig.any():
            click.echo(f"Figure range: {df.loc[has_fig, 'figure_final'].min():.0f} "
                        f"to {df.loc[has_fig, 'figure_final'].max():.0f}")
            click.echo(f"Figure mean:  {df.loc[has_fig, 'figure_final'].mean():.1f}")
            click.echo(f"Figure std:   {df.loc[has_fig, 'figure_final'].std():.1f}")

        if not no_persist:
            n = persist_figures(session, df)
            click.echo(f"Persisted figures for {n:,} runners.")
        else:
            click.echo("(--no-persist: figures not written to DB)")

    finally:
        session.close()


@cli.command("figures-status")
@click.pass_context
def figures_status(ctx):
    """Show speed figure coverage statistics."""
    engine = ctx.obj["engine"]
    session = get_session(engine)

    try:
        n_runners = session.execute(
            select(func.count(RunnerRow.id))
        ).scalar() or 0
        n_with_fig = session.execute(
            select(func.count(RunnerRow.id)).where(RunnerRow.speed_figure.isnot(None))
        ).scalar() or 0
        n_daily = session.execute(
            select(func.count(DailyFigureRow.id))
        ).scalar() or 0

        click.echo("=== Speed Figure Coverage ===")
        click.echo(f"Total runners:          {n_runners:>8,}")
        click.echo(f"Runners with figures:   {n_with_fig:>8,}")
        if n_runners > 0:
            pct = 100.0 * n_with_fig / n_runners
            click.echo(f"Coverage:               {pct:>7.1f}%")
        click.echo(f"Daily figure records:   {n_daily:>8,}")

        if n_with_fig == 0:
            click.echo("\nNo figures computed yet. Run 'compute-figures' to start.")
            return

        # Distribution summary
        from sqlalchemy import cast, Float
        avg_fig = session.execute(
            select(func.avg(RunnerRow.speed_figure)).where(
                RunnerRow.speed_figure.isnot(None)
            )
        ).scalar()
        min_fig = session.execute(
            select(func.min(RunnerRow.speed_figure)).where(
                RunnerRow.speed_figure.isnot(None)
            )
        ).scalar()
        max_fig = session.execute(
            select(func.max(RunnerRow.speed_figure)).where(
                RunnerRow.speed_figure.isnot(None)
            )
        ).scalar()

        click.echo(f"\nFigure distribution:")
        click.echo(f"  Min:  {min_fig:>8.1f}")
        click.echo(f"  Mean: {avg_fig:>8.1f}")
        click.echo(f"  Max:  {max_fig:>8.1f}")

        # Coverage by year
        click.echo("\nCoverage by year:")
        yearly = session.execute(
            select(
                func.strftime("%Y", MeetingRow.race_date).label("year"),
                func.count(RunnerRow.id).label("total"),
                func.count(RunnerRow.speed_figure).label("with_fig"),
            )
            .join(RaceRow, RaceRow.meeting_id == MeetingRow.id)
            .join(RunnerRow, RunnerRow.race_id == RaceRow.id)
            .group_by("year")
            .order_by("year")
        ).all()
        for year, total, with_fig in yearly:
            pct = 100.0 * with_fig / total if total > 0 else 0
            click.echo(f"  {year}  {with_fig:>7,} / {total:>7,}  ({pct:.0f}%)")

    finally:
        session.close()


@cli.command("rate-today")
@click.option("--date", "target_date", type=click.DateTime(formats=["%Y-%m-%d"]),
              default=None, help="Date to rate (default: today).")
@click.option("--artifact-dir", default=None,
              help="Artifact directory (default: output/france).")
@click.option("--output-csv", default=None,
              help="Output CSV path.")
@click.option("--s3-bucket", default=lambda: os.environ.get("S3_BUCKET", "frenchspeedfigures"),
              help="S3 bucket name (default: $S3_BUCKET or frenchspeedfigures).")
@click.option("--s3-key", default=lambda: os.environ.get("S3_KEY", "france.db"),
              help="S3 object key (default: $S3_KEY or france.db).")
@click.option("--skip-download", is_flag=True,
              help="Skip S3 download (use existing local DB).")
@click.pass_context
def rate_today(ctx, target_date, artifact_dir, output_csv,
               s3_bucket, s3_key, skip_download):
    """Compute live daily ratings using pre-built artifacts.

    Downloads france.db from S3 by default to ensure ratings use the
    latest data.  Loads standard times, LPL, and going allowances from
    output/france/, then rates all runners for the target date.
    """
    from .live_ratings import FranceLiveRatingEngine, LIVE_DIR

    engine = ctx.obj["engine"]

    # Download DB from S3 by default
    if not skip_download:
        if not s3_bucket:
            click.echo("Warning: no S3 bucket configured, using local DB.", err=True)
        else:
            db_url = str(engine.url)
            db_path = db_url.replace("sqlite:///", "") if "sqlite:///" in db_url else "france.db"
            sync = S3Sync(bucket=s3_bucket, key=s3_key, db_path=db_path)
            click.echo(f"Downloading s3://{s3_bucket}/{s3_key} ...")
            if sync.download():
                click.echo("Download complete.")
                engine.dispose()
                engine = get_engine(db_url)
                ctx.obj["engine"] = engine
            else:
                click.echo("S3 download failed — falling back to local DB.", err=True)

    session = get_session(engine)
    d = target_date.date() if target_date else datetime.date.today()

    try:
        rating_engine = FranceLiveRatingEngine(artifact_dir=artifact_dir)
        rating_engine.load()

        df = rating_engine.rate_day(session, d)

        if df.empty:
            click.echo(f"No results for {d}.")
            return

        has_fig = df["figure_final"].notna()
        click.echo(f"\n=== France Live Ratings: {d} ===")
        click.echo(f"Runners rated: {has_fig.sum()} / {len(df)}")

        if has_fig.any():
            click.echo(f"Figure range: {df.loc[has_fig, 'figure_final'].min():.0f} "
                        f"to {df.loc[has_fig, 'figure_final'].max():.0f}")

            top = df[has_fig].nlargest(10, "figure_final")
            click.echo("\nTop 10:")
            for _, r in top.iterrows():
                pos = int(r["positionOfficial"]) if r["positionOfficial"] else 0
                click.echo(
                    f"  {r.get('horseName', '?'):25s}  "
                    f"{r.get('courseName', '?'):20s}  "
                    f"Pos {pos}  Fig {r['figure_final']:.0f}"
                )

        # Save xlsx archive
        from pathlib import Path
        xlsx_dir = Path(LIVE_DIR).parent.parent / "output" / "daily_ratings"
        os.makedirs(xlsx_dir, exist_ok=True)
        xlsx_path = xlsx_dir / f"france_ratings_{d.isoformat()}.xlsx"
        xlsx_cols = [c for c in [
            "meetingDate", "courseName", "raceNumber", "race_id",
            "horseName", "positionOfficial", "distance", "going",
            "raceSurfaceName", "raceClass", "horseAge", "weightCarried",
            "finishingTime", "distanceCumulative", "going_allowance",
            "raw_figure", "weight_adj", "wfa_adj", "figure_calibrated", "figure_final",
        ] if c in df.columns]
        df[xlsx_cols].sort_values(
            ["courseName", "raceNumber", "positionOfficial"]
        ).to_excel(str(xlsx_path), index=False, sheet_name="Ratings")
        click.echo(f"XLSX: {xlsx_path}")

        # Save CSV
        os.makedirs(LIVE_DIR, exist_ok=True)
        csv_path = output_csv or str(LIVE_DIR / f"ratings_{d.isoformat()}.csv")
        out_cols = [c for c in [
            "meetingDate", "courseName", "raceNumber", "race_id",
            "horseName", "positionOfficial", "distance", "going",
            "raceSurfaceName", "raceClass", "horseAge", "weightCarried",
            "finishingTime", "distanceCumulative", "going_allowance",
            "raw_figure", "weight_adj", "wfa_adj", "figure_calibrated", "figure_final",
            "figure_comment",
        ] if c in df.columns]
        df[out_cols].to_csv(csv_path, index=False)
        click.echo(f"\nSaved: {csv_path}")

        # Save detailed audit CSV with all intermediate columns
        audit_dir = LIVE_DIR.parent.parent / "output" / "france_audit"
        os.makedirs(audit_dir, exist_ok=True)
        audit_path = audit_dir / f"audit_{d.isoformat()}.csv"
        audit_cols = [c for c in [
            "meetingDate", "courseName", "raceNumber", "race_id",
            "horseName", "positionOfficial", "distance", "going",
            "raceSurfaceName", "raceClass", "horseAge", "weightCarried",
            "finishingTime", "distanceCumulative",
            "standard_time", "lpl", "going_allowance",
            "raw_figure", "weight_adj", "figure_after_weight",
            "wfa_adj", "figure_after_wfa",
            "figure_calibrated", "figure_final", "figure_comment",
        ] if c in df.columns]
        df[audit_cols].to_csv(str(audit_path), index=False)
        click.echo(f"Audit CSV: {audit_path}")

        # Save QA output (full calculation chain + logic breakdown)
        from .live_ratings import save_qa_output
        qa_dir = save_qa_output(df, d, run_source="workflow")
        click.echo(f"QA output: {qa_dir}")

    finally:
        session.close()


if __name__ == "__main__":
    cli()
