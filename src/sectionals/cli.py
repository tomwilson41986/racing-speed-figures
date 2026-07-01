"""CLI for the day-after sectional pipeline.

    python -m src.sectionals.cli run --date 2026-06-26            # full pipeline
    python -m src.sectionals.cli ingest --date 2026-06-26         # Drive -> store
    python -m src.sectionals.cli build-pars                       # recompute pars
    python -m src.sectionals.cli report --date 2026-06-26 [--no-email]
    python -m src.sectionals.cli dump --file path/to.pdf          # calibrate parser
"""

from __future__ import annotations

import logging
from pathlib import Path

import click

from . import drive_client, matching, parser, pars as pars_mod, report as report_mod, store

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent
PDF_DIR = ROOT / "data" / "sectionals_pdfs"


@click.group()
def cli():
    pass


@cli.command("s3-download")
def s3_download():
    store.s3_download()


@cli.command("s3-upload")
def s3_upload():
    store.s3_upload()


@cli.command()
@click.option("--date", required=True, help="Race date YYYY-MM-DD")
def ingest(date):
    """Download the day's PDFs from Drive, parse them and store the sectionals."""
    session = store.get_session()
    index = matching.load_ratings_index(date)
    track_country = index.track_country

    try:
        service = drive_client.get_service()
    except Exception as e:
        log.error("Drive service unavailable (%s). Set GDRIVE_SA_JSON.", e)
        return

    n_pdf = n_run = 0
    for track_name, pdf in drive_client.iter_race_pdfs(service, date):
        tn = parser.normalise_track(track_name)
        country = track_country.get(tn, "GB/IRE")
        unit = "m" if country == "FR" else "f"
        dest = PDF_DIR / date / track_name / pdf["name"]
        try:
            drive_client.download_pdf(service, pdf["id"], str(dest))
            ctx = parser.parse_filename(pdf["name"], folder_date=date, folder_track=track_name)
            rs = parser.parse_pdf(str(dest), track=track_name, race_date=date,
                                  race_time=ctx["race_time"], distance_unit=unit)
            rows = parser.to_store_rows(rs, source_file=pdf["name"], country=country)
            for row in rows:
                store.upsert_runner(session, row)
            n_run += len(rows)
            store.record_pdf(session, file_id=pdf["id"], file_name=pdf["name"],
                             race_date=date, track=track_name, s3_key=None,
                             parsed_ok=bool(rows), error=rs.error)
            n_pdf += 1
        except Exception as e:  # keep going on a bad PDF
            log.warning("Failed on %s: %s", pdf["name"], e)
            store.record_pdf(session, file_id=pdf["id"], file_name=pdf["name"],
                             race_date=date, track=track_name, s3_key=None,
                             parsed_ok=False, error=str(e))
    session.commit()
    log.info("Ingested %d PDFs, %d runner rows for %s", n_pdf, n_run, date)


@cli.command("build-pars")
@click.option("--date", default=None, help="Stamp for the dated pars artifact")
def build_pars(date):
    """Recompute sectional + stride pars from the whole store."""
    session = store.get_session()
    df = pars_mod.compute_pars(session)
    stamp = date or "latest"
    pars_mod.export(df, stamp)
    log.info("Computed %d par rows", len(df))


@cli.command()
@click.option("--date", required=True, help="Race date YYYY-MM-DD (yesterday)")
@click.option("--no-email", is_flag=True, help="Render only, do not send")
def report(date, no_email):
    """Build and send the day-after sectional email."""
    report_mod.run(date, send=not no_email)


@cli.command()
@click.option("--date", required=True, help="Race date YYYY-MM-DD (yesterday)")
@click.option("--no-email", is_flag=True)
@click.option("--skip-s3", is_flag=True, help="Do not sync the store to S3")
@click.pass_context
def run(ctx, date, no_email, skip_s3):
    """Full pipeline: (s3-download) ingest -> pars -> (s3-upload) -> report."""
    if not skip_s3:
        store.s3_download()
    ctx.invoke(ingest, date=date)
    ctx.invoke(build_pars, date=date)
    if not skip_s3:
        store.s3_upload()
    report_mod.run(date, send=not no_email)


@cli.command()
@click.option("--file", "file_", required=True, help="Local PDF to dump text from")
def dump(file_):
    """Print a PDF's extracted text (calibrate the parser against real layouts)."""
    click.echo(parser.dump_text(file_))


if __name__ == "__main__":
    cli()
