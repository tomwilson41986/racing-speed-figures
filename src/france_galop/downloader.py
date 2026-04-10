"""Orchestrates France Galop PDF discovery and download.

Ties together the client (site navigation), database models (tracking),
and file I/O with idempotency.  PDFs are saved locally then synced to S3.

Usage:
    client = FranceGalopClient()
    client.login()
    session = get_session(engine)
    downloader = FGPDFDownloader(client, session)
    stats = downloader.download_date(datetime.date.today())
"""

import datetime
import logging
import os
import random
import time
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import select
from sqlalchemy.orm import Session

from .client import FranceGalopClient, _jittered_sleep
from .models import FGMeetingPDF, FGRacePDF

log = logging.getLogger(__name__)

PDF_BASE_DIR = Path("data/france_galop_pdfs")


class FGPDFDownloader:
    """Orchestrates France Galop PDF discovery and download for a date range.

    Parameters
    ----------
    client : FranceGalopClient
        Authenticated client for site navigation.
    db_session : Session
        SQLAlchemy session for tracking downloads.
    pdf_dir : Path
        Base directory for saving PDFs (default: data/france_galop_pdfs/).
    s3_bucket : str, optional
        S3 bucket for PDF storage.
    s3_prefix : str
        S3 key prefix for PDFs (default: france_galop_pdfs/).
    """

    def __init__(
        self,
        client: FranceGalopClient,
        db_session: Session,
        pdf_dir: Path = PDF_BASE_DIR,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "france_galop_pdfs",
    ):
        self.client = client
        self.session = db_session
        self.pdf_dir = pdf_dir
        self.s3_bucket = s3_bucket or os.environ.get("S3_BUCKET")
        self.s3_prefix = s3_prefix
        self._s3_client = None

    @property
    def s3(self):
        if self._s3_client is None:
            self._s3_client = boto3.client("s3")
        return self._s3_client

    # ---- Main entry points -------------------------------------------------

    def download_date(self, date: datetime.date) -> dict:
        """Download all sectional times PDFs for a given date.

        Returns dict with keys:
            meetings_checked, races_checked, pdfs_found,
            pdfs_downloaded, pdfs_skipped, errors
        """
        stats = {
            "meetings_checked": 0,
            "races_checked": 0,
            "pdfs_found": 0,
            "pdfs_downloaded": 0,
            "pdfs_skipped": 0,
            "errors": 0,
        }

        meetings = self.client.get_meetings_for_date(date)
        if not meetings:
            log.info("No meetings found for %s", date.isoformat())
            return stats

        for meeting_info in meetings:
            stats["meetings_checked"] += 1
            try:
                meeting_stats = self._process_meeting(date, meeting_info)
                for key in meeting_stats:
                    stats[key] += meeting_stats[key]
            except Exception:
                log.exception(
                    "Error processing meeting %s on %s — continuing.",
                    meeting_info.get("venue", "?"),
                    date.isoformat(),
                )
                stats["errors"] += 1

        self.session.commit()
        log.info(
            "Date %s: %d meetings, %d races, %d PDFs found, "
            "%d downloaded, %d skipped, %d errors",
            date.isoformat(),
            stats["meetings_checked"],
            stats["races_checked"],
            stats["pdfs_found"],
            stats["pdfs_downloaded"],
            stats["pdfs_skipped"],
            stats["errors"],
        )
        return stats

    def download_date_range(
        self, start: datetime.date, end: datetime.date
    ) -> dict:
        """Download PDFs for all dates in [start, end] inclusive.

        Returns aggregate stats.
        """
        aggregate = {
            "dates_processed": 0,
            "meetings_checked": 0,
            "races_checked": 0,
            "pdfs_found": 0,
            "pdfs_downloaded": 0,
            "pdfs_skipped": 0,
            "errors": 0,
        }

        current = start
        while current <= end:
            log.info("--- Processing %s ---", current.isoformat())
            try:
                day_stats = self.download_date(current)
                for key in day_stats:
                    aggregate[key] += day_stats[key]
            except Exception:
                log.exception(
                    "Failed to process %s — continuing.", current.isoformat()
                )
                aggregate["errors"] += 1

            aggregate["dates_processed"] += 1
            current += datetime.timedelta(days=1)
            _jittered_sleep(1.0)

        return aggregate

    # ---- Meeting processing ------------------------------------------------

    def _process_meeting(self, date: datetime.date, meeting_info: dict) -> dict:
        """Process a single meeting: discover races, find PDFs, download."""
        stats = {
            "races_checked": 0,
            "pdfs_found": 0,
            "pdfs_downloaded": 0,
            "pdfs_skipped": 0,
            "errors": 0,
        }

        venue = meeting_info["venue"]
        meeting_url = meeting_info["meeting_url"]
        fg_id = meeting_info.get("meeting_fg_id", "")

        # Get or create meeting tracking row
        meeting_row = self._get_or_create_meeting(date, venue, meeting_url, fg_id)

        # Discover races
        races = self.client.get_race_links(meeting_url)
        meeting_row.races_checked = len(races)

        for race_info in races:
            stats["races_checked"] += 1
            try:
                race_stats = self._process_race(meeting_row, race_info)
                for key in race_stats:
                    stats[key] += race_stats[key]
            except Exception:
                log.exception(
                    "Error processing race %s at %s — continuing.",
                    race_info.get("race_number", "?"),
                    venue,
                )
                stats["errors"] += 1

        # Update meeting summary
        meeting_row.pdfs_found = stats["pdfs_found"]
        meeting_row.pdfs_downloaded = stats["pdfs_downloaded"]
        self.session.flush()

        return stats

    def _process_race(self, meeting_row: FGMeetingPDF, race_info: dict) -> dict:
        """Process a single race: find PDF links, download sectional times."""
        stats = {"pdfs_found": 0, "pdfs_downloaded": 0, "pdfs_skipped": 0, "errors": 0}

        race_url = race_info["race_url"]
        race_number = race_info["race_number"]
        race_name = race_info.get("race_name", "")

        pdfs = self.client.get_pdf_links(race_url)

        # Filter for sectional times PDFs (or download all if none specifically identified)
        sectional_pdfs = [p for p in pdfs if p["pdf_type"] == "sectional_times"]
        if not sectional_pdfs:
            # If no sectional PDFs identified, keep all PDFs for review
            sectional_pdfs = pdfs

        stats["pdfs_found"] = len(sectional_pdfs)

        for pdf_info in sectional_pdfs:
            pdf_url = pdf_info["pdf_url"]
            pdf_type = pdf_info["pdf_type"]

            # Idempotency check
            if self._is_already_downloaded(meeting_row.id, race_number, pdf_type):
                log.debug(
                    "PDF already downloaded: R%s %s at %s",
                    race_number, pdf_type, meeting_row.venue,
                )
                stats["pdfs_skipped"] += 1
                continue

            # Build local path
            local_path = self._build_local_path(
                meeting_row.race_date,
                meeting_row.venue,
                race_number,
                pdf_type,
            )

            # Download
            content = self.client.get_pdf_content(pdf_url)
            if content is None:
                self._record_download(
                    meeting_row, race_number, race_name, pdf_url,
                    str(local_path), pdf_type, 0, "failed",
                    error="Download returned None",
                )
                stats["errors"] += 1
                continue

            # Save locally
            full_path = self.pdf_dir / local_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_bytes(content)
            file_size = len(content)

            # Upload to S3
            s3_success = self._upload_to_s3(local_path, content)
            if not s3_success:
                log.warning(
                    "S3 upload failed for %s (local copy saved)", local_path
                )

            # Record in database
            self._record_download(
                meeting_row, race_number, race_name, pdf_url,
                str(local_path), pdf_type, file_size, "success",
            )
            stats["pdfs_downloaded"] += 1
            log.info(
                "Downloaded: %s R%s %s (%.1f KB)",
                meeting_row.venue, race_number, pdf_type, file_size / 1024,
            )

        return stats

    # ---- Database helpers --------------------------------------------------

    def _get_or_create_meeting(
        self, date: datetime.date, venue: str, meeting_url: str, fg_id: str
    ) -> FGMeetingPDF:
        """Get existing or create new FGMeetingPDF row."""
        existing = self.session.execute(
            select(FGMeetingPDF).where(
                FGMeetingPDF.race_date == date,
                FGMeetingPDF.venue == venue,
            )
        ).scalar_one_or_none()

        if existing is not None:
            return existing

        row = FGMeetingPDF(
            race_date=date,
            venue=venue,
            meeting_url=meeting_url,
            meeting_fg_id=fg_id,
        )
        self.session.add(row)
        self.session.flush()
        return row

    def _is_already_downloaded(
        self, meeting_id: int, race_number: int, pdf_type: str
    ) -> bool:
        """Check if a PDF has already been successfully downloaded."""
        existing = self.session.execute(
            select(FGRacePDF).where(
                FGRacePDF.meeting_id == meeting_id,
                FGRacePDF.race_number == race_number,
                FGRacePDF.pdf_type == pdf_type,
                FGRacePDF.download_status == "success",
            )
        ).scalar_one_or_none()
        return existing is not None

    def _record_download(
        self,
        meeting_row: FGMeetingPDF,
        race_number: int,
        race_name: str,
        pdf_url: str,
        local_path: str,
        pdf_type: str,
        file_size: int,
        status: str,
        error: Optional[str] = None,
    ):
        """Create or update a FGRacePDF record."""
        existing = self.session.execute(
            select(FGRacePDF).where(
                FGRacePDF.meeting_id == meeting_row.id,
                FGRacePDF.race_number == race_number,
                FGRacePDF.pdf_type == pdf_type,
            )
        ).scalar_one_or_none()

        if existing is not None:
            existing.pdf_url = pdf_url
            existing.local_path = local_path
            existing.file_size_bytes = file_size
            existing.download_status = status
            existing.downloaded_at = datetime.datetime.utcnow()
            existing.error_message = error
        else:
            row = FGRacePDF(
                meeting_id=meeting_row.id,
                race_number=race_number,
                race_name=race_name,
                pdf_type=pdf_type,
                pdf_url=pdf_url,
                local_path=local_path,
                file_size_bytes=file_size,
                download_status=status,
                downloaded_at=datetime.datetime.utcnow(),
                error_message=error,
            )
            self.session.add(row)

        self.session.flush()

    # ---- File path helpers -------------------------------------------------

    def _build_local_path(
        self,
        date: datetime.date,
        venue: str,
        race_number: int,
        pdf_type: str,
    ) -> Path:
        """Build a relative path for a PDF under the base directory.

        Example: 2026-04-09/LONGCHAMP_R1_sectional_times.pdf
        """
        safe_venue = venue.replace(" ", "_").replace("/", "_")
        filename = f"{safe_venue}_R{race_number}_{pdf_type}.pdf"
        return Path(date.isoformat()) / filename

    # ---- S3 helpers --------------------------------------------------------

    def _upload_to_s3(self, local_path: Path, content: bytes) -> bool:
        """Upload PDF content to S3.  Returns True on success."""
        if not self.s3_bucket:
            log.debug("No S3 bucket configured, skipping upload.")
            return True  # Not an error — just not configured

        s3_key = f"{self.s3_prefix}/{local_path}"
        try:
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=content,
                ContentType="application/pdf",
            )
            log.debug("Uploaded to s3://%s/%s", self.s3_bucket, s3_key)
            return True
        except ClientError as e:
            log.error("S3 upload failed for %s: %s", s3_key, e)
            return False

    def sync_pdfs_to_s3(self, date: datetime.date) -> int:
        """Upload all locally saved PDFs for a date to S3.

        Returns the number of files uploaded.
        """
        if not self.s3_bucket:
            log.warning("No S3 bucket configured.")
            return 0

        date_dir = self.pdf_dir / date.isoformat()
        if not date_dir.exists():
            log.info("No local PDFs for %s", date.isoformat())
            return 0

        uploaded = 0
        for pdf_file in date_dir.glob("*.pdf"):
            relative = pdf_file.relative_to(self.pdf_dir)
            s3_key = f"{self.s3_prefix}/{relative}"
            try:
                self.s3.upload_file(str(pdf_file), self.s3_bucket, s3_key)
                uploaded += 1
                log.debug("Uploaded %s → s3://%s/%s", pdf_file, self.s3_bucket, s3_key)
            except ClientError as e:
                log.error("S3 upload failed for %s: %s", pdf_file, e)

        log.info("Uploaded %d PDFs to S3 for %s", uploaded, date.isoformat())
        return uploaded

    # ---- Retry failed downloads --------------------------------------------

    def retry_failed(self) -> dict:
        """Re-attempt all PDFs with download_status='failed'.

        Returns dict with: retried, succeeded, still_failed
        """
        failed = self.session.execute(
            select(FGRacePDF).where(FGRacePDF.download_status == "failed")
        ).scalars().all()

        stats = {"retried": 0, "succeeded": 0, "still_failed": 0}

        for pdf_row in failed:
            stats["retried"] += 1
            if not pdf_row.pdf_url:
                stats["still_failed"] += 1
                continue

            content = self.client.get_pdf_content(pdf_row.pdf_url)
            if content is None:
                stats["still_failed"] += 1
                continue

            # Save locally
            local_path = Path(pdf_row.local_path) if pdf_row.local_path else None
            if local_path:
                full_path = self.pdf_dir / local_path
                full_path.parent.mkdir(parents=True, exist_ok=True)
                full_path.write_bytes(content)

            # Upload to S3
            if local_path:
                self._upload_to_s3(local_path, content)

            pdf_row.download_status = "success"
            pdf_row.file_size_bytes = len(content)
            pdf_row.downloaded_at = datetime.datetime.utcnow()
            pdf_row.error_message = None
            stats["succeeded"] += 1

        self.session.commit()
        return stats
