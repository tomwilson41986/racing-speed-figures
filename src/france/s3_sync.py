"""Upload the SQLite database to S3 periodically during backfill."""

import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


class S3Sync:
    """Handles uploading the local SQLite DB file to an S3 bucket."""

    def __init__(self, bucket: str, key: str = "france.db", db_path: str = "france.db"):
        self.bucket = bucket
        self.key = key
        self.db_path = os.path.abspath(db_path)
        self.s3 = boto3.client("s3")
        self._last_sync_time: float = 0

    def upload(self) -> bool:
        """Upload the database file to S3.  Returns True on success."""
        if not os.path.exists(self.db_path):
            log.warning("DB file %s not found, skipping S3 upload.", self.db_path)
            return False

        size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
        log.info("Uploading %s (%.1f MB) → s3://%s/%s ...", self.db_path, size_mb, self.bucket, self.key)

        try:
            self.s3.upload_file(self.db_path, self.bucket, self.key)
            self._last_sync_time = time.time()
            log.info("S3 upload complete.")
            return True
        except ClientError as e:
            log.error("S3 upload failed: %s", e)
            return False

    def download(self) -> bool:
        """Download the database file from S3.  Returns True on success."""
        log.info("Downloading s3://%s/%s → %s ...", self.bucket, self.key, self.db_path)
        try:
            self.s3.download_file(self.bucket, self.key, self.db_path)
            size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
            log.info("S3 download complete (%.1f MB).", size_mb)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                log.info("No existing DB on S3, starting fresh.")
            else:
                log.error("S3 download failed: %s", e)
            return False

    def maybe_sync(self, interval_seconds: int = 300) -> bool:
        """Upload if at least *interval_seconds* have passed since last sync."""
        elapsed = time.time() - self._last_sync_time
        if elapsed >= interval_seconds:
            return self.upload()
        return False
