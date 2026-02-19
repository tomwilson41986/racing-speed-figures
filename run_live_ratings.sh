#!/bin/bash
# Live Racing Ratings — Cron wrapper
#
# Runs the live ratings pipeline and logs output.
#
# Cron setup (6pm and 10pm GMT daily):
#   0 18 * * * /home/user/racing-speed-figures/run_live_ratings.sh
#   0 22 * * * /home/user/racing-speed-figures/run_live_ratings.sh
#
# To install:
#   chmod +x /home/user/racing-speed-figures/run_live_ratings.sh
#   crontab -e   # then add the two lines above
#
# Required environment variables (set in this script or in crontab):
#   HRB_USER   — HorseRaceBase username
#   HRB_PASS   — HorseRaceBase password
#   SMTP_USER  — Gmail address (sender)
#   SMTP_PASS  — Gmail App Password (16-char)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/data/live"
mkdir -p "${LOG_DIR}"

LOG_FILE="${LOG_DIR}/cron_$(date +%Y-%m-%d_%H%M).log"

# ── Credentials (edit these or set them in crontab MAILTO= etc.) ──
export HRB_USER="${HRB_USER:-}"
export HRB_PASS="${HRB_PASS:-}"
export SMTP_USER="${SMTP_USER:-}"
export SMTP_PASS="${SMTP_PASS:-}"

echo "=== Live Ratings Run: $(date -u '+%Y-%m-%d %H:%M:%S') UTC ===" | tee "${LOG_FILE}"

cd "${SCRIPT_DIR}"
python3 src/live_ratings.py 2>&1 | tee -a "${LOG_FILE}"

echo "=== Done: $(date -u '+%Y-%m-%d %H:%M:%S') UTC ===" | tee -a "${LOG_FILE}"
