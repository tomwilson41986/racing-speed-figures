#!/usr/bin/env python3
"""
Live Racing Ratings — Same-Day Speed Figures
=============================================
Scrapes today's results from HorseRaceBase, computes speed figures using
pre-built lookup tables from the full pipeline, and emails a formatted
ratings report.

Scheduled to run at 6pm and 10pm GMT daily.

Configuration (environment variables):
  HRB_USER         HorseRaceBase username
  HRB_PASS         HorseRaceBase password
  SMTP_USER        Sender email address (e.g. racingsquared@gmail.com)
  SMTP_PASS        Gmail App Password (16-char)

Usage:
  python src/live_ratings.py                     # Process today, send email
  python src/live_ratings.py --date 2026-02-18   # Specific date
  python src/live_ratings.py --no-email          # Compute only, no email
  python src/live_ratings.py --schedule          # Run on 6pm/10pm schedule
"""

import os
import sys
import io
import argparse
import smtplib
import logging
import time
from datetime import datetime, date, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import numpy as np

# Import pipeline constants and helpers
sys.path.insert(0, os.path.dirname(__file__))
from speed_figures import (
    BASE_RATING,
    BASE_WEIGHT_LBS,
    SECONDS_PER_LENGTH,
    LBS_PER_SECOND_5F,
    BENCHMARK_FURLONGS,
    LPL_SURFACE_MULTIPLIER,
    SEX_ALLOWANCE_SUMMER,
    SEX_ALLOWANCE_WINTER,
    FEMALE_GENDERS,
    UK_COURSES,
    IRE_COURSES,
    WFA_3YO,
    WFA_2YO,
    generic_lbs_per_length,
    get_wfa_allowance,
    compute_class_adjustment,
    interpolate_lookup,
)

# ─── Directories ─────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT_DIR / "output"
LIVE_DIR = ROOT_DIR / "data" / "live"

# ─── Configuration (environment variables) ───────────────────────────
RECIPIENTS = [
    "racingsquared@gmail.com",
    "tom.biggs@blandfordbloodstock.com",
    "fred@blandfordbloodstock.com",
    "stuart@blandfordbloodstock.com",
    "richard@blandfordbloodstock.com",
]
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
HRB_USER = os.environ.get("HRB_USER", "")
HRB_PASS = os.environ.get("HRB_PASS", "")
HRB_USER_ID = os.environ.get("HRB_USER_ID", "")

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("live_ratings")

# ─── NH race types to exclude (keep flat racing only) ────────────────
NH_RACE_TYPES = {
    "Maiden Hurdle", "Novices Hurdle", "Handicap Hurdle",
    "Handicap Chase", "Handicap Novices Chase", "Novices Chase",
    "Hurdle", "Chase", "NH Flat", "Bumper", "Hunters Chase",
    "National Hunt Flat", "Beginners Chase",
}

# ─── Going allowance estimates (seconds per furlong) ─────────────────
# These are used as fallback when real GA cannot be computed (e.g.
# incomplete meetings at the 6pm run).  Values updated 2026-03 from
# empirical pipeline going allowances (2015-2026, 10,625 meetings).
# Previous estimates massively underestimated soft (+0.15 vs actual +0.51)
# and heavy (+0.25 vs actual +0.82), causing 2-3 lbs errors on those
# going descriptions.
GOING_GA_ESTIMATES = {
    "Hard": -0.25, "Firm": -0.21,
    "Good To Firm": -0.09, "Good to Firm": -0.09,
    "Gd/Frm": -0.09,
    "Good": 0.05,
    "Good to Yielding": 0.15, "Good To Yielding": 0.15,
    "Yielding": 0.35, "Yielding To Soft": 0.40,
    "Good to Soft": 0.25, "Good To Soft": 0.25,
    "Gd/Sft": 0.25,
    "Soft": 0.51, "Soft To Heavy": 0.65,
    "Sft/Hvy": 0.65, "Hvy/Sft": 0.65,
    "Heavy": 0.82,
    "Standard": 0.04, "Std": 0.04,
    "Standard To Slow": 0.06, "Std/Slow": 0.06, "Standard/Slow": 0.06,
    "Slow": 0.15,
    "Standard To Fast": 0.01, "Std/Fast": 0.01,
    "Fast": -0.03,
}

# ─── Going group mapping (for calibration offsets) ────────────────
GOING_GROUPS = {
    "Firm": ["Hard", "Firm", "Fast"],
    "GdFm": ["Gd/Frm", "Good To Firm", "Good to Firm", "Std/Fast"],
    "Good": ["Good", "Standard", "Std"],
    "GdSft": [
        "Gd/Sft", "Good to Soft", "Good To Yielding",
        "Good to Yielding", "Std/Slow", "Standard/Slow",
        "Standard To Slow", "Standard to Slow", "Slow",
    ],
    "Soft": ["Soft", "Yielding", "Yld/Sft", "Sft/Hvy", "Hvy/Sft"],
    "Heavy": ["Heavy"],
}
GOING_MAP = {}
for _grp, _goings in GOING_GROUPS.items():
    for _g in _goings:
        GOING_MAP[_g] = _grp

# ─── Going ordinal encoding (for GBR features) ────────────────────
GOING_ORDINAL = {
    "Hard": 1, "Firm": 1, "Fast": 1,
    "Gd/Frm": 2, "Good To Firm": 2, "Good to Firm": 2, "Std/Fast": 2,
    "Good": 3, "Standard": 3, "Std": 3,
    "Gd/Sft": 4, "Good to Soft": 4, "Good To Yielding": 4,
    "Good to Yielding": 4, "Std/Slow": 4, "Standard/Slow": 4,
    "Standard To Slow": 4, "Standard to Slow": 4, "Slow": 4,
    "Soft": 5, "Yielding": 5, "Yld/Sft": 5, "Sft/Hvy": 5, "Hvy/Sft": 5,
    "Heavy": 6,
}

# ─── Beaten-distance text codes (lengths) ─────────────────────────
BEATEN_DIST_CODES = {
    "NK": 0.15, "nk": 0.15,     # Neck
    "HD": 0.10, "hd": 0.10,     # Head
    "SH": 0.05, "sh": 0.05,     # Short Head
    "NSE": 0.03, "nse": 0.03,   # Nose
}

# AW surface types from HorseRaceBase
AW_SURFACE_TYPES = {"Polytrack", "Tapeta", "Fibresand"}

# ─── HorseRaceBase → Timeform course name mapping ───────────────────
# HRB uses short names; Timeform uses full official names.
HRB_TO_TIMEFORM_COURSE = {
    "KEMPTON": "KEMPTON PARK",
    "LINGFIELD": "LINGFIELD PARK",
    "EPSOM": "EPSOM DOWNS",
    "SANDOWN": "SANDOWN PARK",
    "HAMILTON": "HAMILTON PARK",
    "HAYDOCK": "HAYDOCK PARK",
    "CATTERICK": "CATTERICK BRIDGE",
    "NEWMARKET": "NEWMARKET (ROWLEY)",
    "NEWMARKET (JULY)": "NEWMARKET (JULY)",
    "NEWMARKET (ROWLEY)": "NEWMARKET (ROWLEY)",
}


# ═════════════════════════════════════════════════════════════════════
# DATA ACQUISITION — HorseRaceBase Scraper
# ═════════════════════════════════════════════════════════════════════

def fetch_results(target_date):
    """
    Fetch race results for a given date.

    Tries:
      1. Manual CSV override in data/live/<date>.csv or data/live/today.csv
      2. HorseRaceBase CSV download (requires credentials)
    """
    log.info(f"Fetching results for {target_date}")

    # Strategy 1: Manual CSV (takes priority — lets you test with your own data)
    df = _load_manual_csv(target_date)
    if df is not None and len(df) > 0:
        return df

    # Strategy 2: HorseRaceBase
    df = _fetch_from_hrb(target_date)
    if df is not None and len(df) > 0:
        return df

    log.error(
        f"No results found for {target_date}. "
        f"Set HRB_USER/HRB_PASS env vars for automatic download, or "
        f"place a CSV at {LIVE_DIR}/{target_date}.csv"
    )
    return None


def _fetch_from_hrb(target_date):
    """Download results CSV from HorseRaceBase for a specific date."""
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        log.warning("requests/beautifulsoup4 not installed — skipping HRB")
        return None

    hrb_user = HRB_USER
    hrb_pass = HRB_PASS
    if not hrb_user or not hrb_pass:
        log.info("HRB credentials not set (HRB_USER / HRB_PASS) — skipping")
        return None

    log.info(f"HRB_USER={hrb_user[:3]}***, HRB_USER_ID={HRB_USER_ID}")

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })

    try:
        import re

        # Step 1: Get CSRF token from multiple pages
        log.info("Logging into HorseRaceBase...")

        csrf = ""
        for page_url in [
            "https://www.horseracebase.com/horse-racing-results.php",
            "https://www.horseracebase.com/horsebase1.php",
            "https://www.horseracebase.com/",
        ]:
            resp = session.get(page_url, timeout=15)
            soup = BeautifulSoup(resp.text, "html.parser")
            csrf_input = soup.find("input", {"name": "CSRFtoken"})
            if csrf_input:
                csrf = csrf_input["value"]
                log.info(f"  CSRF token found on {page_url}")
                break

        if not csrf:
            # Try extracting from raw HTML via regex (JS-rendered forms)
            match = re.search(
                r'name=["\']CSRFtoken["\']\s+value=["\']([^"\']+)',
                resp.text,
            )
            if match:
                csrf = match.group(1)
                log.info("  CSRF token found via regex")
            else:
                log.warning(
                    "  No CSRF token found — login may fail. "
                    "Attempting without it."
                )

        # Step 2: Login
        login_resp = session.post(
            "https://www.horseracebase.com/horsebase1.php",
            data={
                "login": hrb_user,
                "password": hrb_pass,
                "CSRFtoken": csrf,
            },
            timeout=15,
        )
        if login_resp.status_code != 200:
            log.error(f"HRB login returned HTTP {login_resp.status_code}")
            return None

        # Verify login succeeded
        login_ok = "log out" in login_resp.text.lower()
        if not login_ok:
            # Check for other success indicators
            login_ok = (
                "my horseracebase" in login_resp.text.lower()
                or "welcome" in login_resp.text.lower()
            )

        if not login_ok:
            log.error(
                "HRB login FAILED — 'log out' not found in response. "
                "Check HRB_USER and HRB_PASS secrets are correct. "
                f"Response length: {len(login_resp.text)} chars, "
                f"cookies: {list(session.cookies.keys())}"
            )
            # Show a snippet of the response for debugging
            snippet = login_resp.text[:500].replace("\n", " ")
            log.error(f"Response snippet: {snippet}")
            return None

        log.info("  Login successful")

        # Step 3: Get the results page for the target date and parse
        # the CSV download form to extract the correct parameters
        # (including the user ID embedded as a hidden field).
        results_url = (
            "https://www.horseracebase.com/horse-racing-results.php"
            f"?racedate={target_date}"
        )
        log.info(f"Fetching results page: {results_url}")
        results_resp = session.get(results_url, timeout=15)
        results_soup = BeautifulSoup(results_resp.text, "html.parser")

        # Find the form that submits to excelresults.php (the CSV form)
        csv_form_params = None
        for form in results_soup.find_all("form"):
            action = (form.get("action") or "").lower()
            if "excelresults" in action:
                csv_form_params = {}
                for inp in form.find_all("input"):
                    name = inp.get("name")
                    value = inp.get("value", "")
                    if name:
                        csv_form_params[name] = value
                log.info(
                    f"  Found CSV form (action={form.get('action')}), "
                    f"fields: {csv_form_params}"
                )
                break

        if csv_form_params is None:
            # Log all forms found for debugging
            all_forms = results_soup.find_all("form")
            log.warning(
                f"  No CSV form found on results page. "
                f"Found {len(all_forms)} form(s) with actions: "
                f"{[f.get('action') for f in all_forms]}"
            )

        # Also try to find the user ID via regex as a fallback
        user_id = HRB_USER_ID
        if not user_id and csv_form_params:
            user_id = csv_form_params.get("user", "")
        if not user_id:
            # Search across multiple pages for patterns containing user ID
            log.info("  HRB_USER_ID not set — attempting auto-detection...")
            search_pages = [
                ("login response", login_resp.text),
                ("results page", results_resp.text),
            ]
            for page_name, page_url in [
                ("excel results page", "https://www.horseracebase.com/excelresults.php"),
                ("account page", "https://www.horseracebase.com/payments.php"),
                ("profile page", "https://www.horseracebase.com/myhrb.php"),
            ]:
                try:
                    page_resp = session.get(page_url, timeout=15)
                    search_pages.append((page_name, page_resp.text))
                except Exception:
                    pass

            patterns = [
                r'["\']user["\'][,:\s]+["\']?(\d{3,})',
                r'user_?id["\s=:]+["\']?(\d{3,})',
                r'userid["\s=:]+["\']?(\d{3,})',
                r'name=["\']user["\'][^>]*value=["\'](\d{3,})',
                r'value=["\'](\d{3,})["\'][^>]*name=["\']user',
                r'excelresults\.php\?[^"\']*user=(\d{3,})',
                r'/user/(\d{3,})',
            ]

            for page_name, page_text in search_pages:
                for pattern in patterns:
                    uid_match = re.search(pattern, page_text, re.IGNORECASE)
                    if uid_match:
                        user_id = uid_match.group(1)
                        log.info(
                            f"  Auto-detected HRB user ID: {user_id} "
                            f"(from {page_name}, pattern: {pattern})"
                        )
                        break
                if user_id:
                    break

        if not user_id:
            log.warning(
                "Could not auto-detect HRB user ID. "
                "Will attempt CSV download without it. "
                "If this fails, set HRB_USER_ID as a GitHub secret."
            )

        # Step 4: Download date-specific CSV
        # Strategy A: Use the exact form fields parsed from the results page
        # Strategy B: Build parameters manually (with/without user ID)
        download_attempts = []
        if csv_form_params:
            # Use form fields, ensure csv=1 and correct date
            form_data = dict(csv_form_params)
            form_data["csv"] = "1"
            form_data["racedate"] = target_date
            download_attempts.append(form_data)
        if user_id:
            download_attempts.append(
                {"csv": "1", "user": user_id, "racedate": target_date}
            )
        # Also try without user ID (server may infer from session)
        download_attempts.append(
            {"csv": "1", "racedate": target_date}
        )

        content = None
        for attempt_data in download_attempts:
            log.info(
                f"Downloading results CSV for {target_date} "
                f"(params={attempt_data})..."
            )
            csv_resp = session.post(
                "https://www.horseracebase.com/excelresults.php",
                data=attempt_data,
                timeout=30,
            )

            if csv_resp.status_code != 200:
                log.warning(
                    f"CSV download returned HTTP {csv_resp.status_code}"
                )
                continue

            resp_text = csv_resp.text.strip()

            # Skip HTML error pages (including bare <p> tags from HRB)
            if (
                "<html" in resp_text.lower()
                or "<!doctype" in resp_text.lower()
                or resp_text.lstrip().startswith("<")
            ):
                snippet = resp_text[:300].replace("\n", " ")
                if "membership" in resp_text.lower():
                    log.error(
                        "HRB account does not have a valid membership. "
                        "CSV download requires an active HorseRaceBase "
                        "subscription. Response: " + snippet
                    )
                else:
                    log.warning(
                        f"CSV download returned HTML (not CSV): {snippet}"
                    )
                continue

            if not resp_text or resp_text.count("\n") < 1:
                log.warning(
                    f"CSV download returned empty data for {target_date}."
                )
                continue

            content = resp_text
            break

        if content is None:
            log.error(
                f"All CSV download attempts failed for {target_date}. "
                "If you haven't set HRB_USER_ID, find your numeric user "
                "ID on HorseRaceBase (check your profile/account page or "
                "the URL when you download results) and add it as a "
                "GitHub secret named HRB_USER_ID."
            )
            return None

        try:
            df = pd.read_csv(io.StringIO(content))
        except Exception as e:
            log.error(
                f"Failed to parse CSV response: {e}. "
                f"Content starts with: {content[:200]}"
            )
            return None

        log.info(f"Downloaded {len(df)} rows from HorseRaceBase")

        if len(df) == 0:
            log.warning(f"CSV parsed but contained 0 rows for {target_date}")
            return None

        log.info(f"  Columns: {list(df.columns)}")

        # Validate that essential columns exist (guards against HTML/error
        # responses that pandas may parse as a single-column DataFrame)
        required_cols = {"racedate", "track", "horse_name"}
        missing = required_cols - set(df.columns)
        if missing:
            log.error(
                f"CSV is missing required columns: {missing}. "
                f"Actual columns: {list(df.columns)}. "
                "This usually means the HRB account lacks a valid "
                "membership or the download URL has changed."
            )
            return None

        return _transform_hrb_data(df)

    except Exception as e:
        log.error(f"HRB fetch failed: {e}", exc_info=True)
        return None


def _transform_hrb_data(df):
    """
    Transform HorseRaceBase CSV columns to match our pipeline format.

    HRB Column          → Pipeline Column
    racedate            → meetingDate
    track               → courseName (UPPER CASE)
    Yards + RailMove    → distanceYards → distance (actual furlongs)
    Dist_Furlongs       → distanceNominal (catalogue distance)
    going_description   → going
    surfacetype         → raceSurfaceName (Polytrack→AW, else→Turf)
    race_class          → raceClass (parse "Class 5" → 5)
    placing_numerical   → positionOfficial
    horse_name          → horseName
    horse_age           → horseAge
    HorseSex            → horseGender
    pounds              → weightCarried
    comptime_numeric    → finishingTime
    TotalDstBt          → distanceCumulative
    CardNo              → raceNumber
    stall               → draw
    official_rating     → officialRating
    MedianOR            → medianOR
    MaxORinRace         → maxOR
    """
    log.info("Transforming HRB data...")

    out = pd.DataFrame()
    # Normalize date format to YYYY-MM-DD (HRB can use DD/MM/YYYY or YYYY-MM-DD)
    out["meetingDate"] = pd.to_datetime(
        df["racedate"], dayfirst=True, format="mixed", errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    out["courseName"] = df["track"].str.strip().str.upper().map(
        lambda c: HRB_TO_TIMEFORM_COURSE.get(c, c)
    )
    # racetime is the unique race identifier (e.g. "5.30."); CardNo is the
    # horse's card/stall number.  Create sequential race numbers per course
    # from the ordered race times.
    df["_racetime"] = df["racetime"]
    race_order = (
        df.groupby(["racedate", "track"])["_racetime"]
        .transform(lambda s: pd.Categorical(s).codes + 1)
    )
    out["raceNumber"] = race_order
    out["raceTime"] = df["racetime"]

    # Distance: use Yards + RailMove for actual race distance
    yards = pd.to_numeric(df["Yards"], errors="coerce")
    rail_move = pd.to_numeric(df["RailMove"], errors="coerce").fillna(0)
    out["distanceYards"] = yards + rail_move
    out["distance"] = out["distanceYards"] / 220.0
    out["distanceNominal"] = pd.to_numeric(df["Dist_Furlongs"], errors="coerce")

    out["going"] = df["going_description"]
    out["raceClass"] = df["race_class"].astype(str).str.extract(r"(\d+)")[0]
    out["raceClass"] = pd.to_numeric(out["raceClass"], errors="coerce")
    out["numberOfRunners"] = pd.to_numeric(df["number_of_runners"], errors="coerce")
    out["horseName"] = df["horse_name"]
    out["horseAge"] = pd.to_numeric(df["horse_age"], errors="coerce")
    out["weightCarried"] = pd.to_numeric(df["pounds"], errors="coerce")
    out["finishingTime"] = pd.to_numeric(df["comptime_numeric"], errors="coerce")
    out["distanceCumulative"] = pd.to_numeric(df["TotalDstBt"], errors="coerce")

    # Position: parse numeric, mark non-finishers as NaN
    # (needed early for TotalDstBt reconstruction below)
    out["positionOfficial"] = pd.to_numeric(
        df["placing_numerical"], errors="coerce"
    )

    # Reconstruct cumulative beaten distance from individual distbt when
    # TotalDstBt is missing.  HRB sometimes publishes individual margins
    # before cumulative distances are available.
    if "distbt" in df.columns:
        def _parse_distbt(val):
            if pd.isna(val):
                return 0.0
            val = str(val).strip()
            if val in BEATEN_DIST_CODES:
                return BEATEN_DIST_CODES[val]
            try:
                return float(val)
            except ValueError:
                return 0.0

        needs_rebuild = (
            out["distanceCumulative"].isna()
            & out["positionOfficial"].notna()
            & (out["positionOfficial"] > 1)
        )
        if needs_rebuild.any():
            log.info(
                f"  Rebuilding TotalDstBt from distbt for "
                f"{needs_rebuild.sum()} runners"
            )
            parsed = df["distbt"].apply(_parse_distbt)
            # Group by race (racedate + track + racetime) and cumsum
            race_key = df["racedate"].astype(str) + "_" + df["track"].astype(str) + "_" + df["racetime"].astype(str)
            cumulative = parsed.groupby(race_key).cumsum()
            # Only fill where TotalDstBt is missing AND horse finished
            out.loc[needs_rebuild, "distanceCumulative"] = cumulative.loc[needs_rebuild]

    out["draw"] = pd.to_numeric(df["stall"], errors="coerce")
    out["odds"] = pd.to_numeric(df["odds"], errors="coerce")
    out["race_name"] = df["race_name"]

    # Official ratings
    out["officialRating"] = pd.to_numeric(df["official_rating"], errors="coerce")
    out["medianOR"] = pd.to_numeric(df["MedianOR"], errors="coerce")
    out["maxOR"] = pd.to_numeric(df["MaxORinRace"], errors="coerce")

    # Surface: map AW surfaces, everything else is Turf
    out["raceSurfaceName"] = df["surfacetype"].apply(
        lambda s: "All Weather" if s in AW_SURFACE_TYPES else "Turf"
    )

    # Gender: map to pipeline codes
    gender_map = {"Filly": "f", "Mare": "m", "Gelding": "g", "Colt": "c", "Horse": "h"}
    out["horseGender"] = df["HorseSex"].map(gender_map).fillna("g")

    # Race type for filtering
    out["_raceType"] = df["RaceType"]

    # Filter to flat racing only (exclude hurdles, chases, NH flat)
    # Use both the explicit set AND keyword matching for robustness
    _rt_lower = out["_raceType"].astype(str).str.lower()
    is_nh = out["_raceType"].isin(NH_RACE_TYPES) | _rt_lower.str.contains(
        r"hurdle|chase|bumper|nh flat|national hunt|n\.?h\.?\s",
        regex=True, na=False,
    )
    n_before = len(out)
    if is_nh.sum() < n_before:
        # Log which race types survived the filter
        surviving_types = out.loc[~is_nh, "_raceType"].unique()
        log.info(f"  Flat race types: {sorted(surviving_types)}")
    out = out[~is_nh].copy()
    log.info(f"  Flat filter: {n_before} → {len(out)} runners (excluded {is_nh.sum()} NH)")

    # Filter to UK/IRE courses
    all_courses = UK_COURSES | IRE_COURSES
    known = out["courseName"].isin(all_courses)
    if not known.all():
        unknown = out.loc[~known, "courseName"].unique()
        log.warning(f"  Unknown courses (excluded): {list(unknown)}")
        out = out[known].copy()

    # Drop the temporary column
    out = out.drop(columns=["_raceType"])

    # ── Determine meeting completeness ────────────────────────────────
    # Going allowance requires all races at a track to be complete, so
    # we must detect whether every scheduled flat race at each course on
    # this date has results.  This check happens BEFORE filtering out
    # unrun races so we can compare total vs completed counts.
    race_key = (
        out["meetingDate"].astype(str) + "_"
        + out["courseName"].astype(str) + "_"
        + out["raceTime"].astype(str)
    )
    course_date = (
        out["meetingDate"].astype(str) + "_"
        + out["courseName"].astype(str)
    )
    # A race has "run" if at least one runner has finishingTime > 0
    # and a valid positionOfficial
    has_result = (
        (out["finishingTime"] > 0) & out["positionOfficial"].notna()
    )
    races_with_results = set(race_key[has_result])
    race_has_run = race_key.isin(races_with_results)

    # Count total scheduled flat races vs completed per course/date
    race_info = pd.DataFrame({
        "course_date": course_date,
        "race_key": race_key,
        "has_run": race_has_run,
    }).drop_duplicates(subset="race_key")
    total_per_cd = race_info.groupby("course_date")["race_key"].count()
    completed_per_cd = race_info[race_info["has_run"]].groupby(
        "course_date"
    )["race_key"].count()
    completed_per_cd = completed_per_cd.reindex(
        total_per_cd.index, fill_value=0
    )
    complete_meetings = set(
        total_per_cd[total_per_cd == completed_per_cd].index
    )

    # Mark each runner with whether their meeting is complete
    out["meeting_complete"] = course_date.isin(complete_meetings)

    # Log incomplete meetings
    incomplete_cd = total_per_cd.index.difference(complete_meetings)
    for cd in incomplete_cd:
        log.info(
            f"  Meeting {cd}: {int(completed_per_cd[cd])}/{int(total_per_cd[cd])} "
            f"races complete — will defer ratings"
        )

    # ── Filter out unrun races ────────────────────────────────────────
    # At the 6pm run, later races may appear in HRB with no results:
    # comptime_numeric=0, no placing, no beaten distances.
    n_unrun = (~race_has_run).sum()
    if n_unrun > 0:
        unrun_races = race_key[~race_has_run].unique()
        log.info(
            f"  Excluded {n_unrun} runners from {len(unrun_races)} "
            f"unrun race(s)"
        )
        out = out[race_has_run].copy()

    # Warn if finishers are missing beaten distances (data quality)
    finishers_no_beaten = (
        out["positionOfficial"].notna()
        & (out["positionOfficial"] > 1)
        & out["distanceCumulative"].isna()
    )
    if finishers_no_beaten.any():
        log.warning(
            f"  {finishers_no_beaten.sum()} finishers missing beaten "
            f"distance — figures will be approximate"
        )

    log.info(
        f"  Final: {len(out)} runners across "
        f"{out['courseName'].nunique()} courses"
    )
    return out


HISTORIC_DIR = ROOT_DIR / "data" / "historic"


def _load_manual_csv(target_date):
    """
    Load results from a manually-placed CSV file.

    Search order:
      1. data/live/<date>.csv  or  data/live/today.csv
      2. data/historic/results_<date>.csv  (zero-padded and non-padded)
    """
    LIVE_DIR.mkdir(parents=True, exist_ok=True)

    # Build candidate paths in priority order
    candidates = [
        LIVE_DIR / f"{target_date}.csv",
        LIVE_DIR / "today.csv",
    ]

    # Historic folder: try both zero-padded (2026-02-18) and
    # non-padded (2026-2-18) since HRB exports use non-padded dates
    candidates.append(HISTORIC_DIR / f"results_{target_date}.csv")
    try:
        d = datetime.strptime(target_date, "%Y-%m-%d")
        alt_date = f"{d.year}-{d.month}-{d.day}"
        if alt_date != target_date:
            candidates.append(HISTORIC_DIR / f"results_{alt_date}.csv")
    except ValueError:
        pass

    for path in candidates:
        if path.exists():
            log.info(f"Loading manual CSV: {path}")
            df = pd.read_csv(path, low_memory=False)
            # If it looks like HRB format, transform it
            if "track" in df.columns and "comptime_numeric" in df.columns:
                return _transform_hrb_data(df)
            # Otherwise assume it's already in pipeline format
            return df

    return None


# ═════════════════════════════════════════════════════════════════════
# LITE RATING ENGINE
# ═════════════════════════════════════════════════════════════════════

class LiteRatingEngine:
    """
    Computes speed figures using pre-built lookup tables from the full
    pipeline.  This is a streamlined version that:

    - Uses pre-computed standard times (not re-derived)
    - Estimates going allowance from going description, or computes it
      live if enough results are available
    - Applies WFA and surface-specific calibration
    """

    def __init__(self):
        self.std_times = {}
        self.lpl_dict = {}
        self.cal_params = {}
        self._artifacts = None
        self._loaded = False

    def load_lookup_tables(self):
        """Load pre-computed tables from the full pipeline output."""
        log.info("Loading lookup tables...")

        # 1. Standard times
        std_path = OUTPUT_DIR / "standard_times.csv"
        if not std_path.exists():
            raise FileNotFoundError(
                f"Standard times not found at {std_path}. "
                "Run the full pipeline first: python src/speed_figures.py"
            )
        std_df = pd.read_csv(std_path)
        self.std_times = dict(zip(std_df["std_key"], std_df["median_time"]))
        log.info(f"  Standard times: {len(self.std_times)} combos")

        # 2. Compute LPL from standard times
        self._compute_lpl(std_df)
        log.info(f"  Lbs-per-length: {len(self.lpl_dict)} combos")

        # 3. Calibration parameters
        self._fit_calibration()

        self._loaded = True

    def _compute_lpl(self, std_df):
        """Compute course-specific lbs-per-length from standard times."""
        std_df = std_df.copy()
        std_df["spf"] = std_df["median_time"] / std_df["distance"]
        std_df["dist_band"] = std_df["distance"].round(0)
        mean_spf = std_df.groupby("dist_band")["spf"].mean()
        std_df["mean_spf"] = std_df["dist_band"].map(mean_spf)
        std_df["correction"] = std_df["mean_spf"] / std_df["spf"]
        std_df["generic_lpl"] = std_df["distance"].apply(generic_lbs_per_length)
        std_df["surf_mult"] = std_df["surface"].map(LPL_SURFACE_MULTIPLIER).fillna(1.0)
        std_df["course_lpl"] = (
            std_df["generic_lpl"] * std_df["correction"] * std_df["surf_mult"]
        )
        self.lpl_dict = dict(zip(std_df["std_key"], std_df["course_lpl"]))

    def _fit_calibration(self):
        """Load calibration from batch pipeline artifacts, or fit simple linear."""
        # Try loading full calibration artifacts first
        artifact_path = OUTPUT_DIR / "calibration_artifacts.pkl"
        if artifact_path.exists():
            import pickle
            log.info(f"  Loading calibration artifacts from {artifact_path}")
            with open(artifact_path, "rb") as f:
                self._artifacts = pickle.load(f)
            surfaces = list(self._artifacts.get("cal_params", {}).keys())
            log.info(f"  Full calibration chain: {surfaces}")
            if self._artifacts.get("gbr_models"):
                log.info(
                    f"  GBR models: "
                    f"{list(self._artifacts['gbr_models'].keys())}"
                )
            if self._artifacts.get("qm_params"):
                log.info(
                    f"  QM params: "
                    f"{list(self._artifacts['qm_params'].keys())}"
                )
            return

        # Fallback: simple linear calibration from speed_figures.csv
        log.warning("No calibration artifacts — falling back to simple linear")
        fig_path = OUTPUT_DIR / "speed_figures.csv"
        if not fig_path.exists():
            log.warning("No speed_figures.csv — using default calibration")
            self.cal_params = {
                "Turf": (0.77, 23.0),
                "All Weather": (0.89, 11.0),
            }
            return

        log.info("  Fitting calibration from pipeline output...")
        cols = [
            "timefigure", "figure_final", "raceSurfaceName", "source_year",
        ]
        df = pd.read_csv(fig_path, usecols=cols, low_memory=False)

        mask = (
            df["timefigure"].notna()
            & (df["timefigure"] != 0)
            & df["timefigure"].between(-200, 200)
            & df["figure_final"].notna()
            & (df["source_year"] <= 2023)
        )

        for surface in df["raceSurfaceName"].dropna().unique():
            fit = df[mask & (df["raceSurfaceName"] == surface)]
            if len(fit) < 100:
                continue
            x = fit["figure_final"].values
            y = fit["timefigure"].values
            A = np.vstack([x, np.ones(len(x))]).T
            (a, b), *_ = np.linalg.lstsq(A, y, rcond=None)
            self.cal_params[surface] = (a, b)
            log.info(f"    {surface}: cal = {a:.4f}x + {b:.2f}")

    def compute_figures(self, df):
        """
        Compute speed figures for today's results.

        Returns the input DataFrame with added columns:
          raw_figure, wfa_adj, figure_final, figure_calibrated
        """
        if not self._loaded:
            self.load_lookup_tables()

        df = self._prepare_data(df)
        df = self._estimate_going_allowances(df)
        df = self._compute_winner_figures(df)
        df = self._extend_to_all_runners(df)
        df = self._apply_weight_adjustment(df)
        df = self._apply_wfa(df)
        df = self._compute_sex_allowance(df)
        df = self._apply_calibration(df)
        df = self._apply_gbr(df)
        df = self._apply_quantile_mapping(df)
        df = self._apply_oos_corrections(df)

        # Diagnostic: how much the figure-based rank diverges from finish position.
        # Positive = figure ranks higher than finishing position (weight boost).
        # Large values flag counter-intuitive ordering for manual QA review.
        fig_rank = df.groupby("race_id")["figure_calibrated"].rank(
            ascending=False, na_option="bottom"
        )
        df["rank_vs_position"] = df["positionOfficial"] - fig_rank

        return df

    def _prepare_data(self, df):
        """Build pipeline keys and coerce types."""
        df = df.copy()

        for col in [
            "finishingTime", "distance", "positionOfficial",
            "distanceCumulative", "horseAge", "weightCarried",
            "raceClass", "raceNumber",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Round distance to nearest 0.5f for std_key lookup
        # Use actual yards (includes rail movements) when available
        if "distanceYards" in df.columns and df["distanceYards"].notna().any():
            df["dist_round"] = (df["distanceYards"] / 110).round() * 110 / 220
        else:
            df["dist_round"] = (df["distance"] * 2).round(0) / 2

        # Default surface for known AW-only courses
        if "raceSurfaceName" not in df.columns:
            df["raceSurfaceName"] = "Turf"

        # Build keys
        df["race_id"] = (
            df["meetingDate"].astype(str) + "_"
            + df["courseName"].astype(str) + "_"
            + df["raceNumber"].astype(str)
        )
        df["meeting_id"] = (
            df["meetingDate"].astype(str) + "_"
            + df["courseName"].astype(str) + "_"
            + df["raceSurfaceName"].astype(str)
        )
        df["std_key"] = (
            df["courseName"].astype(str) + "_"
            + df["dist_round"].astype(str) + "_"
            + df["raceSurfaceName"].astype(str)
        )

        df["month"] = pd.to_datetime(
            df["meetingDate"], errors="coerce", format="%Y-%m-%d"
        ).dt.month

        return df

    def _estimate_going_allowances(self, df):
        """
        Estimate going allowance for each meeting.

        If >=3 winners have known finishing times and standard times,
        compute a real going allowance with:
          - Per-meeting outlier removal (z-score based)
          - Winsorized median (consistent with pipeline)
          - Bayesian shrinkage toward going-description prior
          - Non-linear correction for extreme going
        Otherwise fall back to an estimate derived from going description.
        """
        from speed_figures import (
            GA_OUTLIER_ZSCORE, GA_SHRINKAGE_K, GA_NONLINEAR_THRESHOLD,
            GA_NONLINEAR_BETA, GOING_GA_PRIOR, IRE_COURSES,
        )

        log.info("Estimating going allowances...")

        meetings = df.groupby("meeting_id").first()[
            ["going", "courseName", "raceSurfaceName"]
        ]
        ga_dict = {}

        # Try to compute real GA from today's results
        winners = df[df["positionOfficial"] == 1].copy()
        winners["standard_time"] = interpolate_lookup(winners, self.std_times)
        winners = winners[
            winners["finishingTime"].notna()
            & (winners["finishingTime"] > 0)
            & winners["standard_time"].notna()
        ].copy()

        if len(winners) > 0:
            winners["class_adj"] = winners.apply(
                lambda r: compute_class_adjustment(
                    r.get("raceClass", 4), r["distance"]
                ),
                axis=1,
            )
            winners["adj_time"] = (
                winners["finishingTime"] - winners["class_adj"]
            )
            winners["dev_per_furlong"] = (
                (winners["adj_time"] - winners["standard_time"])
                / winners["distance"]
            )

            for mid, group in winners.groupby("meeting_id"):
                if len(group) >= 3:
                    vals = group["dev_per_furlong"].sort_values().values.copy()
                    n = len(vals)

                    # Per-meeting z-score outlier removal
                    if n > 2:
                        med = np.median(vals)
                        std = np.std(vals, ddof=1)
                        if std > 0:
                            z = np.abs((vals - med) / std)
                            vals = vals[z <= GA_OUTLIER_ZSCORE]
                            n = len(vals)

                    if n < 3:
                        continue

                    # Winsorized median
                    if n > 2:
                        vals[0] = vals[1]
                        vals[-1] = vals[-2]
                    raw_ga = float(np.median(vals))

                    # Bayesian shrinkage toward going-description prior
                    going_desc = meetings.loc[mid, "going"] if mid in meetings.index else "Good"
                    if pd.isna(going_desc):
                        going_desc = "Good"
                    prior_ga = GOING_GA_PRIOR.get(going_desc, 0.0)
                    course = meetings.loc[mid, "courseName"] if mid in meetings.index else ""
                    k = GA_SHRINKAGE_K / 2.0 if course in IRE_COURSES else GA_SHRINKAGE_K
                    ga = (n * raw_ga + k * prior_ga) / (n + k)

                    # Non-linear correction for extreme going
                    abs_ga = abs(ga)
                    if abs_ga > GA_NONLINEAR_THRESHOLD:
                        sign = 1.0 if ga > 0 else -1.0
                        excess = abs_ga - GA_NONLINEAR_THRESHOLD
                        ga += sign * GA_NONLINEAR_BETA * excess ** 2

                    ga_dict[mid] = ga
                    log.info(
                        f"  {mid}: computed GA = {ga:+.3f} s/f "
                        f"({len(group)} winners, raw={raw_ga:+.3f})"
                    )

        # Fill remaining meetings with estimated GA
        for mid, row in meetings.iterrows():
            if mid not in ga_dict:
                going = row["going"] if pd.notna(row["going"]) else "Good"
                ga = GOING_GA_ESTIMATES.get(going, 0.0)
                ga_dict[mid] = ga
                log.info(
                    f"  {mid}: estimated GA = {ga:+.3f} s/f (going: {going})"
                )

        df["going_allowance"] = df["meeting_id"].map(ga_dict).fillna(0.0)
        self._ga_dict = ga_dict
        return df

    def _compute_winner_figures(self, df):
        """Compute raw speed figures for race winners."""
        log.info("Computing winner figures...")

        # Diagnostic: explain which condition(s) fail
        all_winners = df[df["positionOfficial"] == 1]
        n_winners = len(all_winners)
        if n_winners == 0:
            log.warning(
                "  No runners with positionOfficial==1 found. "
                "Unique positions: "
                f"{sorted(df['positionOfficial'].dropna().unique()[:10].tolist())}"
            )
        else:
            has_time = all_winners["finishingTime"].notna() & (
                all_winners["finishingTime"] > 0
            )
            # Check interpolation coverage instead of exact key match
            interp_std = interpolate_lookup(all_winners, self.std_times)
            has_std = interp_std.notna()
            log.info(
                f"  {n_winners} winners: "
                f"{has_time.sum()} with finishingTime, "
                f"{has_std.sum()} with standard time (interpolated)"
            )
            if has_time.sum() == 0:
                log.info(
                    "  finishingTime values (first 5 winners): "
                    f"{all_winners['finishingTime'].head().tolist()}"
                )
            if has_std.sum() == 0:
                # Show what keys exist in std_times for these courses
                for crs in all_winners["courseName"].unique():
                    avail = [
                        k for k in self.std_times if k.startswith(crs + "_")
                    ]
                    log.info(
                        f"  Standard times for {crs}: "
                        f"{avail[:5]} ({len(avail)} total)"
                    )

        w = df[
            (df["positionOfficial"] == 1)
            & df["finishingTime"].notna()
            & (df["finishingTime"] > 0)
        ].copy()

        # Interpolate standard times to actual distances
        w["standard_time"] = interpolate_lookup(w, self.std_times)
        w = w[w["standard_time"].notna()].copy()

        if len(w) == 0:
            log.warning("No winners with valid data — cannot compute figures")
            self._winner_figs = {}
            return df

        w["corrected_time"] = (
            w["finishingTime"] - (w["going_allowance"] * w["distance"])
        )
        w["deviation_seconds"] = w["corrected_time"] - w["standard_time"]
        w["deviation_lengths"] = w["deviation_seconds"] / SECONDS_PER_LENGTH

        # Course-specific LPL interpolated to actual distance
        w["lpl"] = interpolate_lookup(w, self.lpl_dict)
        missing_lpl = w["lpl"].isna()
        if missing_lpl.any():
            w.loc[missing_lpl, "lpl"] = w.loc[
                missing_lpl, "distance"
            ].apply(generic_lbs_per_length)

        w["deviation_lbs"] = w["deviation_lengths"] * w["lpl"]
        w["winner_raw_figure"] = BASE_RATING - w["deviation_lbs"]

        self._winner_figs = dict(zip(w["race_id"], w["winner_raw_figure"]))
        log.info(f"  Winner figures: {len(self._winner_figs)} races")
        if self._winner_figs:
            vals = list(self._winner_figs.values())
            log.info(f"  Range: {min(vals):.0f} to {max(vals):.0f}")

        return df

    def _extend_to_all_runners(self, df):
        """Extend figures to all runners via beaten lengths."""
        log.info("Extending to all runners...")

        df["raw_figure"] = np.nan
        df["standard_time"] = np.nan
        df["est_time"] = np.nan

        in_race = df["race_id"].isin(self._winner_figs)
        df_in = df[in_race].copy()

        if len(df_in) == 0:
            return df

        df_in["winner_figure"] = df_in["race_id"].map(self._winner_figs)

        # Standard time interpolated to actual distance
        df_in["standard_time"] = interpolate_lookup(df_in, self.std_times)

        # LPL interpolated to actual distance
        df_in["lpl"] = interpolate_lookup(df_in, self.lpl_dict)
        missing = df_in["lpl"].isna()
        if missing.any():
            df_in.loc[missing, "lpl"] = df_in.loc[
                missing, "distance"
            ].apply(generic_lbs_per_length)

        # Beaten lengths (capped at 30)
        is_winner = df_in["positionOfficial"] == 1
        cum = df_in["distanceCumulative"].fillna(0).clip(lower=0, upper=30)

        df_in["lbs_behind"] = cum * df_in["lpl"]
        df_in.loc[is_winner, "lbs_behind"] = 0.0
        df_in["raw_figure"] = df_in["winner_figure"] - df_in["lbs_behind"]

        # Non-finishers (no position or position == 0) should not receive a figure
        no_pos = df_in["positionOfficial"].isna() | (df_in["positionOfficial"] == 0)
        df_in.loc[no_pos, "raw_figure"] = np.nan

        # Estimated finish times:
        #   Winner = actual comptime (finishingTime)
        #   Others = winner_time + cumulative_beaten_lengths * SECONDS_PER_LENGTH
        winner_times = (
            df_in.loc[is_winner, ["race_id", "finishingTime"]]
            .drop_duplicates("race_id")
            .set_index("race_id")["finishingTime"]
        )
        df_in["winner_time"] = df_in["race_id"].map(winner_times)
        df_in["est_time"] = df_in["winner_time"]
        non_winner = ~is_winner & df_in["distanceCumulative"].notna()
        df_in.loc[non_winner, "est_time"] = (
            df_in.loc[non_winner, "winner_time"]
            + df_in.loc[non_winner, "distanceCumulative"] * SECONDS_PER_LENGTH
        )

        # Write back
        df.loc[df_in.index, "raw_figure"] = df_in["raw_figure"]
        df.loc[df_in.index, "standard_time"] = df_in["standard_time"]
        df.loc[df_in.index, "est_time"] = df_in["est_time"]
        log.info(f"  All-runner figures: {df['raw_figure'].notna().sum()}")
        return df

    def _apply_weight_adjustment(self, df):
        """
        Adjust for weight carried (matching training pipeline Stage 6).

        figure += (weightCarried - BASE_WEIGHT_LBS)

        A horse carrying more than 9st 0lb (126 lbs) gets a positive
        adjustment — it achieved its time despite the extra burden.
        """
        log.info("Applying weight adjustment...")

        df["weight_adj"] = 0.0
        has_w = df["weightCarried"].notna() & df["raw_figure"].notna()
        if has_w.any():
            df.loc[has_w, "weight_adj"] = (
                df.loc[has_w, "weightCarried"] - BASE_WEIGHT_LBS
            )
            df["raw_figure"] = df["raw_figure"] + df["weight_adj"]
            log.info(
                f"  {has_w.sum()} runners adjusted "
                f"(base={BASE_WEIGHT_LBS} lbs, "
                f"range: {df.loc[has_w, 'weight_adj'].min():+.0f} to "
                f"{df.loc[has_w, 'weight_adj'].max():+.0f})"
            )

        return df

    def _apply_wfa(self, df):
        """Apply weight-for-age adjustment."""
        log.info("Applying WFA...")

        df["wfa_adj"] = df.apply(
            lambda r: get_wfa_allowance(
                r.get("horseAge"), r.get("month"), r.get("distance"),
                r.get("raceSurfaceName"),
            ),
            axis=1,
        )
        df["figure_final"] = df["raw_figure"] + df["wfa_adj"]

        has_wfa = (df["wfa_adj"] > 0) & df["raw_figure"].notna()
        if has_wfa.any():
            log.info(f"  WFA applied to {has_wfa.sum()} runners")

        return df

    def _compute_sex_allowance(self, df):
        """Compute sex allowance indicator for fillies/mares."""
        log.info("Computing sex allowance...")

        def _sex_alw(row):
            if row.get("horseGender") not in FEMALE_GENDERS:
                return 0
            m = row.get("month", 1)
            if 5 <= m <= 9:
                return SEX_ALLOWANCE_SUMMER
            return SEX_ALLOWANCE_WINTER

        df["sex_allowance"] = df.apply(_sex_alw, axis=1)
        n = (df["sex_allowance"] > 0).sum()
        if n > 0:
            log.info(f"  Sex allowance flagged for {n} runners")
        return df

    def _apply_calibration(self, df):
        """Apply calibration — full chain if artifacts available, else linear."""
        log.info("Applying calibration...")

        if self._artifacts and self._artifacts.get("cal_params"):
            df = self._apply_full_calibration(df)
        else:
            df = self._apply_simple_calibration(df)

        df["figure_calibrated"] = df["figure_calibrated"].round(1)

        # Exclude runners with no finish position (pulled up, fell, etc.)
        no_position = (
            (df["positionOfficial"].isna() | (df["positionOfficial"] == 0))
            & df["figure_calibrated"].notna()
        )
        n_no_pos = no_position.sum()
        if n_no_pos > 0:
            df.loc[no_position, "figure_calibrated"] = np.nan
            log.info(f"  Excluded {n_no_pos} non-finishers (no position)")

        # Flag runners beaten > 20 lengths as low confidence (but keep figures).
        # The batch pipeline uses soft attenuation (0.5x beyond 20L) and never
        # nulls finishers.  Previously this code hard-excluded them, which
        # contradicted the batch pipeline and destroyed valid data.
        beaten_far = (
            df["distanceCumulative"].notna()
            & (df["distanceCumulative"] > 20)
            & (df["positionOfficial"] != 1)
        )
        if "figure_confidence" not in df.columns:
            df["figure_confidence"] = "high"
        df.loc[beaten_far, "figure_confidence"] = "low"
        n_flagged = beaten_far.sum()
        if n_flagged > 0:
            log.info(f"  Flagged {n_flagged} runners beaten > 20 lengths as low confidence")

        return df

    def _apply_simple_calibration(self, df):
        """Fallback: simple linear calibration."""
        df["figure_calibrated"] = df["figure_final"]
        for surface, (a, b) in self.cal_params.items():
            mask = (
                (df["raceSurfaceName"] == surface) & df["figure_final"].notna()
            )
            df.loc[mask, "figure_calibrated"] = (
                a * df.loc[mask, "figure_final"] + b
            )
            n = mask.sum()
            if n > 0:
                log.info(
                    f"  {surface}: {n} runners, simple cal = {a:.3f}x + {b:.1f}"
                )
        return df

    def _apply_full_calibration(self, df):
        """Apply the full calibration chain from batch pipeline artifacts."""
        cal = self._artifacts["cal_params"]
        df["figure_calibrated"] = np.nan

        for surface, params in cal.items():
            mask = (
                (df["raceSurfaceName"] == surface) & df["figure_final"].notna()
            )
            if mask.sum() == 0:
                continue

            x = df.loc[mask, "figure_final"].values.astype(float)
            a = params["a"]
            b = params["b"]
            a2 = params["a2"]
            x_mean = params["x_mean"]

            # Quadratic calibration
            if a2 != 0:
                x_c = x - x_mean
                cal_vals = a * x + a2 * x_c ** 2 + b
            else:
                cal_vals = a * x + b

            # Class offsets
            class_offsets = params.get("class_offsets", {})
            if class_offsets:
                rc = (
                    pd.to_numeric(
                        df.loc[mask, "raceClass"], errors="coerce"
                    )
                    .fillna(0)
                    .astype(int)
                    .astype(str)
                )
                cal_vals += rc.map(class_offsets).fillna(0).values

            # Course x distance offsets
            cd_offsets = params.get("course_dist_offsets", {})
            if cd_offsets:
                cd_key = (
                    df.loc[mask, "courseName"]
                    + "_"
                    + df.loc[mask, "distance"].round(0).astype(int).astype(str)
                )
                cal_vals += cd_key.map(cd_offsets).fillna(0).values

            # Going group offsets
            going_offsets = params.get("going_offsets", {})
            if going_offsets:
                going_grp = (
                    df.loc[mask, "going"].map(GOING_MAP).fillna("Good")
                )
                cal_vals += going_grp.map(going_offsets).fillna(0).values

            # Continuous GA coefficient
            ga_coeff = params.get("ga_coeff", 0.0)
            if ga_coeff != 0 and "going_allowance" in df.columns:
                ga_vals = df.loc[mask, "going_allowance"].fillna(0).values
                cal_vals += ga_coeff * ga_vals

            # Beaten-length band offsets
            bl_offsets = params.get("bl_offsets", {})
            if bl_offsets:
                bl = (
                    df.loc[mask, "distanceCumulative"]
                    .fillna(0)
                    .clip(lower=0)
                )
                bl_band = pd.cut(
                    bl,
                    bins=[0, 1, 3, 5, 10, 15, 20],
                    labels=["0-1", "1-3", "3-5", "5-10", "10-15", "15-20"],
                ).astype(str).fillna("0-1")
                bl_band = bl_band.where(
                    df.loc[mask, "positionOfficial"] != 1, "winner"
                )
                cal_vals += bl_band.map(bl_offsets).fillna(0).values

            # Age offsets
            age_offsets = params.get("age_offsets", {})
            if age_offsets and "horseAge" in df.columns:
                age = (
                    df.loc[mask, "horseAge"]
                    .clip(upper=12)
                    .astype(int)
                    .astype(str)
                )
                cal_vals += age.map(age_offsets).fillna(0).values

            df.loc[mask, "figure_calibrated"] = cal_vals
            n = mask.sum()
            log.info(
                f"  {surface}: {n} runners, full calibration chain "
                f"(a={a:.4f}, b={b:.2f}, a2={a2:.6f}, "
                f"{len(class_offsets)} class, "
                f"{len(cd_offsets)} course×dist, "
                f"{len(going_offsets)} going, "
                f"ga_coeff={ga_coeff:+.2f}, "
                f"{len(bl_offsets)} bl, "
                f"{len(age_offsets)} age offsets)"
            )

        return df

    def _apply_gbr(self, df):
        """Apply pre-trained GBR models from batch pipeline."""
        if not self._artifacts or not self._artifacts.get("gbr_models"):
            return df

        log.info("Applying GBR enhancement...")
        gbr_models = self._artifacts["gbr_models"]
        course_freq = self._artifacts.get("course_freq", {})

        GBR_FEATURES = [
            "figure_calibrated", "figure_final", "raceClass",
            "distance", "horseAge", "positionOfficial",
            "weightCarried", "ga_value", "going_num", "course_freq",
            "numberOfRunners", "draw",
        ]

        # Temporary feature columns
        df["going_num"] = df["going"].map(GOING_ORDINAL).fillna(3)
        df["course_freq"] = df["courseName"].map(course_freq).fillna(0)
        df["ga_value"] = df["going_allowance"].fillna(0)

        for surface, gbr in gbr_models.items():
            mask = (
                (df["raceSurfaceName"] == surface)
                & df["figure_calibrated"].notna()
            )
            if mask.sum() == 0:
                continue

            sub = df.loc[mask, GBR_FEATURES].copy()
            for col in GBR_FEATURES:
                if col not in sub.columns:
                    sub[col] = 0
                sub[col] = pd.to_numeric(sub[col], errors="coerce").fillna(0)

            # Cap age at 4 to match training
            sub["horseAge"] = sub["horseAge"].clip(upper=4)

            preds = gbr.predict(sub.values)
            df.loc[mask, "figure_calibrated"] = preds
            log.info(f"  {surface}: GBR applied to {mask.sum()} runners")

        df.drop(
            columns=["going_num", "course_freq", "ga_value"],
            errors="ignore",
            inplace=True,
        )
        return df

    def _apply_quantile_mapping(self, df):
        """Apply pre-computed quantile mapping from batch pipeline."""
        if not self._artifacts or not self._artifacts.get("qm_params"):
            return df

        from scipy.interpolate import PchipInterpolator

        log.info("Applying quantile mapping...")

        for surface, qm in self._artifacts["qm_params"].items():
            mask = (
                (df["raceSurfaceName"] == surface)
                & df["figure_calibrated"].notna()
            )
            if mask.sum() == 0:
                continue

            pred_q = np.array(qm["pred_quantiles"])
            tf_q = np.array(qm["tf_quantiles"])
            mapper = PchipInterpolator(pred_q, tf_q, extrapolate=True)

            x = df.loc[mask, "figure_calibrated"].values.astype(float)
            df.loc[mask, "figure_calibrated"] = mapper(x)
            log.info(f"  {surface}: QM applied to {mask.sum()} runners")

        df["figure_calibrated"] = df["figure_calibrated"].round(1)
        return df

    def _apply_oos_corrections(self, df):
        """
        Apply post-hoc OOS corrections from batch pipeline.

        These corrections address three systematic bias sources:
        1. Distance-specific bias (certain dist × surface combos)
        2. Going-group residual bias (e.g. Good/Firm over-rated)
        3. Temporal drift (calibration trained on older data)
        """
        if not self._artifacts or not self._artifacts.get("oos_corrections"):
            return df

        log.info("Applying OOS corrections...")

        corrections = self._artifacts["oos_corrections"]

        for surface, params in corrections.items():
            mask = (
                (df["raceSurfaceName"] == surface)
                & df["figure_calibrated"].notna()
            )
            if mask.sum() == 0:
                continue

            total_adj = np.zeros(mask.sum())

            # Distance correction
            dist_corr = params.get("dist_corrections", {})
            if dist_corr:
                dist_round = df.loc[mask, "distance"].round(0).astype(int)
                dist_adj = dist_round.map(dist_corr).fillna(0).values
                total_adj += dist_adj

            # Going correction
            going_corr = params.get("going_corrections", {})
            if going_corr:
                going_grp = df.loc[mask, "going"].map(GOING_MAP).fillna("Good")
                going_adj = going_grp.map(going_corr).fillna(0).values
                total_adj += going_adj

            # Temporal correction (always applied for live data)
            temporal = params.get("temporal_offset", 0.0)
            total_adj += temporal

            df.loc[mask, "figure_calibrated"] -= total_adj

            n_adj = (np.abs(total_adj) > 0.01).sum()
            log.info(
                f"  {surface}: OOS corrections applied to {n_adj} runners "
                f"(temporal={temporal:+.2f})"
            )

        df["figure_calibrated"] = df["figure_calibrated"].round(1)
        return df


# ═════════════════════════════════════════════════════════════════════
# EMAIL PUBLISHER
# ═════════════════════════════════════════════════════════════════════

def format_email_html(df, target_date, run_time):
    """Format the ratings as a styled HTML email."""
    # Sort with non-finishers (pos 0 or NaN) at the bottom of each race
    df = df.copy()
    df["_sort_pos"] = df["positionOfficial"].where(
        df["positionOfficial"].notna() & (df["positionOfficial"] > 0), other=9999
    )
    df = df.sort_values(["courseName", "raceNumber", "_sort_pos"])
    df = df.drop(columns=["_sort_pos"])

    css = """
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; color: #333;
               max-width: 800px; margin: 0 auto; padding: 10px; }
        h1 { color: #1a3a5c; border-bottom: 3px solid #c8102e;
             padding-bottom: 10px; }
        h2 { color: #1a3a5c; margin-top: 30px; }
        h3 { color: #555; margin-top: 20px;
             border-left: 4px solid #c8102e; padding-left: 10px; }
        table { border-collapse: collapse; width: 100%;
                margin: 10px 0; font-size: 14px; }
        th { background: #1a3a5c; color: white;
             padding: 8px 12px; text-align: left; }
        td { padding: 6px 12px; border-bottom: 1px solid #ddd; }
        tr:nth-child(even) { background: #f8f9fa; }
        .high { color: #c8102e; font-weight: bold; }
        .good { color: #1a5c3a; font-weight: bold; }
        .avg  { color: #555; }
        .box  { background: #f0f4f8; border: 1px solid #d0d8e0;
                border-radius: 8px; padding: 15px; margin: 15px 0; }
        .meta { color: #666; font-size: 13px; margin-bottom: 5px; }
        .note { color: #888; font-size: 12px; font-style: italic; }
        .foot { color: #888; font-size: 12px; margin-top: 30px;
                border-top: 1px solid #ddd; padding-top: 10px; }
    </style>
    """

    html = f"""<html><head>{css}</head><body>
    <h1>Live Speed Figures &mdash; {target_date}</h1>
    <p>Generated at {run_time} GMT</p>
    """

    # ── Top performers ──
    rated = df[df["figure_calibrated"].notna() & (df["figure_calibrated"] >= 0)]
    top = rated.nlargest(10, "figure_calibrated")
    if len(top) > 0:
        html += '<div class="box"><h2>Top Performers</h2><table>'
        html += (
            "<tr><th>#</th><th>Horse</th><th>Course</th>"
            "<th>Race</th><th>Pos</th><th>Figure</th></tr>"
        )
        for i, (_, r) in enumerate(top.iterrows(), 1):
            fig = r["figure_calibrated"]
            cls = _fig_class(fig)
            html += (
                f'<tr><td>{i}</td><td><b>{r.get("horseName", "?")}</b></td>'
                f'<td>{r.get("courseName", "")}</td>'
                f'<td>R{int(r.get("raceNumber", 0))}</td>'
                f'<td>{int(r.get("positionOfficial", 0))}</td>'
                f'<td class="{cls}">{fig:.0f}</td></tr>'
            )
        html += "</table></div>"

    # ── Race-by-race breakdown ──
    html += "<h2>Full Results</h2>"

    for (course, race_num), race_df in df.groupby(
        ["courseName", "raceNumber"], sort=True
    ):
        first = race_df.iloc[0]
        dist = first.get("distance", "?")
        dist_yards = first.get("distanceYards", "?")
        going = first.get("going", "?")
        surface = first.get("raceSurfaceName", "?")
        rc = first.get("raceClass", "?")
        ga = first.get("going_allowance", 0)
        name = first.get("race_name", "")
        median_or = first.get("medianOR", 0)
        max_or = first.get("maxOR", 0)

        # Format distance: show yards if available
        if pd.notna(dist_yards) and dist_yards > 0:
            dist_str = f"{int(dist_yards)}y ({dist:.1f}f)"
        elif pd.notna(dist):
            dist_str = f"{dist}f"
        else:
            dist_str = "?"

        html += f"<h3>{course} &mdash; Race {int(race_num)}</h3>"
        if name:
            html += f'<p class="meta"><em>{name}</em></p>'
        html += (
            f'<p class="meta">{dist_str} &middot; {going} &middot; '
            f'{surface} &middot; Class {rc}</p>'
        )
        # OR info line
        or_parts = []
        if pd.notna(median_or) and median_or > 0:
            or_parts.append(f"Median OR: {median_or:.0f}")
        if pd.notna(max_or) and max_or > 0:
            or_parts.append(f"Max OR: {max_or:.0f}")
        or_str = " &middot; ".join(or_parts) if or_parts else ""
        if or_str:
            html += f'<p class="note">{or_str} &middot; Going allowance: {ga:+.3f} s/f</p>'
        else:
            html += f'<p class="note">Going allowance: {ga:+.3f} s/f</p>'

        html += (
            "<table><tr><th>Pos</th><th>Horse</th><th>Age</th>"
            "<th>Wgt</th><th>OR</th><th>Beaten</th><th>Time</th>"
            "<th>Figure</th></tr>"
        )

        for _, r in race_df.iterrows():
            pos = (
                int(r["positionOfficial"])
                if pd.notna(r.get("positionOfficial"))
                and r["positionOfficial"] > 0
                else "-"
            )
            horse = r.get("horseName", "?")
            age = (
                int(r["horseAge"])
                if pd.notna(r.get("horseAge"))
                else "?"
            )
            wgt = (
                f'{int(r["weightCarried"])}'
                if pd.notna(r.get("weightCarried"))
                else "-"
            )
            or_val = (
                f'{int(r["officialRating"])}'
                if pd.notna(r.get("officialRating"))
                and r.get("officialRating", 0) > 0
                else "-"
            )
            beaten = (
                f'{r["distanceCumulative"]:.2f}'
                if pd.notna(r.get("distanceCumulative"))
                and r.get("distanceCumulative", 0) > 0
                else "-"
            )
            est_time = (
                f'{r["est_time"]:.2f}'
                if pd.notna(r.get("est_time"))
                else "-"
            )
            fig = r.get("figure_calibrated")
            if pd.notna(fig) and fig >= 0:
                fig_str = f"{fig:.0f}"
                cls = _fig_class(fig)
            else:
                fig_str = "-"
                cls = ""

            html += (
                f"<tr><td>{pos}</td><td><b>{horse}</b></td>"
                f"<td>{age}</td><td>{wgt}</td><td>{or_val}</td>"
                f"<td>{beaten}</td>"
                f"<td>{est_time}</td>"
                f'<td class="{cls}">{fig_str}</td></tr>'
            )

        html += "</table>"

    # ── Footer ──
    total = rated.shape[0]
    races = df["race_id"].nunique() if "race_id" in df.columns else "?"
    html += f"""
    <div class="foot">
        <p>{total} runners rated across {races} races</p>
        <p>Figures calibrated to Timeform scale using {len(rated['courseName'].unique()) if len(rated) > 0 else 0}
           course standard times from 2015&ndash;2026 dataset.</p>
        <p>Racing Speed Figures &mdash; Live Ratings Engine</p>
    </div></body></html>
    """
    return html


def _fig_class(fig):
    """CSS class name for a figure value."""
    if pd.isna(fig):
        return ""
    if fig >= 110:
        return "high"
    if fig >= 85:
        return "good"
    return "avg"


def send_email(html, target_date, run_time, recipients=RECIPIENTS):
    """Send the ratings email via SMTP."""
    log.info(
        f"Email config: SMTP_USER={'set' if SMTP_USER else 'EMPTY'}, "
        f"SMTP_PASS={'set' if SMTP_PASS else 'EMPTY'}, "
        f"host={SMTP_HOST}:{SMTP_PORT}, recipients={recipients}"
    )

    if not SMTP_USER or not SMTP_PASS:
        log.error(
            "Email not configured. Set SMTP_USER and SMTP_PASS env vars. "
            "For Gmail, use an App Password "
            "(Google Account → Security → 2FA → App Passwords)."
        )
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"Live Speed Figures — {target_date} ({run_time} GMT)"
    )
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    text = (
        f"Live Speed Figures for {target_date}. "
        "View this email in HTML format for full results."
    )
    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        log.info(f"Connecting to {SMTP_HOST}:{SMTP_PORT}...")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            log.info(f"Authenticating as {SMTP_USER}...")
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, recipients, msg.as_string())
        log.info(f"Email sent successfully to {recipients}")
        return True
    except smtplib.SMTPAuthenticationError as e:
        log.error(
            f"SMTP authentication failed: {e}. "
            "Check that SMTP_USER is a valid Gmail address and "
            "SMTP_PASS is a 16-character Gmail App Password "
            "(NOT your regular Google password). "
            "Generate one at: Google Account → Security → "
            "2-Step Verification → App Passwords."
        )
        return False
    except Exception as e:
        log.error(f"Failed to send email: {e}", exc_info=True)
        return False


# ═════════════════════════════════════════════════════════════════════
# SCHEDULER
# ═════════════════════════════════════════════════════════════════════

SCHEDULE_HOURS = [18, 22]  # 6pm and 10pm GMT


def run_scheduled():
    """
    Run on a schedule: 6pm and 10pm GMT daily.

    For production, prefer using cron instead (see below).
    """
    log.info(f"Scheduler started. Will run at {SCHEDULE_HOURS} GMT")

    while True:
        now = datetime.now(timezone.utc)

        next_runs = []
        for hour in SCHEDULE_HOURS:
            candidate = now.replace(
                hour=hour, minute=0, second=0, microsecond=0
            )
            if candidate <= now:
                candidate += timedelta(days=1)
            next_runs.append(candidate)

        next_run = min(next_runs)
        wait_seconds = (next_run - now).total_seconds()

        log.info(
            f"Next run at {next_run.strftime('%Y-%m-%d %H:%M')} GMT "
            f"(in {wait_seconds / 3600:.1f} hours)"
        )
        time.sleep(wait_seconds)

        log.info("=== Scheduled run starting ===")
        try:
            run_once(date.today().isoformat())
        except Exception as e:
            log.error(f"Scheduled run failed: {e}", exc_info=True)


# ═════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════

def run_once(target_date=None, send_email_flag=True):
    """Run the live ratings pipeline once."""
    if target_date is None:
        target_date = date.today().isoformat()

    run_time = datetime.now(timezone.utc).strftime("%H:%M")

    log.info("=" * 60)
    log.info(f"LIVE RATINGS — {target_date} ({run_time} GMT)")
    log.info("=" * 60)

    # 1. Fetch results
    df = fetch_results(target_date)
    if df is None or len(df) == 0:
        log.error("No results to process")
        return None

    log.info(
        f"Results: {len(df)} runners across "
        f"{df['courseName'].nunique()} courses"
    )

    # 1b. Exclude incomplete meetings — going allowance requires all
    #     races at a track to be finished before we can rate any of them.
    if "meeting_complete" in df.columns:
        incomplete = df[~df["meeting_complete"]]
        if len(incomplete) > 0:
            incomplete_courses = sorted(incomplete["courseName"].unique())
            log.info(
                f"Deferring {len(incomplete)} runners from "
                f"{len(incomplete_courses)} incomplete meeting(s): "
                f"{incomplete_courses}"
            )
            df = df[df["meeting_complete"]].copy()

        if len(df) == 0:
            log.info("No complete meetings to rate — all deferred")
            return None

    # 2. Compute figures
    engine = LiteRatingEngine()
    engine.load_lookup_tables()
    df = engine.compute_figures(df)

    rated = df["figure_calibrated"].notna().sum()
    log.info(f"Rated: {rated}/{len(df)} runners")

    if rated > 0:
        top = df.nlargest(5, "figure_calibrated")
        log.info("Top 5:")
        for _, r in top.iterrows():
            log.info(
                f"  {r['horseName']:25s}  {r['courseName']:15s}  "
                f"R{int(r['raceNumber'])}  "
                f"fig={r['figure_calibrated']:.0f}"
            )

    # 3. Save to CSV
    LIVE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LIVE_DIR / f"ratings_{target_date}.csv"
    df.to_csv(out_path, index=False)
    log.info(f"Saved: {out_path}")

    # 3b. Save xlsx archive (committed to repo)
    xlsx_dir = ROOT_DIR / "output" / "daily_ratings"
    xlsx_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = xlsx_dir / f"uk_ratings_{target_date}.xlsx"

    XLSX_COLS = [
        "raceDate", "courseName", "raceNumber", "raceName",
        "distance", "going", "raceSurfaceName", "raceClass",
        "positionOfficial", "horseName", "horseAge", "jockeyName",
        "trainerName", "weightCarried", "officialRating",
        "distanceCumulative", "finishingTime", "figure_calibrated",
        "figure_confidence", "rank_vs_position",
    ]
    export_cols = [c for c in XLSX_COLS if c in df.columns]
    export_df = df[export_cols].copy()
    export_df = export_df.sort_values(
        ["courseName", "raceNumber", "positionOfficial"]
    )
    export_df.to_excel(xlsx_path, index=False, sheet_name="Ratings")
    log.info(f"XLSX saved: {xlsx_path}")

    # 4. Format and send email
    html = format_email_html(df, target_date, run_time)

    html_path = LIVE_DIR / f"ratings_{target_date}.html"
    with open(html_path, "w") as f:
        f.write(html)
    log.info(f"HTML saved: {html_path}")

    if send_email_flag:
        send_email(html, target_date, run_time)

    log.info("Done!")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Live Racing Ratings — Same-Day Speed Figures",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Compute figures only, don't send email",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run on schedule (6pm and 10pm GMT daily)",
    )
    args = parser.parse_args()

    if args.schedule:
        run_scheduled()
    else:
        run_once(
            target_date=args.date,
            send_email_flag=not args.no_email,
        )


if __name__ == "__main__":
    main()
