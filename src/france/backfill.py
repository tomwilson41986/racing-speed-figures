"""
Historical backfill: scrape PMU flat race results day-by-day and store in DB.

Designed to be idempotent — skips dates/races already ingested.
Supports resuming from the last successful date.

Typical throughput: ~1 req/sec, ~3 requests per race day (programme + N
participant pages).  Full 2015–2026 backfill ≈ 12 hours.
"""

import datetime
import json
import logging
import random
import time
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from .database import MeetingRow, RaceRow, RunnerRow
from .pmu_client import PMUClient, temps_obtenu_to_seconds

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jittered_sleep(base: float = 1.0):
    """Sleep with random jitter (0.5× to 1.5× base)."""
    time.sleep(base * random.uniform(0.5, 1.5))


def _extract_hippodrome(data: dict) -> tuple[str, str]:
    """Return (code, name) from a reunion or course dict, tolerating varied shapes."""
    hippo = data.get("hippodrome", {})
    if isinstance(hippo, dict):
        code = hippo.get("codeHippodrome", hippo.get("code", hippo.get("libelleCourt", "")))
        name = hippo.get("libelleLong", hippo.get("libelleCourt", ""))
    else:
        code = str(hippo) if hippo else ""
        name = code
    return code, name


def _extract_country(reunion: dict) -> str:
    pays = reunion.get("pays", {})
    if isinstance(pays, dict):
        return pays.get("code", "")
    return str(pays) if pays else ""


def _extract_going(course: dict) -> str:
    """Extract going description from penetrometre or terrain fields."""
    pen = course.get("penetrometre")
    if isinstance(pen, dict):
        return pen.get("intitule", "")
    return course.get("terrain", "")


def _extract_beaten_distance(participant: dict) -> Optional[str]:
    """Extract beaten distance string from distanceChevalPrecedent."""
    dcp = participant.get("distanceChevalPrecedent")
    if isinstance(dcp, dict):
        return dcp.get("libelleCourt")
    return participant.get("ecart")


def _extract_odds(participant: dict) -> Optional[float]:
    """Extract starting price from dernierRapportDirect."""
    rapport = participant.get("dernierRapportDirect")
    if isinstance(rapport, dict):
        return rapport.get("rapport")
    return participant.get("coteDirect")


def _extract_weight_kg(participant: dict) -> Optional[float]:
    """Convert handicapPoids (hectograms) to kg, or fall back to poids."""
    hp = participant.get("handicapPoids")
    if hp is not None:
        return hp / 10.0
    for field in ("poids", "poidsConditionMonte"):
        val = participant.get(field)
        if val is not None:
            return float(val)
    return None


# ---------------------------------------------------------------------------
# Core ingestion for a single race
# ---------------------------------------------------------------------------

def _ingest_race(
    client: PMUClient,
    session: Session,
    meeting_row: MeetingRow,
    race_date: datetime.date,
    reunion_num: int,
    course: dict,
) -> Optional[RaceRow]:
    """Fetch participants for one race and write to DB.  Returns the RaceRow or None."""
    course_num = course.get("numOrdre", 0)

    # Skip if already ingested
    existing = session.execute(
        select(RaceRow).where(
            RaceRow.meeting_id == meeting_row.id,
            RaceRow.course_num == course_num,
        )
    ).scalar_one_or_none()
    if existing is not None:
        log.debug("Race R%sC%s already in DB, skipping.", reunion_num, course_num)
        return existing

    hippo_code = meeting_row.hippodrome_code or ""
    distance = course.get("distance")
    log.info(
        "Processing %s R%sC%s (%s %sm)...",
        race_date.isoformat(), reunion_num, course_num,
        hippo_code, distance,
    )

    _jittered_sleep()
    # Use raw JSON (not parsed Runner models) for flexible field access
    participants_url = client._build_url(
        race_date, f"R{reunion_num}", f"C{course_num}", "participants"
    )
    raw_data = client._fetch_json(participants_url)
    participants_data = raw_data.get("participants", []) if raw_data else None

    # dureeCourse is winner time in milliseconds at course level
    duree_ms = course.get("dureeCourse")
    winner_time_s: Optional[float] = None
    if duree_ms and duree_ms > 0:
        winner_time_s = duree_ms / 1000.0

    going = _extract_going(course)

    race_row = RaceRow(
        meeting_id=meeting_row.id,
        course_num=course_num,
        race_name=course.get("libelle", ""),
        distance_m=distance,
        discipline=course.get("discipline"),
        specialite=course.get("specialite"),
        prize_money=course.get("montantPrix"),
        going=going,
        parcours=course.get("parcours"),
        corde=course.get("corde"),
        num_starters=course.get("nombreDeclaresPartants"),
        winner_time_s=winner_time_s,
    )
    session.add(race_row)
    session.flush()  # get race_row.id

    if participants_data is None:
        log.warning("No participants returned for R%sC%s.", reunion_num, course_num)
        return race_row

    for p in participants_data:
        # Individual runner times: tempsObtenu may or may not be present
        raw_temps = p.get("tempsObtenu")
        time_s: Optional[float] = None
        if raw_temps and raw_temps > 0:
            time_s = temps_obtenu_to_seconds(raw_temps)

        finish_pos = p.get("ordreArrivee")

        runner = RunnerRow(
            race_id=race_row.id,
            num_pmu=p.get("numPmu"),
            horse_name=p.get("nom", ""),
            age=p.get("age"),
            sex=p.get("sexe"),
            finish_position=finish_pos,
            temps_obtenu=raw_temps,
            time_seconds=time_s,
            beaten_lengths=_extract_beaten_distance(p),
            weight_kg=_extract_weight_kg(p),
            jockey=p.get("driver", p.get("jockey", "")),
            trainer=p.get("entraineur"),
            sire=p.get("nomPere"),
            dam=p.get("nomMere"),
            odds=_extract_odds(p),
            raw_json=json.dumps(p, ensure_ascii=False),
        )
        session.add(runner)

    return race_row


# ---------------------------------------------------------------------------
# Single-day ingestion
# ---------------------------------------------------------------------------

def ingest_day(
    client: PMUClient,
    session: Session,
    race_date: datetime.date,
) -> int:
    """Fetch and store all PLAT races for a single date.

    Returns the number of races ingested (new ones only).
    """
    # Check if any meeting already exists for this date (quick skip)
    date_str = race_date.strftime("%d%m%Y")
    programme = client.get_programme(race_date)
    if programme is None:
        log.warning("No programme for %s, skipping.", race_date.isoformat())
        return 0

    reunions = programme.get("programme", {}).get("reunions", [])
    if not reunions:
        # Some responses nest differently
        reunions = programme.get("reunions", [])

    races_ingested = 0

    for reunion in reunions:
        reunion_num = reunion.get("numOfficiel")
        if reunion_num is None:
            continue

        # Check which courses are PLAT
        flat_courses = [
            c for c in reunion.get("courses", [])
            if c.get("discipline") == "PLAT"
        ]
        if not flat_courses:
            continue

        # Ensure meeting row exists
        existing_meeting = session.execute(
            select(MeetingRow).where(
                MeetingRow.race_date == race_date,
                MeetingRow.reunion_num == reunion_num,
            )
        ).scalar_one_or_none()

        if existing_meeting is None:
            hippo_code, hippo_name = _extract_hippodrome(reunion)
            country = _extract_country(reunion)
            meeting_row = MeetingRow(
                race_date=race_date,
                reunion_num=reunion_num,
                hippodrome_code=hippo_code,
                hippodrome_name=hippo_name,
                country=country,
            )
            session.add(meeting_row)
            session.flush()
        else:
            meeting_row = existing_meeting

        for course in flat_courses:
            try:
                race_row = _ingest_race(
                    client, session, meeting_row, race_date, reunion_num, course,
                )
                if race_row is not None:
                    races_ingested += 1
            except Exception:
                log.exception(
                    "Error ingesting R%sC%s on %s — continuing.",
                    reunion_num, course.get("numOrdre"), race_date.isoformat(),
                )

    session.commit()
    return races_ingested


# ---------------------------------------------------------------------------
# Date-range backfill
# ---------------------------------------------------------------------------

def last_ingested_date(session: Session) -> Optional[datetime.date]:
    """Return the most recent race_date in the meetings table, or None."""
    result = session.execute(
        select(MeetingRow.race_date).order_by(MeetingRow.race_date.desc()).limit(1)
    ).scalar_one_or_none()
    return result


def _dates_by_year(
    start_date: datetime.date, end_date: datetime.date
) -> list[tuple[int, datetime.date, datetime.date]]:
    """Split a date range into (year, year_start, year_end) chunks."""
    chunks = []
    current_year = start_date.year
    while current_year <= end_date.year:
        y_start = max(start_date, datetime.date(current_year, 1, 1))
        y_end = min(end_date, datetime.date(current_year, 12, 31))
        chunks.append((current_year, y_start, y_end))
        current_year += 1
    return chunks


def backfill_date_range(
    start_date: datetime.date,
    end_date: datetime.date,
    session: Session,
    client: Optional[PMUClient] = None,
    resume: bool = True,
) -> dict:
    """Backfill all PLAT races from *start_date* to *end_date* inclusive.

    Parameters
    ----------
    start_date, end_date : date
        Inclusive date bounds.
    session : Session
        Active SQLAlchemy session.
    client : PMUClient, optional
        Reuses an existing client; creates one if not provided.
    resume : bool
        If True, skip dates up to the last ingested date in the DB.

    Returns
    -------
    dict  with keys: dates_processed, races_ingested, errors
    """
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    if client is None:
        client = PMUClient()

    # Resume support
    if resume:
        last = last_ingested_date(session)
        if last is not None and last >= start_date:
            start_date = last + datetime.timedelta(days=1)
            log.info("Resuming from %s (last ingested: %s)", start_date, last)

    if start_date > end_date:
        log.info("Nothing to backfill — already up to date.")
        return {"dates_processed": 0, "races_ingested": 0, "errors": 0}

    stats = {"dates_processed": 0, "races_ingested": 0, "errors": 0}
    year_chunks = _dates_by_year(start_date, end_date)
    total_years = len(year_chunks)

    for yi, (year, y_start, y_end) in enumerate(year_chunks, 1):
        year_days = (y_end - y_start).days + 1
        year_races = 0
        year_errors = 0

        if tqdm is not None:
            day_iter = tqdm(
                range(year_days),
                desc=f"{year} ({yi}/{total_years})",
                unit="day",
                leave=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} days [{elapsed}<{remaining}, {postfix}]",
            )
        else:
            day_iter = range(year_days)

        current = y_start
        for di in day_iter:
            try:
                n = ingest_day(client, session, current)
                stats["races_ingested"] += n
                year_races += n
            except Exception:
                log.exception("Failed to process %s — will continue.", current.isoformat())
                stats["errors"] += 1
                year_errors += 1
                session.rollback()

            stats["dates_processed"] += 1
            current += datetime.timedelta(days=1)

            if tqdm is not None:
                day_iter.set_postfix(
                    races=year_races, errors=year_errors, refresh=False
                )

            _jittered_sleep(0.3)

        log.info(
            "Year %d complete: %d races ingested, %d errors.",
            year, year_races, year_errors,
        )

    return stats
