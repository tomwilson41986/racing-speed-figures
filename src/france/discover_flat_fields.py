"""
Discovery script: fetch a single PLAT (flat) race from the PMU Turfinfo API
and print ALL JSON keys at every nesting level.

Usage:
    python -m src.france.discover_flat_fields [DDMMYYYY]

If no date is supplied, defaults to yesterday.
"""

import datetime
import json
import logging
import sys
import time

import requests

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://online.turfinfo.api.pmu.fr/rest/client/61/programme"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}
PARAMS = {"specialisation": "INTERNET"}


def fetch(url: str) -> dict | None:
    """GET with basic error handling."""
    log.debug("GET %s", url)
    resp = requests.get(url, headers=HEADERS, params=PARAMS, timeout=30)
    if resp.status_code != 200:
        log.error("HTTP %s for %s", resp.status_code, url)
        return None
    return resp.json()


def dump_keys(obj, prefix: str = "", depth: int = 0):
    """Recursively print every key and its value type."""
    indent = "  " * depth
    if isinstance(obj, dict):
        for key, val in sorted(obj.items()):
            path = f"{prefix}.{key}" if prefix else key
            type_name = type(val).__name__
            if isinstance(val, dict):
                print(f"{indent}{path}  (dict, {len(val)} keys)")
                dump_keys(val, path, depth + 1)
            elif isinstance(val, list):
                print(f"{indent}{path}  (list, {len(val)} items)")
                if val:
                    dump_keys(val[0], f"{path}[0]", depth + 1)
            else:
                preview = repr(val)
                if len(preview) > 80:
                    preview = preview[:77] + "..."
                print(f"{indent}{path} = {preview}  ({type_name})")
    elif isinstance(obj, list):
        if obj:
            dump_keys(obj[0], f"{prefix}[0]", depth)
    else:
        preview = repr(obj)
        if len(preview) > 80:
            preview = preview[:77] + "..."
        print(f"{indent}{prefix} = {preview}  ({type(obj).__name__})")


def find_flat_race(programme: dict) -> tuple[int, int] | None:
    """Return (reunion_num, course_num) for the first PLAT race found."""
    for reunion in programme.get("programme", {}).get("reunions", []):
        reunion_num = reunion.get("numOfficiel")
        for course in reunion.get("courses", []):
            if course.get("discipline") == "PLAT":
                course_num = course.get("numOrdre")
                log.info(
                    "Found PLAT race: R%s C%s — %s",
                    reunion_num,
                    course_num,
                    course.get("libelle", ""),
                )
                return reunion_num, course_num
    return None


def main():
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        date_str = yesterday.strftime("%d%m%Y")

    log.info("Fetching programme for %s", date_str)

    # 1 — Day programme
    programme = fetch(f"{BASE_URL}/{date_str}")
    if programme is None:
        log.error("Could not fetch programme. Try a different date.")
        sys.exit(1)

    result = find_flat_race(programme)
    if result is None:
        log.error("No PLAT race found on %s. Try another date.", date_str)
        sys.exit(1)

    reunion_num, course_num = result
    time.sleep(1)

    # 2 — Participants / results
    participants_url = (
        f"{BASE_URL}/{date_str}/R{reunion_num}/C{course_num}/participants"
    )
    participants = fetch(participants_url)
    time.sleep(1)

    # 3 — Past performances
    perf_url = (
        f"{BASE_URL}/{date_str}/R{reunion_num}/C{course_num}"
        "/performances-detaillees/pretty"
    )
    performances = fetch(perf_url)

    # — Dump everything —
    print("\n" + "=" * 80)
    print("PARTICIPANTS / RESULTS KEYS")
    print("=" * 80)
    if participants:
        dump_keys(participants)
        # Also save raw JSON for offline inspection
        with open("flat_participants_sample.json", "w") as f:
            json.dump(participants, f, indent=2, ensure_ascii=False)
        log.info("Raw JSON saved to flat_participants_sample.json")
    else:
        print("(no data)")

    print("\n" + "=" * 80)
    print("PAST PERFORMANCES KEYS")
    print("=" * 80)
    if performances:
        dump_keys(performances)
        with open("flat_performances_sample.json", "w") as f:
            json.dump(performances, f, indent=2, ensure_ascii=False)
        log.info("Raw JSON saved to flat_performances_sample.json")
    else:
        print("(no data)")

    # — Quick summary of runner-level fields we care about —
    if participants:
        runners = participants.get("participants", [])
        if runners:
            print("\n" + "=" * 80)
            print(f"FIRST RUNNER — ALL TOP-LEVEL KEYS ({len(runners[0])} total)")
            print("=" * 80)
            for key in sorted(runners[0].keys()):
                val = runners[0][key]
                preview = repr(val)
                if len(preview) > 100:
                    preview = preview[:97] + "..."
                print(f"  {key}: {preview}")

            # Check for the specific fields we need
            fields_of_interest = [
                "nom", "numPmu", "age", "sexe", "ordreArrivee",
                "tempsObtenu", "reductionKilometrique", "poids",
                "poidsConditionMonte", "ecart", "driver", "jockey",
                "entraineur", "musique", "gainsParticipant",
                "handicapDistance", "terrain",
            ]
            print("\n" + "=" * 80)
            print("FIELDS OF INTEREST — PRESENCE CHECK")
            print("=" * 80)
            for field in fields_of_interest:
                present = field in runners[0]
                val = runners[0].get(field, "<MISSING>")
                preview = repr(val)
                if len(preview) > 80:
                    preview = preview[:77] + "..."
                status = "FOUND" if present else "MISSING"
                print(f"  [{status}] {field}: {preview}")


if __name__ == "__main__":
    main()
