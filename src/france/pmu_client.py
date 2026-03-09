"""
PMU Turfinfo API client for French flat horse racing data.

Base URL: https://online.turfinfo.api.pmu.fr/rest/client/61/programme/
Date format in URLs: DDMMYYYY

Endpoints:
    /{date}                                      → full day programme
    /{date}/R{n}                                 → reunion (meeting)
    /{date}/R{n}/C{m}/participants               → runners + results
    /{date}/R{n}/C{m}/performances-detaillees/pretty → past performances
"""

import datetime
import logging
import time
from typing import Optional

import requests
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = (
    "https://online.turfinfo.api.pmu.fr/rest/client/61/programme"
)

CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def temps_obtenu_to_seconds(value: int) -> float:
    """Convert PMU tempsObtenu (1/100ths of a second) to seconds.

    The raw value encodes minutes in the leading digits.
    E.g. 199220 → 1 min 59.22 s → 119.22 seconds.

    Encoding: MMSSss  where MM = minutes, SS = seconds, ss = hundredths.
    More precisely the value is  MM * 10000 + SS * 100 + ss.
    """
    hundredths = value % 100
    remainder = value // 100
    seconds = remainder % 100
    minutes = remainder // 100
    return minutes * 60.0 + seconds + hundredths / 100.0


def _format_date(date: datetime.date) -> str:
    """Format a date as DDMMYYYY for the PMU API."""
    return date.strftime("%d%m%Y")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class Runner(BaseModel):
    """A single runner (participant) in a race."""

    nom: str = ""
    numPmu: int = 0
    age: Optional[int] = None
    sexe: Optional[str] = None

    # Result fields (only populated for completed races)
    ordreArrivee: Optional[int] = None
    tempsObtenu: Optional[int] = None  # raw 1/100ths encoding
    reductionKilometrique: Optional[str] = None

    # Weight — poids for flat, poidsConditionMonte may appear instead
    poids: Optional[float] = None
    poidsConditionMonte: Optional[float] = None

    # Beaten distance — may be None for winner or non-placed
    ecart: Optional[str] = None

    # Connections
    driver: Optional[str] = None
    jockey: Optional[str] = None
    entraineur: Optional[str] = None

    # Form / ratings
    musique: Optional[str] = None
    gainsParticipant: Optional[dict] = None
    handicapDistance: Optional[int] = None

    @property
    def time_seconds(self) -> Optional[float]:
        """Return tempsObtenu converted to seconds, or None."""
        if self.tempsObtenu is not None and self.tempsObtenu > 0:
            return temps_obtenu_to_seconds(self.tempsObtenu)
        return None


class Race(BaseModel):
    """A single race (course) within a meeting."""

    numOrdre: int = 0
    libelle: str = ""
    discipline: Optional[str] = None
    distance: Optional[int] = None
    terrain: Optional[str] = None
    corde: Optional[str] = None
    participants: list[Runner] = Field(default_factory=list)


class RaceResult(BaseModel):
    """Lightweight result container for a completed race."""

    reunion_num: int
    course_num: int
    date: str  # DDMMYYYY
    runners: list[Runner] = Field(default_factory=list)


class Meeting(BaseModel):
    """A reunion / meeting on a given day."""

    numOfficiel: int = 0
    hippodrome: Optional[str] = None
    pays: Optional[str] = None
    courses: list[Race] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# PMU API Client
# ---------------------------------------------------------------------------

class PMUClient:
    """HTTP client for the PMU Turfinfo REST API.

    Parameters
    ----------
    base_url : str
        Root URL for the programme endpoint.
    delay_between_requests : float
        Minimum seconds to wait between consecutive requests (rate limit).
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        delay_between_requests: float = 1.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.delay = delay_between_requests
        self._last_request_time: float = 0.0

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": CHROME_USER_AGENT})
        self.session.params = {"specialisation": "INTERNET"}  # type: ignore[assignment]

    # ---- internal ---------------------------------------------------------

    def _rate_limit(self):
        """Sleep if necessary to respect the minimum delay."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    @retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get(self, url: str) -> requests.Response:
        """Perform a GET with retry logic."""
        self._rate_limit()
        log.debug("GET %s", url)
        self._last_request_time = time.monotonic()
        response = self.session.get(url, timeout=30)
        return response

    def _fetch_json(self, url: str) -> Optional[dict]:
        """Fetch JSON from *url*, returning None on non-200 or errors."""
        try:
            resp = self._get(url)
        except requests.exceptions.RequestException:
            log.error("Request failed after retries: %s", url)
            return None

        if resp.status_code != 200:
            log.error("HTTP %s for %s", resp.status_code, url)
            return None

        return resp.json()

    def _build_url(self, date: datetime.date, *parts: str) -> str:
        """Build an API URL from a date and path segments."""
        date_str = _format_date(date)
        segments = "/".join(parts)
        if segments:
            return f"{self.base_url}/{date_str}/{segments}"
        return f"{self.base_url}/{date_str}"

    # ---- public API -------------------------------------------------------

    def get_programme(self, date: datetime.date) -> Optional[dict]:
        """Fetch the full day programme (raw JSON dict).

        Returns None on failure.
        """
        url = self._build_url(date)
        return self._fetch_json(url)

    def get_reunion(self, date: datetime.date, reunion_num: int) -> Optional[Meeting]:
        """Fetch a single reunion/meeting and return as a Meeting model.

        Only PLAT (flat) races are included in the returned Meeting.
        """
        url = self._build_url(date, f"R{reunion_num}")
        data = self._fetch_json(url)
        if data is None:
            return None

        # Build Meeting from the reunion-level response
        reunion_data = data if "numOfficiel" in data else data.get("reunion", data)

        courses_raw = reunion_data.get("courses", [])
        flat_races = []
        for c in courses_raw:
            if c.get("discipline") == "PLAT":
                flat_races.append(
                    Race(
                        numOrdre=c.get("numOrdre", 0),
                        libelle=c.get("libelle", ""),
                        discipline=c.get("discipline"),
                        distance=c.get("distance"),
                        terrain=c.get("terrain"),
                        corde=c.get("corde"),
                    )
                )

        return Meeting(
            numOfficiel=reunion_data.get("numOfficiel", reunion_num),
            hippodrome=reunion_data.get("hippodrome", {}).get("libelleCourt")
            if isinstance(reunion_data.get("hippodrome"), dict)
            else reunion_data.get("hippodrome"),
            pays=reunion_data.get("pays", {}).get("code")
            if isinstance(reunion_data.get("pays"), dict)
            else reunion_data.get("pays"),
            courses=flat_races,
        )

    def get_participants(
        self,
        date: datetime.date,
        reunion_num: int,
        course_num: int,
    ) -> Optional[list[Runner]]:
        """Fetch runners/results for a specific race.

        Returns a list of Runner models or None on failure.
        """
        url = self._build_url(
            date, f"R{reunion_num}", f"C{course_num}", "participants"
        )
        data = self._fetch_json(url)
        if data is None:
            return None

        runners = []
        for p in data.get("participants", []):
            runners.append(
                Runner(
                    nom=p.get("nom", ""),
                    numPmu=p.get("numPmu", 0),
                    age=p.get("age"),
                    sexe=p.get("sexe"),
                    ordreArrivee=p.get("ordreArrivee"),
                    tempsObtenu=p.get("tempsObtenu"),
                    reductionKilometrique=p.get("reductionKilometrique"),
                    poids=p.get("poids"),
                    poidsConditionMonte=p.get("poidsConditionMonte"),
                    ecart=p.get("ecart"),
                    driver=p.get("driver"),
                    jockey=p.get("jockey"),
                    entraineur=p.get("entraineur"),
                    musique=p.get("musique"),
                    gainsParticipant=p.get("gainsParticipant"),
                    handicapDistance=p.get("handicapDistance"),
                )
            )

        return runners

    def get_performances(
        self,
        date: datetime.date,
        reunion_num: int,
        course_num: int,
    ) -> Optional[dict]:
        """Fetch past performances (raw JSON dict).

        Returns None on failure.
        """
        url = self._build_url(
            date,
            f"R{reunion_num}",
            f"C{course_num}",
            "performances-detaillees",
            "pretty",
        )
        return self._fetch_json(url)

    def get_flat_results(
        self,
        date: datetime.date,
    ) -> list[RaceResult]:
        """Convenience: fetch all PLAT race results for a given date.

        Returns a list of RaceResult, one per flat race that has participants.
        """
        programme = self.get_programme(date)
        if programme is None:
            return []

        results: list[RaceResult] = []
        reunions = programme.get("programme", {}).get("reunions", [])

        for reunion in reunions:
            reunion_num = reunion.get("numOfficiel")
            for course in reunion.get("courses", []):
                if course.get("discipline") != "PLAT":
                    continue
                course_num = course.get("numOrdre")
                runners = self.get_participants(date, reunion_num, course_num)
                if runners:
                    results.append(
                        RaceResult(
                            reunion_num=reunion_num,
                            course_num=course_num,
                            date=_format_date(date),
                            runners=runners,
                        )
                    )

        return results
