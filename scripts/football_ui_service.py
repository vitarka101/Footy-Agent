from __future__ import annotations

import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import storage
from pydantic import BaseModel, Field

from scripts.football_web_fallback import build_web_fallback_bundle

try:
    from litellm import completion
except ImportError:  # pragma: no cover - optional during install/bootstrap
    completion = None

try:
    from agno.agent import Agent as AgnoAgent
    from agno.models.litellm import LiteLLM as AgnoLiteLLM
except ImportError:  # pragma: no cover - optional during install/bootstrap
    AgnoAgent = None
    AgnoLiteLLM = None

DEFAULT_DUCKDB_PATH = "football_data.duckdb"
DEFAULT_DUCKDB_GCS_URI = ""
MODEL = os.getenv("MODEL", "vertex_ai/gemini-2.5-flash-lite")
LITELLM_API_BASE = os.getenv("LITELLM_API_BASE", "").strip()
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "").strip()
MODEL_TIMEOUT_SECONDS = float(os.getenv("MODEL_TIMEOUT_SECONDS", "20"))
MAJOR_ENGLISH_LEAGUE_ORDER = [
    "Premier League",
    "Championship",
    "League 1",
    "League 2",
    "Conference",
    "Division 1",
    "Division 2",
    "Division 3",
]
PROMPT_CHIPS = [
    "How has home advantage changed over time in La Liga?",
    "Compare Spain leagues on goals and cards.",
    "What do the strongest metric correlations look like in Serie A?",
    "Which columns have the most missing data by season in the Premier League?",
    "Compare Bundesliga 1 and Serie A on scoring trends.",
    "Show the current MLS scoring profile.",
    "How complete is referee data in Ligue 1?",
    "Analyze the latest Japan J1 League trends.",
]
PLAYER_FACT_PATTERNS = (
    r"\b(?:which|what)\s+(?:league|team|club)\s+does\s+.+\s+play\b",
    r"\bwhere\s+does\s+.+\s+play\b",
    r"\bwho\s+does\s+.+\s+play\s+for\b",
    r"\bwhat\s+team\s+is\s+.+\s+on\b",
    r"\bwhich\s+club\s+is\s+.+\s+at\b",
)
UNSUPPORTED_GRAIN_TERMS = (
    "player",
    "manager",
    "coach",
    "transfer",
    "contract",
    "salary",
    "wages",
    "net worth",
    "position",
    "age",
    "nationality",
)
UNSUPPORTED_TEAM_FACT_TERMS = (
    "jersey",
    "shirt",
    "kit",
    "color",
    "colour",
    "badge",
    "crest",
    "mascot",
    "nickname",
    "stadium",
    "owner",
    "founded",
    "founded in",
    "captain",
)
DIRECT_FACT_TIME_SENSITIVE_TERMS = (
    "latest",
    "current",
    "today",
    "now",
    "this season",
    "most profitable",
    "revenue",
    "valuation",
    "worth",
    "price",
)
KNOWN_TEAM_FACTS = {
    "arsenal": {
        "jersey_color": "Arsenal's home jersey is traditionally red with white sleeves.",
    },
}
FOOTBALL_DOMAIN_TERMS = {
    "football",
    "soccer",
    "match",
    "matches",
    "goal",
    "goals",
    "league",
    "standings",
    "club",
    "team",
    "teams",
    "season",
    "seasons",
    "world cup",
    "fifa",
    "uefa",
    "premier league",
    "la liga",
    "serie a",
    "bundesliga",
    "champions league",
    "relegation",
    "promotion",
    "player",
    "players",
    "manager",
    "coach",
    "striker",
    "midfielder",
    "defender",
    "goalkeeper",
    "keeper",
    "transfer",
    "transfers",
    "assist",
    "assists",
    "clean sheet",
    "clean sheets",
    "xg",
    "expected goals",
    "fixture",
    "fixtures",
    "table",
    "form",
    "performance",
    "performing",
    "win rate",
    "loss rate",
    "draw",
    "draws",
    "concede",
    "conceded",
    "scored",
    "pressing",
    "possession",
    "offside",
    "offsides",
    "corner",
    "corners",
    "referee",
    "yellow card",
    "red card",
    "derby",
}
NON_FOOTBALL_HINTS = {
    "weather",
    "temperature",
    "recipe",
    "cook",
    "cooking",
    "bitcoin",
    "crypto",
    "stock",
    "stocks",
    "share price",
    "doctor",
    "medical",
    "medicine",
    "disease",
    "lawyer",
    "legal",
    "lawsuit",
    "movie",
    "movies",
    "netflix",
    "amazon product",
    "restaurant",
    "flight",
    "hotel",
    "chemistry",
    "physics",
    "biology",
}
EXTERNAL_FOOTBALL_HINTS = {
    "india": {"label": "India football", "query": "India football league national team results analysis"},
    "indian": {"label": "India football", "query": "India football league national team results analysis"},
    "indian super league": {"label": "Indian Super League", "query": "Indian Super League standings clubs football analysis"},
    "isl": {"label": "Indian Super League", "query": "Indian Super League standings clubs football analysis"},
    "a league": {"label": "A-League", "query": "A-League Australia football standings analysis"},
    "saudi pro league": {"label": "Saudi Pro League", "query": "Saudi Pro League football standings clubs analysis"},
    "fifa world cup": {"label": "FIFA World Cup", "query": "FIFA World Cup winners by country most titles"},
    "world cup": {"label": "FIFA World Cup", "query": "FIFA World Cup winners by country most titles"},
    "national team": {"label": "International football", "query": "international football national team records analysis"},
}
EXTERNAL_ONLY_FOOTBALL_HINTS = {
    "revenue": {"label": "Football league revenue", "query": "highest revenue association football league soccer"},
    "richest league": {"label": "Football league revenue", "query": "richest association football league soccer revenue"},
    "profit": {"label": "Football league profitability", "query": "most profitable association football league soccer"},
    "profitable": {"label": "Football league profitability", "query": "most profitable association football league soccer"},
    "earnings": {"label": "Football league profitability", "query": "association football league earnings profitability soccer"},
    "money": {"label": "Football finances", "query": "association football league finances revenue soccer"},
    "valuation": {"label": "Football valuation", "query": "most valuable football league clubs valuation"},
    "valuable": {"label": "Football valuation", "query": "most valuable football league clubs valuation"},
    "salary": {"label": "Football wages", "query": "football league wage bill salary analysis"},
    "wages": {"label": "Football wages", "query": "football league wage bill salary analysis"},
    "attendance": {"label": "Football attendance", "query": "football league attendance analysis"},
    "fanbase": {"label": "Football fanbase", "query": "largest football league fanbase analysis"},
    "broadcast": {"label": "Football broadcast rights", "query": "football league broadcast rights revenue"},
    "tv rights": {"label": "Football broadcast rights", "query": "football league broadcast rights revenue"},
    "ownership": {"label": "Football club ownership", "query": "football club ownership analysis"},
    "net worth": {"label": "Football finances", "query": "football club net worth league revenue analysis"},
}
PRIMARY_LEAGUES = {
    "England": "Premier League",
    "Scotland": "Premier League",
    "Germany": "Bundesliga 1",
    "Italy": "Serie A",
    "Spain": "La Liga Primera Division",
    "France": "Le Championnat",
    "Netherlands": "Eredivisie",
    "Belgium": "Jupiler League",
    "Portugal": "Liga I",
    "Turkey": "Futbol Ligi 1",
    "Greece": "Ethniki Katigoria",
    "Argentina": "Liga Profesional",
    "Austria": "Bundesliga",
    "Brazil": "Serie A",
    "China": "Super League",
    "Denmark": "Superliga",
    "Finland": "Veikkausliiga",
    "Ireland": "Premier Division",
    "Japan": "J1 League",
    "Mexico": "Liga MX",
    "Norway": "Eliteserien",
    "Poland": "Ekstraklasa",
    "Romania": "Superliga",
    "Russia": "Premier League",
    "Sweden": "Allsvenskan",
    "Switzerland": "Super League",
    "USA": "MLS",
}
EXTERNAL_VALIDATION_TIMEOUT_SECONDS = float(os.getenv("EXTERNAL_VALIDATION_TIMEOUT_SECONDS", "4"))
WIKIPEDIA_LEAGUE_URLS = {
    "Premier League": "https://en.wikipedia.org/wiki/Premier_League",
    "Championship": "https://en.wikipedia.org/wiki/EFL_Championship",
    "League 1": "https://en.wikipedia.org/wiki/EFL_League_One",
    "League 2": "https://en.wikipedia.org/wiki/EFL_League_Two",
    "Conference": "https://en.wikipedia.org/wiki/National_League",
    "Bundesliga 1": "https://en.wikipedia.org/wiki/Bundesliga",
    "Bundesliga 2": "https://en.wikipedia.org/wiki/2._Bundesliga",
    "Serie A": "https://en.wikipedia.org/wiki/Serie_A",
    "Serie B": "https://en.wikipedia.org/wiki/Serie_B",
    "La Liga Primera Division": "https://en.wikipedia.org/wiki/La_Liga",
    "La Liga Segunda Division": "https://en.wikipedia.org/wiki/Segunda_Divisi%C3%B3n",
    "Le Championnat": "https://en.wikipedia.org/wiki/Ligue_1",
    "Division 2": "https://en.wikipedia.org/wiki/Ligue_2",
    "Eredivisie": "https://en.wikipedia.org/wiki/Eredivisie",
    "Jupiler League": "https://en.wikipedia.org/wiki/Belgian_Pro_League",
    "Liga I": "https://en.wikipedia.org/wiki/Primeira_Liga",
    "Futbol Ligi 1": "https://en.wikipedia.org/wiki/S%C3%BCper_Lig",
    "Ethniki Katigoria": "https://en.wikipedia.org/wiki/Super_League_Greece",
    "MLS": "https://en.wikipedia.org/wiki/Major_League_Soccer",
    "J1 League": "https://en.wikipedia.org/wiki/J1_League",
}
FBREF_LEAGUE_URLS = {
    "Premier League": "https://fbref.com/en/comps/9/Premier-League-Stats",
    "Bundesliga 1": "https://fbref.com/en/comps/20/Bundesliga-Stats",
    "Serie A": "https://fbref.com/en/comps/11/Serie-A-Stats",
    "La Liga Primera Division": "https://fbref.com/en/comps/12/La-Liga-Stats",
    "Le Championnat": "https://fbref.com/en/comps/13/Ligue-1-Stats",
    "MLS": "https://fbref.com/en/comps/22/Major-League-Soccer-Stats",
}
COUNTRY_ALIASES = {
    "england": "England",
    "english": "England",
    "scotland": "Scotland",
    "scottish": "Scotland",
    "germany": "Germany",
    "german": "Germany",
    "italy": "Italy",
    "italian": "Italy",
    "spain": "Spain",
    "spanish": "Spain",
    "france": "France",
    "french": "France",
    "netherlands": "Netherlands",
    "dutch": "Netherlands",
    "holland": "Netherlands",
    "belgium": "Belgium",
    "belgian": "Belgium",
    "portugal": "Portugal",
    "portuguese": "Portugal",
    "turkey": "Turkey",
    "turkish": "Turkey",
    "greece": "Greece",
    "greek": "Greece",
    "argentina": "Argentina",
    "argentine": "Argentina",
    "austria": "Austria",
    "austrian": "Austria",
    "brazil": "Brazil",
    "brazilian": "Brazil",
    "china": "China",
    "chinese": "China",
    "denmark": "Denmark",
    "danish": "Denmark",
    "finland": "Finland",
    "finnish": "Finland",
    "ireland": "Ireland",
    "irish": "Ireland",
    "japan": "Japan",
    "japanese": "Japan",
    "mexico": "Mexico",
    "mexican": "Mexico",
    "norway": "Norway",
    "norwegian": "Norway",
    "poland": "Poland",
    "polish": "Poland",
    "romania": "Romania",
    "romanian": "Romania",
    "russia": "Russia",
    "russian": "Russia",
    "sweden": "Sweden",
    "swedish": "Sweden",
    "switzerland": "Switzerland",
    "swiss": "Switzerland",
    "usa": "USA",
    "us": "USA",
    "united states": "USA",
    "america": "USA",
}
LEAGUE_ALIASES = {
    "epl": ("England", "Premier League"),
    "premier league": (None, "Premier League"),
    "premiership": ("Scotland", "Premier League"),
    "scottish premiership": ("Scotland", "Premier League"),
    "english premier league": ("England", "Premier League"),
    "championship": ("England", "Championship"),
    "league one": ("England", "League 1"),
    "league 1": ("England", "League 1"),
    "league two": ("England", "League 2"),
    "league 2": ("England", "League 2"),
    "serie a": ("Italy", "Serie A"),
    "serie b": ("Italy", "Serie B"),
    "la liga": ("Spain", "La Liga Primera Division"),
    "laliga": ("Spain", "La Liga Primera Division"),
    "la liga primera": ("Spain", "La Liga Primera Division"),
    "la liga segunda": ("Spain", "La Liga Segunda Division"),
    "segunda division": ("Spain", "La Liga Segunda Division"),
    "la liga 2": ("Spain", "La Liga Segunda Division"),
    "bundesliga": ("Germany", "Bundesliga 1"),
    "bundesliga 1": ("Germany", "Bundesliga 1"),
    "bundesliga 2": ("Germany", "Bundesliga 2"),
    "austrian bundesliga": ("Austria", "Bundesliga"),
    "ligue 1": ("France", "Le Championnat"),
    "ligue one": ("France", "Le Championnat"),
    "ligue 2": ("France", "Division 2"),
    "eredivisie": ("Netherlands", "Eredivisie"),
    "primeira liga": ("Portugal", "Liga I"),
    "liga portugal": ("Portugal", "Liga I"),
    "liga i": ("Portugal", "Liga I"),
    "ligi 1": ("Turkey", "Futbol Ligi 1"),
    "jupiler league": ("Belgium", "Jupiler League"),
    "super league greece": ("Greece", "Ethniki Katigoria"),
    "liga mx": ("Mexico", "Liga MX"),
    "mls": ("USA", "MLS"),
    "major league soccer": ("USA", "MLS"),
    "argentina primera division": ("Argentina", "Liga Profesional"),
    "primera division argentina": ("Argentina", "Liga Profesional"),
    "brazil serie a": ("Brazil", "Serie A"),
    "brasileirao": ("Brazil", "Serie A"),
    "chinese super league": ("China", "Super League"),
    "danish superliga": ("Denmark", "Superliga"),
    "irish premier division": ("Ireland", "Premier Division"),
    "j league": ("Japan", "J1 League"),
    "j-league": ("Japan", "J1 League"),
    "j1 league": ("Japan", "J1 League"),
    "norwegian eliteserien": ("Norway", "Eliteserien"),
    "romanian liga 1": ("Romania", "Superliga"),
    "russian premier league": ("Russia", "Premier League"),
    "allsvenskan": ("Sweden", "Allsvenskan"),
    "swiss super league": ("Switzerland", "Super League"),
    "switzerland super league": ("Switzerland", "Super League"),
    "challenge league": ("Switzerland", "Challenge League"),
}
SPECIALIST_ORDER = ["aggregate", "segment", "correlation", "quality"]
SEASON_PATTERN = re.compile(r"\b(?:19|20)\d{2}(?:-(?:19|20)\d{2})?\b")
_DUCKDB_SYNC_LOCK = threading.Lock()
CASUAL_CONVERSATION_TERMS = {
    "hi",
    "hello",
    "hey",
    "yo",
    "sup",
    "how are you",
    "good morning",
    "good afternoon",
    "good evening",
    "thanks",
    "thank you",
}
RECENT_CONTEXT_MARKERS = (
    "here",
    "there",
    "this league",
    "that league",
    "same league",
    "this team",
    "that team",
    "same team",
    "this season",
    "that season",
    "same season",
    "current selection",
    "selected league",
    "selected team",
)
CLARIFICATION_PATTERNS = (
    r"^\s*i\s+meant\s+(?P<subject>.+?)\s*$",
    r"^\s*i\s+was\s+asking\s+about\s+(?P<subject>.+?)\s*$",
    r"^\s*i\s+am\s+asking\s+about\s+(?P<subject>.+?)\s*$",
    r"^\s*asking\s+about\s+(?P<subject>.+?)\s*$",
)
INTENT_OPTIONS = {
    "count_lookup",
    "team_recent_claim",
    "team_performance",
    "home_advantage",
    "correlation",
    "data_quality",
    "league_compare",
    "scoring",
    "overview",
}


class EdaPlannerDecision(BaseModel):
    next_step: Literal["trend", "segment", "correlation", "quality", "distribution", "stop"]
    reason: str = Field(default="")


class SpecialistDigest(BaseModel):
    claim: str = Field(default="")
    evidence_points: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)


def agno_available() -> bool:
    return AgnoAgent is not None and AgnoLiteLLM is not None


def llm_runtime_configured() -> bool:
    auth_hints = (
        LITELLM_API_KEY,
        os.getenv("OPENAI_API_KEY", "").strip(),
        os.getenv("ANTHROPIC_API_KEY", "").strip(),
        os.getenv("GEMINI_API_KEY", "").strip(),
        os.getenv("GOOGLE_API_KEY", "").strip(),
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip(),
        os.getenv("VERTEXAI_PROJECT", "").strip(),
        os.getenv("GOOGLE_CLOUD_PROJECT", "").strip(),
    )
    return any(bool(value) for value in auth_hints)


def framework_agents_enabled() -> bool:
    return agno_available() and llm_runtime_configured()


def build_agno_model(temperature: float = 0) -> AgnoLiteLLM | None:
    if not framework_agents_enabled():
        return None
    kwargs: dict[str, object] = {
        "id": MODEL,
        "name": "LiteLLM",
        "temperature": temperature,
        "request_params": {"timeout": MODEL_TIMEOUT_SECONDS},
    }
    if LITELLM_API_KEY:
        kwargs["api_key"] = LITELLM_API_KEY
    if LITELLM_API_BASE:
        kwargs["api_base"] = LITELLM_API_BASE
    return AgnoLiteLLM(**kwargs)


def extract_agno_content(response: object) -> object:
    content = getattr(response, "content", response)
    if isinstance(content, BaseModel):
        return content
    if isinstance(content, str):
        parsed = extract_json_dict(content)
        return parsed or content
    return content


def build_payload_evidence_objects(step_name: str, payload: dict) -> list[dict]:
    evidence: list[dict] = []
    for highlight in payload.get("highlights", [])[:3]:
        evidence.append(
            {
                "source_step": step_name,
                "kind": "highlight",
                "label": highlight.get("label"),
                "value": highlight.get("value"),
                "detail": highlight.get("detail"),
            }
        )
    table = payload.get("table") or {}
    columns = table.get("columns") or []
    for row in (table.get("rows") or [])[:2]:
        row_object = {
            "source_step": step_name,
            "kind": "table_row",
            "columns": columns,
            "row": row,
        }
        if columns:
            row_object["mapping"] = {str(column): row[index] for index, column in enumerate(columns[: len(row)])}
        evidence.append(row_object)
    return evidence[:4]


def evidence_object_to_text(item: dict) -> str:
    if item.get("kind") == "highlight":
        label = item.get("label", "Metric")
        value = item.get("value", "")
        detail = item.get("detail", "")
        suffix = f" ({detail})" if detail else ""
        return f"{label}: {value}{suffix}"
    if item.get("kind") == "table_row":
        mapping = item.get("mapping") or {}
        if mapping:
            preview = ", ".join(f"{key}={value}" for key, value in list(mapping.items())[:3])
            return f"{item.get('source_step', 'step').title()} row: {preview}"
    return str(item)


SPECIALIST_AGENT_INSTRUCTIONS = {
    "trend": (
        "You are the Trend Detector Agent. Call the specialist tool exactly once, then produce one grounded claim "
        "about directional change over time with 2-3 evidence points and 1-2 caveats."
    ),
    "segment": (
        "You are the Comparison Analyst Agent. Call the specialist tool exactly once, then produce one grounded claim "
        "about how the scoped league or team compares against nearby categories."
    ),
    "correlation": (
        "You are the Relationship Analyst Agent. Call the specialist tool exactly once, then summarize the strongest "
        "association without implying causation."
    ),
    "quality": (
        "You are the Coverage Analyst Agent. Call the specialist tool exactly once, then explain how data completeness "
        "limits or supports the downstream conclusion."
    ),
    "distribution": (
        "You are the Distribution Analyst Agent. Call the specialist tool exactly once, then explain which variable "
        "has the widest spread and why that matters for follow-up analysis."
    ),
}

INTENT_CLASSIFIER_SYSTEM_PROMPT = """\
You classify football user questions into one intent.

Allowed intents:
- count_lookup: direct count/stat lookup such as how many goals, teams, matches, seasons
- team_recent_claim: recent match streak/form/claim checks such as last 5 games, won all last 10
- team_performance: team performance across seasons or years
- home_advantage: venue/home-away advantage questions
- correlation: correlation/relationship between metrics
- data_quality: missingness/completeness/coverage/nulls
- league_compare: compare leagues, standings, tables, shots, cards, fouls, corners
- scoring: scoring trends or goal trends over time
- overview: generic analytics request when no better label fits

Rules:
- "last N seasons" or "last N years" for a team means team_performance, not team_recent_claim.
- "last N games" or "last N matches" means team_recent_claim.
- direct factual lookups like "how many goals did Arsenal score in the last 5 years" are count_lookup.
- return JSON only: {"intent":"one_of_the_allowed_intents"}
"""
FOOTBALL_GLOSSARY = {
    "football": "Football, also called soccer in some countries, is a team sport played between two teams of 11 players who mainly use their feet to move the ball and try to score by getting it into the opponent's goal.",
    "soccer": "Soccer, also called football in many countries, is a team sport played between two teams of 11 players who mainly use their feet to move the ball and try to score by getting it into the opponent's goal.",
    "goal": "A goal is scored when the whole ball crosses the goal line between the posts and under the crossbar, as long as no foul or rule violation occurred first.",
    "goals": "A goal is scored when the whole ball crosses the goal line between the posts and under the crossbar, as long as no foul or rule violation occurred first.",
    "offside": "Offside means an attacking player is penalized for being in an illegal advanced position when a teammate plays the ball to them, if they then become involved in active play.",
    "penalty": "A penalty is a direct shot from the penalty spot, awarded when a defending team commits certain fouls inside its own penalty area.",
    "corner": "A corner kick is awarded to the attacking team when the defending team last touches the ball before it goes out over its own goal line, without a goal being scored.",
    "yellow card": "A yellow card is a caution shown by the referee for misconduct or repeated rule-breaking.",
    "red card": "A red card means a player is sent off and cannot continue in the match.",
    "assist": "An assist is the final pass or action that directly leads to a goal, depending on the competition or data provider's definition.",
    "clean sheet": "A clean sheet means a team or goalkeeper finishes the match without conceding a goal.",
}


@dataclass(frozen=True)
class LeagueCandidate:
    country: str
    league: str
    match_count: int


@dataclass(frozen=True)
class QueryScope:
    country: str | None = None
    league: str | None = None
    season: str | None = None
    team: str | None = None

    @property
    def is_global(self) -> bool:
        return self.country is None and self.league is None and self.season is None and self.team is None

    @property
    def label(self) -> str:
        if self.team and self.season:
            return f"{self.team} ({self.season})"
        if self.team and self.country:
            return f"{self.team} ({self.country})"
        if self.team:
            return self.team
        if self.league and self.country and self.season:
            return f"{self.league} ({self.country}, {self.season})"
        if self.league and self.country:
            return f"{self.league} ({self.country})"
        if self.country and self.season:
            return f"{self.country} ({self.season})"
        if self.league:
            return self.league
        if self.country:
            return self.country
        if self.season:
            return f"all tracked leagues in {self.season}"
        return "the full warehouse"


@dataclass(frozen=True)
class DomainCheck:
    is_football: bool
    reason: str
    matched_terms: tuple[str, ...] = ()
    external_label: str | None = None
    external_query: str | None = None


@dataclass(frozen=True)
class AnswerabilityCheck:
    mode: Literal["warehouse", "external_fact", "clarify"]
    reason: str


@dataclass(frozen=True)
class TeamCandidate:
    team: str
    country: str | None
    league: str | None
    match_count: int


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def contains_phrase(text: str, phrase: str) -> bool:
    return f" {phrase} " in f" {text} "


def sql_identifier(name: str) -> str:
    return f'"{str(name).replace(chr(34), chr(34) * 2)}"'


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    normalized = uri.strip()
    if not normalized.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")

    path = normalized[5:]
    bucket_name, _, object_name = path.partition("/")
    if not bucket_name or not object_name:
        raise ValueError(f"Invalid GCS URI: {uri}")
    return bucket_name, object_name


def ensure_duckdb_file(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    duckdb_gcs_uri: str | None = None,
    force_download: bool = False,
) -> Path:
    database_path = Path(duckdb_path)
    if database_path.exists() and not force_download:
        return database_path

    gcs_uri = (duckdb_gcs_uri or os.getenv("DUCKDB_GCS_URI", DEFAULT_DUCKDB_GCS_URI)).strip()
    if not gcs_uri:
        if database_path.exists():
            return database_path
        raise FileNotFoundError(
            f"DuckDB file not found locally and DUCKDB_GCS_URI is not configured: {duckdb_path}"
        )

    with _DUCKDB_SYNC_LOCK:
        if database_path.exists() and not force_download:
            return database_path

        database_path.parent.mkdir(parents=True, exist_ok=True)
        bucket_name, object_name = parse_gcs_uri(gcs_uri)
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(object_name)
        blob.download_to_filename(str(database_path))
        return database_path


def open_connection(duckdb_path: str) -> duckdb.DuckDBPyConnection:
    database_path = ensure_duckdb_file(duckdb_path)
    return duckdb.connect(str(database_path), read_only=True)


def format_number(value: float | int | None, digits: int = 1) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, int):
        return f"{value:,}"
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{value:.{digits}f}"


def ordered_league_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if "league" not in frame.columns or frame.empty:
        return frame
    if frame["league"].isin(MAJOR_ENGLISH_LEAGUE_ORDER).all():
        ordered = frame.copy()
        ordered["league"] = pd.Categorical(
            ordered["league"],
            categories=MAJOR_ENGLISH_LEAGUE_ORDER,
            ordered=True,
        )
        return ordered.sort_values("league").reset_index(drop=True)
    return frame.sort_values(["country", "league"] if "country" in frame.columns else ["league"]).reset_index(drop=True)


def table_payload(frame: pd.DataFrame, float_digits: int = 2) -> dict:
    records: list[list[str]] = []
    display_frame = frame.copy()
    for column in display_frame.columns:
        if pd.api.types.is_float_dtype(display_frame[column]):
            display_frame[column] = display_frame[column].map(lambda value: f"{value:.{float_digits}f}")
        elif pd.api.types.is_integer_dtype(display_frame[column]):
            display_frame[column] = display_frame[column].map(lambda value: f"{int(value):,}")
        else:
            display_frame[column] = display_frame[column].astype(str)

    for row in display_frame.itertuples(index=False):
        records.append(list(row))

    return {
        "columns": list(display_frame.columns),
        "rows": records,
    }


def tool_call(name: str, label: str, summary: str) -> dict[str, str]:
    return {"name": name, "label": label, "summary": summary}


def metric(label: str, value: str, caption: str) -> dict[str, str]:
    return {"label": label, "value": value, "caption": caption}


def source_item(title: str, snippet: str, url: str | None = None, source_type: str = "warehouse") -> dict[str, str]:
    payload = {
        "title": title,
        "snippet": snippet,
        "source_type": source_type,
    }
    if url:
        payload["url"] = url
    return payload


def clean_external_text(value: str, limit: int = 220) -> str:
    normalized = re.sub(r"\s+", " ", value or "").strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def compact_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def extract_page_snippet(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return clean_external_text(meta["content"])

    og = soup.find("meta", attrs={"property": "og:description"})
    if og and og.get("content"):
        return clean_external_text(og["content"])

    for paragraph in soup.find_all("p"):
        text = clean_external_text(paragraph.get_text(" ", strip=True))
        if len(text) >= 70:
            return text
    return ""


def fetch_external_source(url: str, fallback_title: str, source_type: str) -> dict | None:
    try:
        response = requests.get(
            url,
            timeout=EXTERNAL_VALIDATION_TIMEOUT_SECONDS,
            headers={"User-Agent": "FootyAgent/1.0 (+https://www.football-data.co.uk/data.php)"},
        )
        response.raise_for_status()
    except Exception:
        return source_item(
            fallback_title,
            "Live snippet unavailable at runtime, but this link is part of the external validation pack for the selected football slice.",
            url,
            source_type=source_type,
        )

    soup = BeautifulSoup(response.text, "html.parser")
    title = clean_external_text(soup.title.get_text(" ", strip=True) if soup.title else fallback_title, limit=90) or fallback_title
    snippet = extract_page_snippet(response.text) or fallback_title
    return source_item(title, snippet, url, source_type=source_type)


def wikipedia_url_for_scope(scope: QueryScope) -> str:
    league = scope.league or PRIMARY_LEAGUES.get(scope.country or "", "")
    if league in WIKIPEDIA_LEAGUE_URLS:
        return WIKIPEDIA_LEAGUE_URLS[league]
    query = quote(f"{league or scope.country or 'association football'}")
    return f"https://en.wikipedia.org/wiki/Special:Search?search={query}"


def external_validation_sources(scope: QueryScope) -> list[dict]:
    candidates: list[tuple[str, str, str]] = [
        (
            "https://www.football-data.co.uk/data.php",
            "football-data.co.uk data index",
            "source",
        )
    ]

    league = scope.league or PRIMARY_LEAGUES.get(scope.country or "", "")
    fbref_url = FBREF_LEAGUE_URLS.get(league)
    if fbref_url:
        candidates.append((fbref_url, f"FBref {league} overview", "external_validation"))
    else:
        candidates.append(("https://fbref.com/en/comps/", "FBref competitions overview", "external_validation"))

    candidates.append((wikipedia_url_for_scope(scope), f"{league or scope.label} context", "external_validation"))

    sources: list[dict] = []
    seen_urls: set[str] = set()
    for url, fallback_title, source_type in candidates:
        if url in seen_urls:
            continue
        seen_urls.add(url)
        fetched = fetch_external_source(url, fallback_title, source_type)
        if fetched:
            sources.append(fetched)
        if len(sources) == 3:
            break
    return sources


def build_executive_summary(payload: dict, scope: QueryScope, sources: list[dict]) -> list[str]:
    points: list[str] = []
    highlights = payload.get("highlights", [])
    answer = clean_external_text(payload.get("answer", ""), limit=260)

    if answer:
        points.append(f"**Core finding:** {answer}")

    for item in highlights[:2]:
        points.append(f"**{item['label']}:** {item['value']} ({item['caption']})")

    hypothesis = payload.get("hypothesis") or {}
    if hypothesis.get("statement"):
        points.append(f"**Analyst view:** {hypothesis['statement']}")

    for source in sources[:3]:
        label = source.get("title", "External source")
        snippet = clean_external_text(source.get("snippet", ""), limit=180)
        if snippet:
            points.append(f"**External validation, {label}:** {snippet}")

    return points[:5]


def build_warehouse_executive_summary(payload: dict) -> list[str]:
    points: list[str] = []
    answer = clean_external_text(payload.get("answer", ""), limit=260)
    highlights = payload.get("highlights", [])
    hypothesis = payload.get("hypothesis") or {}

    if answer:
        points.append(f"**Core finding:** {answer}")

    for item in highlights[:3]:
        points.append(f"**{item['label']}:** {item['value']} ({item['caption']})")

    if hypothesis.get("statement"):
        points.append(f"**Analyst view:** {hypothesis['statement']}")

    return points[:5]


def line_chart(
    title: str,
    summary: str,
    x_values: list[str],
    series: list[dict],
    y_label: str = "Value",
) -> dict:
    return {
        "type": "line",
        "title": title,
        "summary": summary,
        "x": x_values,
        "y_label": y_label,
        "series": series,
    }


def bar_chart(
    title: str,
    summary: str,
    categories: list[str],
    series: list[dict],
    y_label: str = "Value",
) -> dict:
    return {
        "type": "bar",
        "title": title,
        "summary": summary,
        "x": categories,
        "y_label": y_label,
        "series": series,
    }


def area_chart(
    title: str,
    summary: str,
    x_values: list[str],
    series: list[dict],
    y_label: str = "Value",
) -> dict:
    return {
        "type": "area",
        "title": title,
        "summary": summary,
        "x": x_values,
        "y_label": y_label,
        "series": series,
    }


def dumbbell_chart(
    title: str,
    summary: str,
    categories: list[str],
    left_series_name: str,
    right_series_name: str,
    left_values: list[float | None],
    right_values: list[float | None],
    y_label: str = "Value",
) -> dict:
    return {
        "type": "dumbbell",
        "title": title,
        "summary": summary,
        "categories": categories,
        "left_series_name": left_series_name,
        "right_series_name": right_series_name,
        "left_values": left_values,
        "right_values": right_values,
        "y_label": y_label,
    }


def heatmap_chart(
    title: str,
    summary: str,
    rows: list[str],
    columns: list[str],
    z_values: list[list[float]],
    value_label: str = "Value",
) -> dict:
    return {
        "type": "heatmap",
        "title": title,
        "summary": summary,
        "rows": rows,
        "columns": columns,
        "z": z_values,
        "value_label": value_label,
    }


def hypothesis_payload(title: str, statement: str, evidence: list[str]) -> dict:
    return {
        "title": title,
        "statement": statement,
        "evidence": evidence,
    }


def season_sort_sql() -> str:
    return "COALESCE(try_cast(substr(season, 1, 4) AS INTEGER), 0)"


def scope_clause(scope: QueryScope, table_alias: str = "") -> tuple[str, list]:
    prefix = f"{table_alias}." if table_alias else ""
    clauses: list[str] = []
    params: list[str] = []

    if scope.country:
        clauses.append(f"{prefix}country = ?")
        params.append(scope.country)
    if scope.league:
        clauses.append(f"{prefix}league = ?")
        params.append(scope.league)
    if scope.season:
        clauses.append(f"{prefix}season = ?")
        params.append(scope.season)
    if scope.team:
        clauses.append(f"({prefix}hometeam = ? OR {prefix}awayteam = ?)")
        params.extend([scope.team, scope.team])

    return (" AND ".join(clauses) if clauses else "1=1"), params


def build_reference_catalog(connection: duckdb.DuckDBPyConnection) -> tuple[dict[str, str], dict[str, list[LeagueCandidate]]]:
    rows = connection.execute(
        """
        SELECT country, league, count(*) AS match_count
        FROM matches
        GROUP BY 1, 2
        """
    ).fetchall()
    countries_by_norm: dict[str, str] = {}
    leagues_by_norm: dict[str, list[LeagueCandidate]] = {}
    for country, league, match_count in rows:
        countries_by_norm.setdefault(normalize_text(country), country)
        key = normalize_text(league)
        leagues_by_norm.setdefault(key, []).append(
            LeagueCandidate(country=country, league=league, match_count=int(match_count))
        )

    for candidates in leagues_by_norm.values():
        candidates.sort(key=lambda item: item.match_count, reverse=True)

    return countries_by_norm, leagues_by_norm


def build_team_catalog(connection: duckdb.DuckDBPyConnection) -> dict[str, TeamCandidate]:
    rows = connection.execute(
        """
        WITH team_rows AS (
            SELECT hometeam AS team, country, league FROM matches
            UNION ALL
            SELECT awayteam AS team, country, league FROM matches
        ),
        counts AS (
            SELECT team, country, league, count(*) AS match_count
            FROM team_rows
            GROUP BY 1, 2, 3
        ),
        ranked AS (
            SELECT
                team,
                country,
                league,
                match_count,
                row_number() OVER (
                    PARTITION BY team
                    ORDER BY match_count DESC, country, league
                ) AS rn
            FROM counts
        )
        SELECT team, country, league, match_count
        FROM ranked
        WHERE rn = 1
        """
    ).fetchall()

    catalog: dict[str, TeamCandidate] = {}
    for team, country, league, match_count in rows:
        if not team:
            continue
        catalog[normalize_text(team)] = TeamCandidate(
            team=str(team),
            country=str(country) if country else None,
            league=str(league) if league else None,
            match_count=int(match_count),
        )
    return catalog


def find_team(normalized_message: str, team_catalog: dict[str, TeamCandidate]) -> TeamCandidate | None:
    matches = [
        candidate
        for normalized_team, candidate in team_catalog.items()
        if contains_phrase(normalized_message, normalized_team)
    ]
    if not matches:
        return None
    matches.sort(key=lambda item: (len(item.team), item.match_count), reverse=True)
    return matches[0]


def find_country(normalized_message: str, countries_by_norm: dict[str, str]) -> str | None:
    alias_pairs = sorted(COUNTRY_ALIASES.items(), key=lambda item: len(item[0]), reverse=True)
    for alias, canonical in alias_pairs:
        if contains_phrase(normalized_message, normalize_text(alias)):
            return canonical

    for normalized_country, canonical in sorted(
        countries_by_norm.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if contains_phrase(normalized_message, normalized_country):
            return canonical
    return None


def find_league(
    normalized_message: str,
    selected_country: str | None,
    leagues_by_norm: dict[str, list[LeagueCandidate]],
) -> LeagueCandidate | None:
    alias_pairs = sorted(LEAGUE_ALIASES.items(), key=lambda item: len(item[0]), reverse=True)
    for alias, (alias_country, alias_league) in alias_pairs:
        if not contains_phrase(normalized_message, normalize_text(alias)):
            continue

        normalized_league = normalize_text(alias_league)
        candidates = leagues_by_norm.get(normalized_league, [])
        if alias_country:
            candidates = [candidate for candidate in candidates if candidate.country == alias_country]
        if selected_country:
            country_candidates = [candidate for candidate in candidates if candidate.country == selected_country]
            if country_candidates:
                candidates = country_candidates
        if candidates:
            return candidates[0]
        return LeagueCandidate(country=alias_country or selected_country or "", league=alias_league, match_count=0)

    matches: list[LeagueCandidate] = []
    for normalized_league, candidates in leagues_by_norm.items():
        if contains_phrase(normalized_message, normalized_league):
            matches.extend(candidates)
    if not matches:
        return None

    if selected_country:
        country_matches = [candidate for candidate in matches if candidate.country == selected_country]
        if country_matches:
            matches = country_matches

    matches.sort(key=lambda item: item.match_count, reverse=True)
    return matches[0]


def resolve_scope(connection: duckdb.DuckDBPyConnection, message: str) -> QueryScope:
    normalized_message = normalize_text(message)
    countries_by_norm, leagues_by_norm = build_reference_catalog(connection)
    team_catalog = build_team_catalog(connection)
    country = find_country(normalized_message, countries_by_norm)
    league_candidate = find_league(normalized_message, country, leagues_by_norm)
    team_candidate = find_team(normalized_message, team_catalog)

    if league_candidate is not None:
        country = country or league_candidate.country or None
        league = league_candidate.league
    else:
        league = None

    team = None
    if team_candidate is not None:
        team = team_candidate.team
        country = country or team_candidate.country
        league = league or team_candidate.league

    season_match = SEASON_PATTERN.search(message)
    season = season_match.group(0) if season_match else None
    return QueryScope(country=country, league=league, season=season, team=team)


def message_needs_recent_context(message: str, scope: QueryScope) -> bool:
    if not scope.is_global:
        return False
    normalized = normalize_text(message)
    if not normalized:
        return False
    if any(contains_phrase(normalized, marker) for marker in RECENT_CONTEXT_MARKERS):
        return True
    follow_up_starters = (
        "what about",
        "how about",
        "compare that",
        "compare this",
        "show me that",
        "show me this",
        "and that",
        "and this",
    )
    return any(normalized.startswith(prefix) for prefix in follow_up_starters)


def history_entry_text(entry: dict) -> str:
    candidates = (
        str(entry.get("question") or "").strip(),
        str(entry.get("text") or "").strip(),
        str(entry.get("message") or "").strip(),
    )
    for candidate in candidates:
        if candidate:
            return candidate
    return ""


def scope_from_history_entry(connection: duckdb.DuckDBPyConnection, entry: dict) -> QueryScope:
    candidates = [
        str(entry.get("scope") or "").strip(),
        str(entry.get("question") or "").strip(),
        str(entry.get("text") or "").strip(),
        str(entry.get("message") or "").strip(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        candidate_scope = resolve_scope(connection, candidate)
        if not candidate_scope.is_global:
            return candidate_scope
    return QueryScope()


def apply_recent_scope(message: str, scope: QueryScope) -> str:
    label = scope.label
    rewritten = message
    replacements = (
        (r"\bhere\b", f"in {label}"),
        (r"\bthere\b", f"in {label}"),
        (r"\bthis league\b", label),
        (r"\bthat league\b", label),
        (r"\bsame league\b", label),
        (r"\bthis team\b", label),
        (r"\bthat team\b", label),
        (r"\bsame team\b", label),
        (r"\bthis season\b", label),
        (r"\bthat season\b", label),
        (r"\bsame season\b", label),
        (r"\bcurrent selection\b", label),
        (r"\bselected league\b", label),
        (r"\bselected team\b", label),
    )
    for pattern, replacement in replacements:
        rewritten = re.sub(pattern, replacement, rewritten, flags=re.IGNORECASE)
    if rewritten == message:
        trimmed = message.rstrip()
        suffix = "" if trimmed.endswith("?") else ""
        rewritten = f"{trimmed} in {label}{suffix}"
    return compact_whitespace(rewritten)


def extract_clarification_subject(message: str) -> str | None:
    text = compact_whitespace(message)
    if not text:
        return None
    for pattern in CLARIFICATION_PATTERNS:
        match = re.match(pattern, text, flags=re.IGNORECASE)
        if match:
            subject = compact_whitespace(match.group("subject"))
            return subject.strip(" .?!,;:") or None
    return None


def merge_subject_hint(existing_subject: str, subject_hint: str) -> str:
    existing = compact_whitespace(existing_subject)
    hint = compact_whitespace(subject_hint)
    if not existing:
        return hint
    if not hint:
        return existing
    existing_tokens = normalize_text(existing).split()
    hint_tokens = normalize_text(hint).split()
    if not existing_tokens or not hint_tokens:
        return hint or existing
    if " ".join(existing_tokens) == " ".join(hint_tokens):
        return existing
    if all(token in existing_tokens for token in hint_tokens):
        return existing
    if all(token in hint_tokens for token in existing_tokens):
        return hint
    if len(existing_tokens) == 1 and len(hint_tokens) == 1:
        return f"{hint} {existing}"
    if len(hint_tokens) == 1 and hint_tokens[0] not in existing_tokens:
        return f"{hint} {existing}"
    return hint


def rewrite_question_with_subject(question: str, subject_hint: str) -> str | None:
    text = compact_whitespace(question)
    if not text:
        return None
    patterns = (
        r"^(?P<prefix>.*?\b(?:is|was|are|were|does|do|did)\s+)(?P<subject>.+?)(?P<suffix>\s+(?:from|for|at|on|in)\b.*)$",
        r"^(?P<prefix>.*?\b(?:is|was|are|were)\s+)(?P<subject>.+?)(?P<suffix>\s+(?:a|an|the)\b.*)$",
    )
    for pattern in patterns:
        match = re.match(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        merged_subject = merge_subject_hint(match.group("subject"), subject_hint)
        return compact_whitespace(f"{match.group('prefix')}{merged_subject}{match.group('suffix')}")
    return compact_whitespace(f"{text} about {subject_hint}")


def resolve_clarification_from_history(message: str, history: list[dict] | None = None) -> str | None:
    subject_hint = extract_clarification_subject(message)
    if not subject_hint or not history:
        return None
    recent_entries = [entry for entry in history[-5:] if isinstance(entry, dict)]
    for entry in reversed(recent_entries):
        if str(entry.get("role") or "").strip().lower() != "user":
            continue
        previous_text = history_entry_text(entry)
        if not previous_text or previous_text.strip() == message.strip():
            continue
        rewritten = rewrite_question_with_subject(previous_text, subject_hint)
        if rewritten:
            return rewritten
    return None


def resolve_message_with_recent_context(
    connection: duckdb.DuckDBPyConnection,
    message: str,
    history: list[dict] | None = None,
) -> tuple[str, QueryScope | None]:
    clarification_rewrite = resolve_clarification_from_history(message, history)
    if clarification_rewrite:
        return clarification_rewrite, None

    current_scope = resolve_scope(connection, message)
    if not history or not message_needs_recent_context(message, current_scope):
        return message, None

    recent_entries = [entry for entry in history[-5:] if isinstance(entry, dict)]
    for entry in reversed(recent_entries):
        if entry.get("out_of_context"):
            continue
        recent_scope = scope_from_history_entry(connection, entry)
        if recent_scope.is_global:
            continue
        return apply_recent_scope(message, recent_scope), recent_scope
    return message, None


def find_external_focus(normalized_message: str) -> tuple[str | None, str | None]:
    for alias, payload in sorted(EXTERNAL_ONLY_FOOTBALL_HINTS.items(), key=lambda item: len(item[0]), reverse=True):
        if contains_phrase(normalized_message, normalize_text(alias)):
            return payload["label"], payload["query"]
    for alias, payload in sorted(EXTERNAL_FOOTBALL_HINTS.items(), key=lambda item: len(item[0]), reverse=True):
        if contains_phrase(normalized_message, normalize_text(alias)):
            return payload["label"], payload["query"]
    if (
        contains_phrase(normalized_message, "fifa")
        and any(token in normalized_message.split() for token in ("won", "winner", "winners", "titles", "title"))
        and any(token in normalized_message.split() for token in ("country", "countries", "nation", "nations", "most"))
    ):
        return "FIFA World Cup", "FIFA World Cup winners by country most titles"
    return None, None


def requires_external_football_info(message: str) -> bool:
    normalized_message = normalize_text(message)
    return any(
        contains_phrase(normalized_message, normalize_text(alias))
        for alias in EXTERNAL_ONLY_FOOTBALL_HINTS
    )


def is_world_cup_titles_query(normalized_message: str) -> bool:
    tokens = set(normalized_message.split())
    has_world_cup_focus = contains_phrase(normalized_message, "fifa world cup") or contains_phrase(normalized_message, "world cup")
    has_fifa_winner_focus = (
        contains_phrase(normalized_message, "fifa")
        and ("won" in tokens or "winner" in tokens or "winners" in tokens or "title" in tokens or "titles" in tokens)
        and ("country" in tokens or "countries" in tokens or "nation" in tokens or "nations" in tokens or "most" in tokens)
    )
    return has_world_cup_focus or has_fifa_winner_focus


def validate_domain(connection: duckdb.DuckDBPyConnection, message: str) -> DomainCheck:
    normalized_message = normalize_text(message)
    tokens = normalized_message.split()
    countries_by_norm, leagues_by_norm = build_reference_catalog(connection)
    team_catalog = build_team_catalog(connection)
    football_vocab = [
        term
        for term in FOOTBALL_DOMAIN_TERMS
        if contains_phrase(normalized_message, normalize_text(term))
    ]
    matched_terms = list(football_vocab)
    country = find_country(normalized_message, countries_by_norm)
    league_candidate = find_league(normalized_message, country, leagues_by_norm)
    team_candidate = find_team(normalized_message, team_catalog)
    external_label, external_query = find_external_focus(normalized_message)
    non_football_terms = [
        term
        for term in NON_FOOTBALL_HINTS
        if contains_phrase(normalized_message, normalize_text(term))
    ]

    if country and (football_vocab or league_candidate):
        matched_terms.append(country)
    if league_candidate:
        matched_terms.append(league_candidate.league)
    if team_candidate:
        matched_terms.append(team_candidate.team)
    if external_label and football_vocab:
        matched_terms.append(external_label)

    if non_football_terms and not football_vocab and league_candidate is None and country is None and external_label is None:
        return DomainCheck(
            is_football=False,
            reason="The request matched clear non-football terms and did not match football entities or football-analysis vocabulary.",
            matched_terms=(),
        )

    deduped_terms = tuple(dict.fromkeys(matched_terms))
    if deduped_terms:
        return DomainCheck(
            is_football=True,
            reason="Matched football entities or football-analysis vocabulary in the request.",
            matched_terms=deduped_terms,
            external_label=external_label,
            external_query=external_query,
        )

    # Do not force vague or low-signal text into football fallback mode.
    if len(tokens) < 3 or len(normalized_message) < 12:
        return DomainCheck(
            is_football=False,
            reason="The request is too short or too ambiguous to classify as a football analytics question.",
            matched_terms=(),
        )

    return DomainCheck(
        is_football=True,
        reason="No strong football entity matched, but the assistant defaults to a football interpretation unless the request is clearly outside the domain.",
        matched_terms=("football_fallback",),
        external_label=external_label or "requested football topic",
        external_query=external_query or f"{message} football soccer analysis",
    )


def is_casual_conversation(message: str) -> bool:
    normalized_message = normalize_text(message)
    if not normalized_message:
        return False
    return any(contains_phrase(normalized_message, normalize_text(term)) for term in CASUAL_CONVERSATION_TERMS)


def resolve_simple_football_term(message: str) -> str | None:
    normalized_message = normalize_text(message)
    if not normalized_message:
        return None
    if not any(
        phrase in normalized_message
        for phrase in ("what is", "what s", "define", "meaning of", "explain")
    ):
        return None

    for term in sorted(FOOTBALL_GLOSSARY.keys(), key=len, reverse=True):
        if contains_phrase(normalized_message, normalize_text(term)):
            return term
    return None


def fetch_total_dataset_metrics(connection: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    total_matches, countries, leagues = connection.execute(
        """
        SELECT
            count(*) AS total_matches,
            count(DISTINCT country) AS countries,
            count(DISTINCT country || ' / ' || league) AS leagues
        FROM matches
        """
    ).fetchone()
    return int(total_matches), int(countries), int(leagues)


def fetch_country_options(connection: duckdb.DuckDBPyConnection) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            """
            SELECT country
            FROM matches
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
    ]


def fetch_league_options(connection: duckdb.DuckDBPyConnection, country: str) -> list[str]:
    leagues = [
        row[0]
        for row in connection.execute(
            """
            SELECT league
            FROM matches
            WHERE country = ?
            GROUP BY 1
            ORDER BY 1
            """,
            [country],
        ).fetchall()
    ]
    preferred = PRIMARY_LEAGUES.get(country)
    if preferred in leagues:
        leagues.remove(preferred)
        leagues.insert(0, preferred)
    return leagues


def resolve_standings_selection(
    connection: duckdb.DuckDBPyConnection,
    country: str | None,
    league: str | None,
) -> tuple[str, str, list[str], list[str]]:
    countries = fetch_country_options(connection)
    selected_country = country if country in countries else None
    if selected_country is None:
        selected_country = "England" if "England" in countries else countries[0]

    leagues = fetch_league_options(connection, selected_country)
    if not leagues:
        raise ValueError(f"No leagues available for {selected_country}.")

    selected_league = league if league in leagues else None
    if selected_league is None:
        selected_league = PRIMARY_LEAGUES.get(selected_country)
    if selected_league not in leagues:
        selected_league = leagues[0]

    return selected_country, selected_league, countries, leagues


def build_match_timestamp(frame: pd.DataFrame) -> pd.Series:
    date_series = pd.to_datetime(frame["date"], dayfirst=True, errors="coerce")
    if "time" not in frame.columns:
        return date_series

    time_values = frame["time"].fillna("").astype(str).str.strip()
    time_parts = time_values.str.extract(r"^(?P<hour>\d{1,2}):(?P<minute>\d{2})$")
    hour = pd.to_numeric(time_parts["hour"], errors="coerce").fillna(0)
    minute = pd.to_numeric(time_parts["minute"], errors="coerce").fillna(0)
    return date_series + pd.to_timedelta(hour, unit="h") + pd.to_timedelta(minute, unit="m")


def compute_standings_frame(matches_frame: pd.DataFrame) -> pd.DataFrame:
    if matches_frame.empty:
        return pd.DataFrame(columns=["rank", "club", "mp", "w", "d", "l", "gf", "ga", "gd", "pts", "last5"])

    frame = matches_frame.copy()
    frame["match_timestamp"] = build_match_timestamp(frame)

    home = pd.DataFrame(
        {
            "team": frame["hometeam"],
            "match_timestamp": frame["match_timestamp"],
            "goals_for": frame["fthg"].astype(int),
            "goals_against": frame["ftag"].astype(int),
            "result": frame["ftr"].map({"H": "W", "D": "D", "A": "L"}),
        }
    )
    away = pd.DataFrame(
        {
            "team": frame["awayteam"],
            "match_timestamp": frame["match_timestamp"],
            "goals_for": frame["ftag"].astype(int),
            "goals_against": frame["fthg"].astype(int),
            "result": frame["ftr"].map({"A": "W", "D": "D", "H": "L"}),
        }
    )

    team_rows = pd.concat([home, away], ignore_index=True)
    team_rows["win"] = (team_rows["result"] == "W").astype(int)
    team_rows["draw"] = (team_rows["result"] == "D").astype(int)
    team_rows["loss"] = (team_rows["result"] == "L").astype(int)

    standings = (
        team_rows.groupby("team", as_index=False)
        .agg(
            mp=("team", "size"),
            w=("win", "sum"),
            d=("draw", "sum"),
            l=("loss", "sum"),
            gf=("goals_for", "sum"),
            ga=("goals_against", "sum"),
        )
        .rename(columns={"team": "club"})
    )
    standings["gd"] = standings["gf"] - standings["ga"]
    standings["pts"] = standings["w"] * 3 + standings["d"]

    recent_form = (
        team_rows.sort_values(["team", "match_timestamp"])
        .groupby("team")["result"]
        .apply(lambda series: list(series.tail(5)))
        .to_dict()
    )
    standings["last5"] = standings["club"].map(recent_form).map(lambda values: values or [])

    standings = standings.sort_values(
        ["pts", "gd", "gf", "club"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    standings.insert(0, "rank", range(1, len(standings) + 1))
    return standings


def form_points(results: list[str]) -> int:
    mapping = {"W": 3, "D": 1, "L": 0}
    return int(sum(mapping.get(result, 0) for result in results))


def standings_pulse_payload(
    matches_frame: pd.DataFrame,
    standings_frame: pd.DataFrame,
    country: str,
    league: str,
    season: str,
) -> dict:
    if matches_frame.empty or standings_frame.empty:
        return {
            "title": "League pulse",
            "summary": f"No completed-match pulse is available yet for {league} in {country} ({season}).",
            "metrics": [],
        }

    leader = standings_frame.iloc[0]
    runner_up = standings_frame.iloc[1] if len(standings_frame) > 1 else None
    avg_goals = float((matches_frame["fthg"] + matches_frame["ftag"]).mean())
    home_win_rate = float((matches_frame["ftr"] == "H").mean() * 100)
    over_2_5_rate = float(((matches_frame["fthg"] + matches_frame["ftag"]) > 2).mean() * 100)

    form_frame = standings_frame.copy()
    form_frame["form_points"] = form_frame["last5"].map(form_points)
    form_leader = form_frame.sort_values(
        ["form_points", "pts", "gd", "club"],
        ascending=[False, False, False, True],
    ).iloc[0]

    if runner_up is not None:
        title_gap = int(leader["pts"] - runner_up["pts"])
        gap_caption = f"{leader['club']} over {runner_up['club']}"
    else:
        title_gap = 0
        gap_caption = "Only one team in the current table"

    summary = (
        f"{leader['club']} leads {league} in {country} for {season} on {leader['pts']} points. "
        f"The current first-place gap is {title_gap} points, matches are averaging {avg_goals:.2f} goals, "
        f"and the hottest recent form belongs to {form_leader['club']}."
    )

    return {
        "title": "League pulse",
        "summary": summary,
        "metrics": [
            metric("Leader", str(leader["club"]), f"{int(leader['pts'])} pts · GD {int(leader['gd']):+d}"),
            metric("Title gap", f"{title_gap} pts", gap_caption),
            metric("Goal climate", f"{avg_goals:.2f}", f"{over_2_5_rate:.0f}% of matches over 2.5 goals"),
            metric("Hottest form", str(form_leader["club"]), f"{int(form_leader['form_points'])} pts from the last five"),
            metric("Home edge", f"{home_win_rate:.1f}%", "Home-win share across completed matches"),
        ],
    }


def standings_payload(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    country: str | None = None,
    league: str | None = None,
) -> dict:
    connection = open_connection(duckdb_path)
    try:
        selected_country, selected_league, countries, leagues = resolve_standings_selection(
            connection,
            country,
            league,
        )
        selected_season = fetch_latest_season(
            connection,
            QueryScope(country=selected_country, league=selected_league),
            prefer_hyphenated=True,
        )
        matches = connection.execute(
            """
            SELECT date, time, hometeam, awayteam, fthg, ftag, ftr
            FROM matches
            WHERE country = ?
              AND league = ?
              AND season = ?
              AND fthg IS NOT NULL
              AND ftag IS NOT NULL
            ORDER BY date, time, hometeam, awayteam
            """,
            [selected_country, selected_league, selected_season],
        ).df()
    finally:
        connection.close()

    standings = compute_standings_frame(matches)
    rows = [
        {
            "rank": int(row.rank),
            "club": str(row.club),
            "mp": int(row.mp),
            "w": int(row.w),
            "d": int(row.d),
            "l": int(row.l),
            "gf": int(row.gf),
            "ga": int(row.ga),
            "gd": int(row.gd),
            "pts": int(row.pts),
            "last5": list(row.last5),
        }
        for row in standings.itertuples(index=False)
    ]

    return {
        "country_options": countries,
        "league_options": leagues,
        "selected_country": selected_country,
        "selected_league": selected_league,
        "selected_season": selected_season,
        "pulse": standings_pulse_payload(matches, standings, selected_country, selected_league, selected_season),
        "rows": rows,
    }


def fetch_scope_metrics(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> tuple[int, int]:
    where_clause, params = scope_clause(scope)
    rows, seasons = connection.execute(
        f"""
        SELECT count(*) AS rows, count(DISTINCT season) AS seasons
        FROM matches
        WHERE {where_clause}
        """,
        params,
    ).fetchone()
    return int(rows), int(seasons)


def fetch_latest_season(
    connection: duckdb.DuckDBPyConnection,
    scope: QueryScope,
    prefer_hyphenated: bool = False,
) -> str:
    where_clause, params = scope_clause(scope)
    hyphen_clause = " AND season LIKE '%-%'" if prefer_hyphenated else ""
    row = connection.execute(
        f"""
        SELECT season
        FROM (
            SELECT
                season,
                {season_sort_sql()} AS season_start_year
            FROM matches
            WHERE {where_clause}{hyphen_clause}
            GROUP BY 1, 2
        )
        ORDER BY season_start_year DESC, season DESC
        LIMIT 1
        """,
        params,
    ).fetchone()
    if row is None and prefer_hyphenated:
        return fetch_latest_season(connection, scope, prefer_hyphenated=False)
    if row is None:
        raise ValueError(f"No data available for {scope.label}.")
    return str(row[0])


def fetch_season_trend_frame(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> pd.DataFrame:
    where_clause, params = scope_clause(scope)
    return connection.execute(
        f"""
        SELECT
            season,
            {season_sort_sql()} AS season_start_year,
            round(avg(fthg), 2) AS avg_home_goals,
            round(avg(ftag), 2) AS avg_away_goals,
            round(avg(fthg + ftag), 2) AS avg_total_goals,
            round(avg(hs + "as"), 1) AS avg_shots,
            round(avg(CASE WHEN ftr = 'H' THEN 1 ELSE 0 END) * 100, 2) AS home_win_rate,
            round(avg(CASE WHEN ftr = 'D' THEN 1 ELSE 0 END) * 100, 2) AS draw_rate,
            round(avg(CASE WHEN ftr = 'A' THEN 1 ELSE 0 END) * 100, 2) AS away_win_rate
        FROM matches
        WHERE {where_clause}
        GROUP BY 1, 2
        ORDER BY 2, 1
        """,
        params,
    ).df()


def fetch_latest_league_snapshot(
    connection: duckdb.DuckDBPyConnection,
    scope: QueryScope,
    latest_season: str,
    limit: int | None = None,
) -> pd.DataFrame:
    snapshot_scope = QueryScope(country=scope.country, season=latest_season)
    where_clause, params = scope_clause(snapshot_scope)
    frame = connection.execute(
        f"""
        SELECT
            country,
            league,
            count(*) AS matches,
            round(avg(fthg + ftag), 2) AS avg_goals,
            round(avg(hs + "as"), 1) AS avg_shots,
            round(avg(hy + ay + hr + ar), 2) AS avg_cards,
            round(avg(CASE WHEN ftr = 'H' THEN 1 ELSE 0 END) * 100, 1) AS home_win_rate
        FROM matches
        WHERE {where_clause}
        GROUP BY 1, 2
        ORDER BY matches DESC, avg_goals DESC, country, league
        """,
        params,
    ).df()
    if limit is not None:
        frame = frame.head(limit)
    return ordered_league_frame(frame)


def fetch_data_quality_frame(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> pd.DataFrame:
    where_clause, params = scope_clause(scope)
    return connection.execute(
        f"""
        SELECT
            season,
            {season_sort_sql()} AS season_start_year,
            round(avg(CASE WHEN hs IS NULL THEN 1 ELSE 0 END) * 100, 1) AS hs_missing_pct,
            round(avg(CASE WHEN hst IS NULL THEN 1 ELSE 0 END) * 100, 1) AS hst_missing_pct,
            round(avg(CASE WHEN referee IS NULL THEN 1 ELSE 0 END) * 100, 1) AS referee_missing_pct,
            round(avg(CASE WHEN time IS NULL THEN 1 ELSE 0 END) * 100, 1) AS time_missing_pct
        FROM matches
        WHERE {where_clause}
        GROUP BY 1, 2
        ORDER BY 2, 1
        """,
        params,
    ).df()


def fetch_correlation_frame(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> pd.DataFrame:
    where_clause, params = scope_clause(scope)
    return connection.execute(
        f"""
        SELECT
            fthg,
            ftag,
            (fthg + ftag) AS total_goals,
            hs,
            "as" AS away_shots,
            (hs + "as") AS total_shots,
            hst,
            ast,
            (hst + ast) AS total_shots_on_target,
            hc,
            ac,
            (hc + ac) AS total_corners,
            hy,
            ay,
            hr,
            ar,
            (hy + ay + hr + ar) AS total_cards
        FROM matches
        WHERE {where_clause}
          AND hs IS NOT NULL
          AND hst IS NOT NULL
          AND hc IS NOT NULL
        """,
        params,
    ).df()


def fetch_team_season_frame(connection: duckdb.DuckDBPyConnection, team: str) -> pd.DataFrame:
    return connection.execute(
        """
        WITH team_matches AS (
            SELECT
                season,
                country,
                league,
                date,
                CASE WHEN hometeam = ? THEN fthg ELSE ftag END AS goals_for,
                CASE WHEN hometeam = ? THEN ftag ELSE fthg END AS goals_against,
                CASE
                    WHEN (hometeam = ? AND ftr = 'H') OR (awayteam = ? AND ftr = 'A') THEN 'W'
                    WHEN ftr = 'D' THEN 'D'
                    ELSE 'L'
                END AS result
            FROM matches
            WHERE hometeam = ? OR awayteam = ?
        )
        SELECT
            season,
            country,
            league,
            count(*) AS matches_played,
            sum(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
            sum(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS draws,
            sum(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
            sum(goals_for) AS goals_for,
            sum(goals_against) AS goals_against,
            round(avg(goals_for), 2) AS goals_per_match,
            round(avg(goals_against), 2) AS goals_allowed_per_match,
            round(avg(CASE WHEN result = 'W' THEN 1 ELSE 0 END) * 100, 1) AS win_rate,
            round(avg(CASE WHEN result = 'L' THEN 1 ELSE 0 END) * 100, 1) AS loss_rate,
            COALESCE(try_cast(substr(season, 1, 4) AS INTEGER), 0) AS season_start_year
        FROM team_matches
        GROUP BY 1, 2, 3, 14
        ORDER BY season_start_year, season
        """,
        [team, team, team, team, team, team],
    ).df()


def fetch_recent_team_matches(connection: duckdb.DuckDBPyConnection, team: str, limit: int = 10) -> pd.DataFrame:
    return connection.execute(
        """
        WITH team_matches AS (
            SELECT
                season,
                country,
                league,
                date,
                time,
                hometeam,
                awayteam,
                fthg,
                ftag,
                ftr,
                CASE WHEN hometeam = ? THEN 'home' ELSE 'away' END AS venue,
                CASE WHEN hometeam = ? THEN awayteam ELSE hometeam END AS opponent,
                CASE WHEN hometeam = ? THEN fthg ELSE ftag END AS goals_for,
                CASE WHEN hometeam = ? THEN ftag ELSE fthg END AS goals_against,
                CASE
                    WHEN (hometeam = ? AND ftr = 'H') OR (awayteam = ? AND ftr = 'A') THEN 'W'
                    WHEN ftr = 'D' THEN 'D'
                    ELSE 'L'
                END AS result
            FROM matches
            WHERE hometeam = ? OR awayteam = ?
        )
        SELECT *
        FROM team_matches
        ORDER BY try_strptime(date, '%d/%m/%Y') DESC NULLS LAST, time DESC NULLS LAST, season DESC
        LIMIT ?
        """,
        [team, team, team, team, team, team, team, team, limit],
    ).df()


def fetch_match_feature_frame(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> pd.DataFrame:
    where_clause, params = scope_clause(scope)
    return connection.execute(
        f"""
        SELECT
            season,
            country,
            league,
            ftr,
            fthg,
            ftag,
            (fthg + ftag) AS total_goals,
            hs,
            "as" AS away_shots,
            (hs + "as") AS total_shots,
            hst,
            ast,
            (hst + ast) AS total_shots_on_target,
            hc,
            ac,
            (hc + ac) AS total_corners,
            hy,
            ay,
            hr,
            ar,
            (hy + ay + hr + ar) AS total_cards
        FROM matches
        WHERE {where_clause}
          AND ftr IS NOT NULL
          AND fthg IS NOT NULL
          AND ftag IS NOT NULL
        """,
        params,
    ).df()


def split_windows(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return frame, frame
    if len(frame) == 1:
        return frame, frame
    window = min(5, max(1, len(frame) // 2))
    return frame.head(window), frame.tail(window)


def rank_label(frame: pd.DataFrame, league: str) -> str:
    ordered = frame.sort_values("avg_goals", ascending=False).reset_index(drop=True)
    matches = ordered.index[ordered["league"] == league]
    if len(matches) == 0:
        return "outside the visible comparison set"
    return f"{int(matches[0]) + 1} of {len(ordered)}"


def safe_band_labels(values: pd.Series, bins: list[float], labels: list[str]) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(dtype="object")
    return pd.cut(numeric, bins=bins, labels=labels, include_lowest=True, right=True).astype(str)


def build_proportion_heatmap(frame: pd.DataFrame, row_col: str, col_col: str, value_col: str) -> dict | None:
    if frame.empty or row_col not in frame.columns or col_col not in frame.columns:
        return None
    grouped = (
        frame[[row_col, col_col]]
        .dropna()
        .assign(_count=1)
        .groupby([row_col, col_col], observed=False)["_count"]
        .sum()
        .reset_index()
    )
    if grouped.empty:
        return None
    grouped["prop"] = grouped.groupby(row_col, observed=False)["_count"].transform(lambda values: values / values.sum())
    pivot = grouped.pivot(index=row_col, columns=col_col, values="prop").fillna(0)
    if pivot.empty:
        return None
    return heatmap_chart(
        value_col,
        "Each row sums to 100%, so the chart shows how result mix shifts as match conditions move across bands.",
        pivot.index.astype(str).tolist(),
        pivot.columns.astype(str).tolist(),
        [[round(float(value), 3) for value in row] for row in pivot.to_numpy()],
        value_label="share",
    )


def build_metric_heatmap(trend: pd.DataFrame) -> dict | None:
    if trend.empty:
        return None
    metric_columns = {
        "Goals": "avg_total_goals",
        "Shots": "avg_shots",
        "Home win %": "home_win_rate",
        "Draw %": "draw_rate",
        "Away win %": "away_win_rate",
    }
    matrix: list[list[float]] = []
    for label, column in metric_columns.items():
        series = pd.to_numeric(trend[column], errors="coerce")
        std = float(series.std(ddof=0) or 0)
        if std == 0:
            normalized = [0.0 for _ in series]
        else:
            normalized = [round(float((value - series.mean()) / std), 3) for value in series]
        matrix.append(normalized)
    return heatmap_chart(
        "Season-by-season metric pressure map",
        "Values are normalized within each metric, so brighter cells show seasons that stand out relative to that metric's own history.",
        list(metric_columns.keys()),
        trend["season"].astype(str).tolist(),
        matrix,
        value_label="z-score",
    )


def build_team_result_mix_heatmap(frame: pd.DataFrame) -> dict | None:
    if frame.empty:
        return None
    working = frame.copy()
    totals = (working["wins"] + working["draws"] + working["losses"]).replace(0, pd.NA)
    heatmap = pd.DataFrame(
        {
            "Wins %": (working["wins"] / totals * 100).round(1),
            "Draws %": (working["draws"] / totals * 100).round(1),
            "Losses %": (working["losses"] / totals * 100).round(1),
        },
        index=working["season"].astype(str),
    ).fillna(0)
    return heatmap_chart(
        "Season result mix",
        "Shows how each season splits between wins, draws, and losses instead of collapsing everything into one average.",
        heatmap.index.tolist(),
        heatmap.columns.tolist(),
        [[round(float(value), 1) for value in row] for row in heatmap.to_numpy()],
        value_label="share %",
    )


def dashboard_payload(duckdb_path: str = DEFAULT_DUCKDB_PATH) -> dict:
    connection = open_connection(duckdb_path)
    try:
        warehouse_scope = QueryScope()
        latest_season = fetch_latest_season(connection, warehouse_scope, prefer_hyphenated=True)
        total_matches, country_count, league_count = fetch_total_dataset_metrics(connection)
        latest_snapshot = fetch_latest_league_snapshot(connection, warehouse_scope, latest_season, limit=8)
        season_trend = fetch_season_trend_frame(connection, warehouse_scope)
    finally:
        connection.close()

    early_window, recent_window = split_windows(season_trend)
    away_goal_delta = recent_window["avg_away_goals"].mean() - early_window["avg_away_goals"].mean()
    home_win_delta = recent_window["home_win_rate"].mean() - early_window["home_win_rate"].mean()
    latest_total_goals = latest_snapshot["avg_goals"].mean()
    latest_home_win = latest_snapshot["home_win_rate"].mean()
    latest_cards = latest_snapshot["avg_cards"].mean()

    spotlight = {
        "title": "Multi-League Analyst Mode",
        "summary": (
            "The chat assistant can now resolve countries and leagues directly from the question, "
            "then run EDA over the matching DuckDB slice before answering."
        ),
        "stats": [
            metric("Latest dashboard season", latest_season, "Current multi-country snapshot"),
            metric("Away-goal shift", f"{away_goal_delta:+.2f}", "Recent seasons vs earliest seasons"),
            metric("Home-win shift", f"{home_win_delta:+.2f} pts", "Recent seasons vs earliest seasons"),
        ],
    }

    welcome_message = {
        "role": "assistant",
        "text": (
            "Footy Agent is live. Ask about Spain, La Liga, Serie A, the Premier League, or the full warehouse "
            "and I will run EDA-backed analysis before giving a conclusion."
        ),
        "tool_calls": [
            tool_call("overview", "Dataset Snapshot", "Loaded the multi-country DuckDB warehouse and latest active leagues."),
        ],
        "highlights": [
            metric("Warehouse", format_number(total_matches), f"{country_count} countries tracked"),
            metric("Leagues", format_number(league_count), "Distinct country/league slices"),
            metric("Latest avg goals", format_number(latest_total_goals, 2), latest_season),
        ],
        "suggested_prompts": PROMPT_CHIPS,
    }

    return {
        "hero": {
            "eyebrow": "Football analyst cockpit",
            "title": "Ask football questions, trigger EDA, and get grounded answers.",
            "description": (
                "The assistant resolves the requested country or league, runs analysis on DuckDB, "
                "and only then hands the evidence to the model layer."
            ),
        },
        "metrics": [
            metric("Warehouse matches", format_number(total_matches), f"{country_count} countries in storage"),
            metric("Tracked leagues", format_number(league_count), "Distinct country/league slices"),
            metric("Latest dashboard season", latest_season, "Hyphenated major-league snapshot"),
            metric("Snapshot avg goals", format_number(latest_total_goals, 2), "Across visible leagues"),
            metric("Snapshot home win rate", f"{latest_home_win:.1f}%", "Across visible leagues"),
            metric("Snapshot avg cards", format_number(latest_cards, 2), "Yellow + red cards per match"),
        ],
        "league_snapshot": latest_snapshot.to_dict(orient="records"),
        "tool_cards": [
            {
                "name": "Domain Gate",
                "tag": "Routing",
                "description": "Validate that the question is actually about football before starting retrieval.",
            },
            {
                "name": "Aggregate",
                "tag": "Statistical",
                "description": "Compute season and league-level averages, scoring rates, and result splits.",
            },
            {
                "name": "Segment",
                "tag": "Grouping",
                "description": "Compare leagues inside a country or compare major leagues across the warehouse.",
            },
            {
                "name": "Correlation",
                "tag": "Exploration",
                "description": "Surface relationships between goals, shots, corners, fouls, and cards.",
            },
            {
                "name": "Fallback RAG",
                "tag": "Coverage",
                "description": "If the warehouse has no coverage, crawl football sources and rank snippets before answering.",
            },
        ],
        "spotlight": spotlight,
        "prompt_chips": PROMPT_CHIPS,
        "welcome_message": welcome_message,
        "runbook": [
            "Validate that the question is inside the football domain.",
            "Resolve the country and league from the user question and fetch the matching DuckDB slice.",
            "If the warehouse has no coverage, switch to web search plus ranked snippet retrieval.",
            "Run aggregate, segment, correlation, and quality specialists in parallel before generating the final evidence-backed hypothesis.",
        ],
    }


def home_advantage_response(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> dict:
    season_trend = fetch_season_trend_frame(connection, scope)
    if season_trend.empty:
        raise ValueError(f"No trend data available for {scope.label}.")

    early, recent = split_windows(season_trend)
    recent_home = recent["home_win_rate"].mean()
    early_home = early["home_win_rate"].mean()
    recent_away = recent["away_win_rate"].mean()
    early_away = early["away_win_rate"].mean()
    recent_away_goals = recent["avg_away_goals"].mean()
    early_away_goals = early["avg_away_goals"].mean()

    answer = (
        f"For {scope.label}, home advantage is still present, but the shape of it changes over time. "
        f"In the earliest comparison window, the average home-win rate was {early_home:.1f}%. "
        f"In the most recent window, it is {recent_home:.1f}%, while away-win rate moved from "
        f"{early_away:.1f}% to {recent_away:.1f}%. Away scoring moved from {early_away_goals:.2f} "
        f"to {recent_away_goals:.2f} goals per match."
    )

    table = season_trend.tail(min(6, len(season_trend)))[
        ["season", "home_win_rate", "away_win_rate", "avg_home_goals", "avg_away_goals", "avg_total_goals"]
    ]
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("aggregate", "Statistical Aggregation", f"Computed season-level result rates and scoring for {scope.label}."),
            tool_call("segment", "Filtering and Grouping", "Compared earlier and recent windows inside the requested slice."),
        ],
        "highlights": [
            metric("Early home-win avg", f"{early_home:.1f}%", "Earliest comparison window"),
            metric("Recent home-win avg", f"{recent_home:.1f}%", "Most recent comparison window"),
            metric("Away-goal shift", f"{recent_away_goals - early_away_goals:+.2f}", "Recent vs early windows"),
        ],
        "table": table_payload(table, float_digits=2),
        "suggested_prompts": [
            "Compare the leagues on goals and cards in Spain.",
            "What are the strongest metric correlations in this league?",
        ],
    }


def league_comparison_response(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> dict:
    comparison_scope = QueryScope(country=scope.country, season=scope.season)
    latest_season = fetch_latest_season(connection, comparison_scope, prefer_hyphenated=comparison_scope.is_global)
    snapshot = fetch_latest_league_snapshot(
        connection,
        comparison_scope,
        latest_season,
        limit=None if comparison_scope.country else 8,
    )
    if snapshot.empty:
        raise ValueError(f"No league comparison data available for {scope.label}.")

    if scope.league:
        target = snapshot.loc[snapshot["league"] == scope.league].iloc[0]
        if len(snapshot) == 1:
            answer = (
                f"{scope.league} is the only tracked league for {scope.country} in the current comparison set, "
                f"so this is a focused league read rather than an internal league comparison. "
                f"It is averaging {target['avg_goals']:.2f} goals, {target['avg_shots']:.1f} shots, and "
                f"{target['avg_cards']:.2f} cards per match in {latest_season}."
            )
        else:
            answer = (
                f"In {latest_season}, {scope.league} in {scope.country} is averaging {target['avg_goals']:.2f} goals, "
                f"{target['avg_shots']:.1f} shots, and {target['avg_cards']:.2f} cards per match. "
                f"Within the visible comparison set, it ranks {rank_label(snapshot, scope.league)} on scoring."
            )
        highlights = [
            metric("Target league", scope.league, scope.country or "Resolved from the question"),
            metric("Goals per match", f"{target['avg_goals']:.2f}", latest_season),
            metric("Scoring rank", rank_label(snapshot, scope.league), "Within the comparison set"),
        ]
    elif scope.country:
        top_goals = snapshot.sort_values("avg_goals", ascending=False).iloc[0]
        disciplined = snapshot.sort_values("avg_cards", ascending=True).iloc[0]
        answer = (
            f"In the latest tracked season for {scope.country} ({latest_season}), {top_goals['league']} is the highest-scoring league "
            f"at {top_goals['avg_goals']:.2f} goals per match. The cleanest discipline profile belongs to "
            f"{disciplined['league']}, averaging {disciplined['avg_cards']:.2f} cards per match."
        )
        highlights = [
            metric("Highest scoring", top_goals["league"], f"{top_goals['avg_goals']:.2f} goals/match"),
            metric("Most disciplined", disciplined["league"], f"{disciplined['avg_cards']:.2f} cards/match"),
            metric("Season", latest_season, scope.country),
        ]
    else:
        top_goals = snapshot.sort_values("avg_goals", ascending=False).iloc[0]
        disciplined = snapshot.sort_values("avg_cards", ascending=True).iloc[0]
        answer = (
            f"Across the latest visible multi-country snapshot ({latest_season}), {top_goals['league']} in {top_goals['country']} "
            f"is the highest-scoring league at {top_goals['avg_goals']:.2f} goals per match. "
            f"The cleanest discipline profile belongs to {disciplined['league']} in {disciplined['country']}."
        )
        highlights = [
            metric("Highest scoring", f"{top_goals['country']} · {top_goals['league']}", f"{top_goals['avg_goals']:.2f} goals/match"),
            metric("Most disciplined", f"{disciplined['country']} · {disciplined['league']}", f"{disciplined['avg_cards']:.2f} cards/match"),
            metric("Season", latest_season, "Global comparison snapshot"),
        ]

    return {
        "answer": answer,
        "tool_calls": [
            tool_call("segment", "Filtering and Grouping", f"Grouped the latest season snapshot for {comparison_scope.label}."),
            tool_call("aggregate", "Statistical Aggregation", "Computed league-level averages for goals, shots, cards, and home-win rate."),
        ],
        "highlights": highlights,
        "table": table_payload(snapshot, float_digits=2),
        "suggested_prompts": [
            "How has home advantage changed over time in this league?",
            "Show me the strongest metric correlations here.",
        ],
    }


def scoring_trend_response(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> dict:
    trend = fetch_season_trend_frame(connection, scope)
    if trend.empty:
        raise ValueError(f"No scoring trend data available for {scope.label}.")

    early, recent = split_windows(trend)
    latest_row = trend.iloc[-1]
    answer = (
        f"For {scope.label}, the latest tracked season is {latest_row['season']} with "
        f"{latest_row['avg_total_goals']:.2f} goals and {latest_row['avg_shots']:.1f} shots per match. "
        f"Compared with the earliest comparison window, scoring moved by "
        f"{recent['avg_total_goals'].mean() - early['avg_total_goals'].mean():+.2f} goals per match."
    )
    table = trend.tail(min(8, len(trend)))[["season", "avg_total_goals", "avg_shots", "avg_home_goals", "avg_away_goals"]]
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("aggregate", "Statistical Aggregation", f"Computed season-level scoring and shot trends for {scope.label}."),
        ],
        "highlights": [
            metric("Latest season", str(latest_row["season"]), scope.label),
            metric("Latest avg goals", f"{latest_row['avg_total_goals']:.2f}", "Goals per match"),
            metric("Trend shift", f"{recent['avg_total_goals'].mean() - early['avg_total_goals'].mean():+.2f}", "Recent vs early windows"),
        ],
        "table": table_payload(table, float_digits=2),
        "suggested_prompts": [
            "How has home advantage changed over time here?",
            "What does the data quality profile look like?",
        ],
    }


def team_performance_response(
    connection: duckdb.DuckDBPyConnection,
    scope: QueryScope,
    message: str = "",
) -> dict:
    if not scope.team:
        raise ValueError("Team performance response requires a team scope.")

    frame = fetch_team_season_frame(connection, scope.team)
    if frame.empty:
        raise ValueError(f"No team history available for {scope.team}.")

    window = extract_recent_year_window(message) if message else 5
    recent = frame.tail(min(window, len(frame))).reset_index(drop=True)
    latest = recent.iloc[-1]
    previous = recent.iloc[:-1]
    avg_points_proxy = previous["wins"].mean() * 3 + previous["draws"].mean() if not previous.empty else None
    latest_points_proxy = int(latest["wins"]) * 3 + int(latest["draws"])
    answer = (
        f"{scope.team} in {latest['season']} ({latest['league']}, {latest['country']}) has played "
        f"{int(latest['matches_played'])} matches with a {latest['win_rate']:.1f}% win rate, "
        f"{latest['goals_per_match']:.2f} goals scored per match, and {latest['goals_allowed_per_match']:.2f} conceded per match. "
        f"Against its last {len(recent)} tracked seasons, this season's points pace is "
        f"{latest_points_proxy - avg_points_proxy:+.1f} versus the prior average." if avg_points_proxy is not None else
        f"{scope.team} in {latest['season']} ({latest['league']}, {latest['country']}) has played "
        f"{int(latest['matches_played'])} matches with a {latest['win_rate']:.1f}% win rate."
    )

    table = recent[[
        "season",
        "country",
        "league",
        "matches_played",
        "wins",
        "draws",
        "losses",
        "goals_for",
        "goals_against",
        "win_rate",
        "goals_per_match",
        "goals_allowed_per_match",
    ]]
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("team_profile", "Team Performance Scan", f"Aggregated season-by-season performance for {scope.team}."),
            tool_call("aggregate", "Statistical Aggregation", f"Compared the latest season for {scope.team} against its prior tracked seasons."),
        ],
        "highlights": [
            metric("Team", scope.team, f"{latest['country']} · {latest['league']}"),
            metric("Latest season", str(latest["season"]), f"{latest['win_rate']:.1f}% win rate"),
            metric("Goals per match", f"{latest['goals_per_match']:.2f}", "Current season scoring rate"),
        ],
        "table": table_payload(table, float_digits=2),
        "suggested_prompts": [
            f"How has {scope.team} home advantage changed over time?",
            f"What does the last 10-season scoring trend for {scope.team} look like?",
        ],
    }


def data_quality_response(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> dict:
    quality = fetch_data_quality_frame(connection, scope)
    if quality.empty:
        raise ValueError(f"No data-quality profile available for {scope.label}.")

    early, recent = split_windows(quality)
    answer = (
        f"For {scope.label}, data completeness improves over time. "
        f"Average home-shot missingness moves from {early['hs_missing_pct'].mean():.1f}% in the early window "
        f"to {recent['hs_missing_pct'].mean():.1f}% in the recent window."
    )
    table = quality.tail(min(6, len(quality)))[
        ["season", "hs_missing_pct", "hst_missing_pct", "referee_missing_pct", "time_missing_pct"]
    ]
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("overview", "Dataset Snapshot", f"Confirmed the available season coverage for {scope.label}."),
            tool_call("missingness", "Data Quality Scan", "Measured missingness for shots, officiating, and kickoff-time columns."),
        ],
        "highlights": [
            metric("Early shot missingness", f"{early['hs_missing_pct'].mean():.1f}%", "Earliest comparison window"),
            metric("Recent shot missingness", f"{recent['hs_missing_pct'].mean():.1f}%", "Recent comparison window"),
            metric("Recent kickoff-time missingness", f"{recent['time_missing_pct'].mean():.1f}%", scope.label),
        ],
        "table": table_payload(table, float_digits=1),
        "suggested_prompts": [
            "How has scoring moved over time in this slice?",
            "What are the strongest metric correlations here?",
        ],
    }


def correlation_response(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> dict:
    frame = fetch_correlation_frame(connection, scope)
    if frame.empty:
        raise ValueError(f"No correlation-ready data available for {scope.label}.")

    correlation = frame.corr(numeric_only=True)
    pairs: list[dict] = []
    columns = list(correlation.columns)
    for left_index, left_name in enumerate(columns):
        for right_name in columns[left_index + 1 :]:
            value = correlation.loc[left_name, right_name]
            pairs.append(
                {
                    "metric_a": left_name,
                    "metric_b": right_name,
                    "correlation": round(float(value), 3),
                    "abs_correlation": abs(float(value)),
                }
            )
    strongest = pd.DataFrame(pairs).sort_values("abs_correlation", ascending=False).head(8)
    top_pair = strongest.iloc[0]
    answer = (
        f"For {scope.label}, the strongest structural relationship is {top_pair['metric_a']} vs "
        f"{top_pair['metric_b']} at correlation {top_pair['correlation']:.3f}. "
        "This scan is most useful for showing which match-intensity variables travel together inside the selected slice."
    )
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("correlation", "Correlation Analysis", f"Computed pairwise correlations for {scope.label}."),
            tool_call("aggregate", "Statistical Aggregation", "Filtered to rows with sufficient metric coverage before the scan."),
        ],
        "highlights": [
            metric("Top pair", f"{top_pair['metric_a']} ↔ {top_pair['metric_b']}", f"corr {top_pair['correlation']:.3f}"),
            metric("Rows used", format_number(len(frame)), scope.label),
        ],
        "table": table_payload(strongest[["metric_a", "metric_b", "correlation"]], float_digits=3),
        "suggested_prompts": [
            "Compare the leagues on goals and cards in this country.",
            "Show me the data quality profile.",
        ],
    }


def general_overview_response(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> dict:
    rows, seasons = fetch_scope_metrics(connection, scope)
    latest_season = fetch_latest_season(connection, scope, prefer_hyphenated=scope.is_global)
    comparison_scope = QueryScope(country=scope.country, season=scope.season)
    snapshot = fetch_latest_league_snapshot(
        connection,
        comparison_scope,
        latest_season,
        limit=None if comparison_scope.country else 8,
    )

    if scope.league and not snapshot.loc[snapshot["league"] == scope.league].empty:
        focus_row = snapshot.loc[snapshot["league"] == scope.league].iloc[0]
        answer = (
            f"I can analyze {scope.label}. This slice contains {rows:,} matches across {seasons} seasons, "
            f"and the latest tracked season is {latest_season}. In that season, the league is averaging "
            f"{focus_row['avg_goals']:.2f} goals, {focus_row['avg_shots']:.1f} shots, and "
            f"{focus_row['avg_cards']:.2f} cards per match."
        )
        highlights = [
            metric("Slice", scope.label, "Resolved directly from the question"),
            metric("Matches", format_number(rows), f"{seasons} seasons"),
            metric("Latest avg goals", f"{focus_row['avg_goals']:.2f}", latest_season),
        ]
    else:
        top_row = snapshot.sort_values("avg_goals", ascending=False).iloc[0]
        answer = (
            f"I can analyze {scope.label} and run EDA steps before answering. "
            f"This slice contains {rows:,} matches across {seasons} seasons, with latest tracked season {latest_season}. "
            f"In the visible comparison set, the top scoring league is {top_row['league']} in {top_row['country']} "
            f"at {top_row['avg_goals']:.2f} goals per match."
        )
        highlights = [
            metric("Slice", scope.label, "Resolved from the question"),
            metric("Matches", format_number(rows), f"{seasons} seasons"),
            metric("Top-scoring league", f"{top_row['country']} · {top_row['league']}", f"{top_row['avg_goals']:.2f} goals/match"),
        ]

    return {
        "answer": answer,
        "tool_calls": [
            tool_call("overview", "Dataset Snapshot", f"Loaded the current analytical slice for {scope.label}."),
        ],
        "highlights": highlights,
        "table": table_payload(snapshot, float_digits=2),
        "suggested_prompts": PROMPT_CHIPS,
    }


def serialize_numeric(values, digits: int = 3) -> list[float | None]:
    serialized: list[float | None] = []
    for value in values:
        if pd.isna(value):
            serialized.append(None)
        else:
            serialized.append(round(float(value), digits))
    return serialized


def warehouse_sources(scope: QueryScope, rows: int, seasons: int, latest_season: str | None = None) -> list[dict]:
    slice_label = scope.label if not scope.is_global else "the full warehouse"
    latest_suffix = f" Latest season in scope: {latest_season}." if latest_season else ""
    return [
        source_item(
            "DuckDB football warehouse",
            f"Retrieved {rows:,} matches across {seasons} seasons for {slice_label}.{latest_suffix}",
            source_type="warehouse",
        ),
        source_item(
            "football-data.co.uk",
            "Canonical historical and recent football results source ingested into GCS and DuckDB.",
            url="https://www.football-data.co.uk/data.php",
            source_type="source",
        ),
    ]


def out_of_context_payload(message: str, domain: DomainCheck) -> dict:
    answer = (
        "That does not look like a football analytics question. Ask about a football league, team, standings, goals, cards, or season."
    )
    return {
        "answer": answer,
        "tool_calls": [],
        "highlights": [],
        "table": None,
        "suggested_prompts": [
            "Analyze La Liga home advantage.",
            "Compare Spain leagues on goals and cards.",
            "Show the current MLS scoring profile.",
        ],
        "charts": [],
        "hypothesis": None,
        "sources": [],
        "executive_summary": [
            "**Scope:** Football analytics only.",
            "**Next step:** Ask a football question with a league, team, standings, match, or season.",
        ],
        "data_mode": "none",
        "out_of_context": True,
    }


def conversational_payload(message: str) -> dict:
    normalized_message = normalize_text(message)
    if "thank" in normalized_message:
        answer = "You're welcome. Ask a football question when you're ready."
        summary = "**Conversation:** You're welcome. Ask a football question when you're ready."
    elif "how are you" in normalized_message:
        answer = "Ready to help with football analytics. Ask about a league, team, standings, or season."
        summary = "**Conversation:** Ready to help with football analytics."
    else:
        answer = "Hi. Ask me a football question about a league, team, standings, match, or season."
        summary = "**Conversation:** Hi. Ask a football question when you're ready."

    return {
        "answer": answer,
        "tool_calls": [],
        "highlights": [],
        "table": None,
        "suggested_prompts": [],
        "charts": [],
        "hypothesis": None,
        "sources": [],
        "executive_summary": [summary],
        "data_mode": "conversation",
        "out_of_context": False,
        "is_conversational": True,
    }


def simple_football_knowledge_payload(message: str, term: str) -> dict:
    canonical_term = term if term in FOOTBALL_GLOSSARY else term.casefold()
    answer = FOOTBALL_GLOSSARY[canonical_term]
    display_term = canonical_term.title() if canonical_term not in {"offside"} else "Offside"
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("domain_gate", "Domain Validation", "Confirmed the request is football-related."),
            tool_call("external_reasoner", "External Reasoner", "The request is about general football knowledge rather than warehouse match data."),
        ],
        "highlights": [
            metric("Mode", "Football knowledge", display_term),
        ],
        "table": None,
        "suggested_prompts": [
            f"Explain {display_term} with an example.",
            "What is a penalty in football?",
            "What is offside in football?",
        ],
        "charts": [],
        "hypothesis": hypothesis_payload(
            "General football knowledge answer",
            answer,
            [
                f"The request is about the football concept '{display_term}'.",
                "This answer uses general football knowledge rather than warehouse match-level analysis.",
            ],
        ),
        "sources": [],
        "executive_summary": [
            f"**Core finding:** {answer}",
            f"**Mode:** '{display_term}' is a football knowledge question, so the answer came from the external/LLM path instead of DuckDB.",
        ],
        "data_mode": "external_fact",
        "out_of_context": False,
    }


def contextual_suggestions(
    scope: QueryScope | None = None,
    subject: str | None = None,
    team: str | None = None,
    league: str | None = None,
) -> list[str]:
    if team:
        return [
            f"How many goals did {team} score this season?",
            f"What is {team}'s recent form?",
            f"How has {team} performed over the last 5 seasons?",
        ]
    if league:
        return [
            f"How many teams are there in {league}?",
            f"Show the current standings for {league}.",
            f"How has scoring changed over time in {league}?",
        ]
    if scope and scope.country:
        return [
            f"Compare leagues in {scope.country} on goals and cards.",
            f"Show the current top league in {scope.country}.",
            f"How many leagues are tracked for {scope.country}?",
        ]
    if subject == "goals":
        return [
            "How many goals did Arsenal score in the last 5 years?",
            "Which team scored the most goals this season?",
            "How has La Liga scoring changed over time?",
        ]
    return [
        "How many teams are there in La Liga?",
        "Show the current Premier League standings.",
        "Analyze Arsenal's recent form.",
    ]


def classify_count_subject(message: str) -> str | None:
    normalized_message = normalize_text(message)
    if not normalized_message:
        return None
    asks_for_count = any(phrase in normalized_message for phrase in ("how many", "number of", "count of"))
    if not asks_for_count:
        return None
    if "goals" in normalized_message or "goal" in normalized_message:
        return "goals"
    if "teams" in normalized_message or "team" in normalized_message or "clubs" in normalized_message or "club" in normalized_message:
        return "teams"
    if "matches" in normalized_message or "match" in normalized_message or "games" in normalized_message or "game" in normalized_message:
        return "matches"
    if "leagues" in normalized_message or "league" in normalized_message:
        return "leagues"
    if "seasons" in normalized_message or "season" in normalized_message:
        return "seasons"
    return "count"


def fetch_distinct_team_count(
    connection: duckdb.DuckDBPyConnection,
    scope: QueryScope,
    season: str | None = None,
) -> int:
    scoped = QueryScope(country=scope.country, league=scope.league, season=season, team=None)
    where_clause, params = scope_clause(scoped)
    row = connection.execute(
        f"""
        WITH teams AS (
            SELECT hometeam AS team
            FROM matches
            WHERE {where_clause}
            UNION
            SELECT awayteam AS team
            FROM matches
            WHERE {where_clause}
        )
        SELECT count(*) FROM teams
        """,
        [*params, *params],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def count_lookup_payload(connection: duckdb.DuckDBPyConnection, message: str, scope: QueryScope) -> dict:
    subject = classify_count_subject(message) or "count"
    if subject == "goals":
        if scope.team:
            frame = fetch_team_season_frame(connection, scope.team)
            if frame.empty:
                raise ValueError(f"No team history available for {scope.team}.")
            years = extract_recent_year_window(message)
            recent = frame.tail(min(years, len(frame))).reset_index(drop=True)
            total_goals = int(recent["goals_for"].sum())
            first_season = str(recent.iloc[0]["season"])
            last_season = str(recent.iloc[-1]["season"])
            answer = (
                f"{scope.team} scored {total_goals} goals across its last {len(recent)} tracked seasons "
                f"from {first_season} to {last_season}."
            )
            return {
                "answer": answer,
                "tool_calls": [],
                "highlights": [
                    metric("Team", scope.team, f"{len(recent)} tracked seasons"),
                    metric("Goals scored", str(total_goals), f"{first_season} to {last_season}"),
                    metric("Latest season", last_season, recent.iloc[-1]["league"]),
                ],
                "table": table_payload(
                    recent[["season", "league", "goals_for", "matches_played", "goals_per_match"]],
                    float_digits=2,
                ),
                "suggested_prompts": contextual_suggestions(team=scope.team),
                "charts": [],
                "hypothesis": None,
                "sources": [],
                "executive_summary": [],
                "data_mode": "lookup",
                "out_of_context": False,
                "is_conversational": False,
                "is_simple_response": True,
            }
        return {
            "answer": "That goals question needs a specific team or league scope.",
            "tool_calls": [],
            "highlights": [],
            "table": None,
            "suggested_prompts": contextual_suggestions(subject="goals"),
            "charts": [],
            "hypothesis": None,
            "sources": [],
            "executive_summary": [],
            "data_mode": "lookup",
            "out_of_context": False,
            "is_conversational": False,
            "is_simple_response": True,
        }

    if subject == "teams":
        if scope.league:
            target_scope = QueryScope(country=scope.country, league=scope.league, season=scope.season)
            season = scope.season or fetch_latest_season(connection, target_scope, prefer_hyphenated=False)
            team_count = fetch_distinct_team_count(connection, target_scope, season=season)
            label = f"{scope.league} ({scope.country})" if scope.country else scope.league
            answer = f"There are {team_count} teams in {label} in the latest tracked season ({season})."
            return {
                "answer": answer,
                "tool_calls": [],
                "highlights": [
                    metric("League", label, season),
                    metric("Teams", str(team_count), "Distinct clubs in scope"),
                ],
                "table": None,
                "suggested_prompts": contextual_suggestions(league=scope.league),
                "charts": [],
                "hypothesis": None,
                "sources": [],
                "executive_summary": [],
                "data_mode": "lookup",
                "out_of_context": False,
                "is_conversational": False,
                "is_simple_response": True,
            }
        answer = (
            "That needs a scope. Ask for a specific league, for example: "
            "`How many teams are there in La Liga?`"
        )
        return {
            "answer": answer,
            "tool_calls": [],
            "highlights": [],
            "table": None,
            "suggested_prompts": contextual_suggestions(subject="teams"),
            "charts": [],
            "hypothesis": None,
            "sources": [],
            "executive_summary": [],
            "data_mode": "lookup",
            "out_of_context": False,
            "is_conversational": False,
            "is_simple_response": True,
        }

    answer = (
        "That count question needs a clearer football scope. Ask about a specific league, country, team, or season."
    )
    return {
        "answer": answer,
        "tool_calls": [],
        "highlights": [],
        "table": None,
        "suggested_prompts": contextual_suggestions(scope=scope, subject=subject, team=scope.team, league=scope.league),
        "charts": [],
        "hypothesis": None,
        "sources": [],
        "executive_summary": [],
        "data_mode": "lookup",
        "out_of_context": False,
        "is_conversational": False,
        "is_simple_response": True,
    }


def extract_recent_match_window(message: str) -> int:
    normalized = normalize_text(message)
    number_match = re.search(r"\blast\s+(\d{1,2})\b", normalized)
    if number_match:
        return max(1, min(20, int(number_match.group(1))))
    if "last ten" in normalized:
        return 10
    if "last five" in normalized:
        return 5
    return 10


def extract_recent_year_window(message: str) -> int:
    normalized = normalize_text(message)
    number_match = re.search(r"\blast\s+(\d{1,2})\s+(?:years?|seasons?)\b", normalized)
    if number_match:
        return max(1, min(20, int(number_match.group(1))))
    if "last five years" in normalized:
        return 5
    if "last ten years" in normalized:
        return 10
    if "last five seasons" in normalized:
        return 5
    if "last ten seasons" in normalized:
        return 10
    return 5


def recent_team_claim_response(connection: duckdb.DuckDBPyConnection, message: str, scope: QueryScope) -> dict:
    if not scope.team:
        raise ValueError("Recent team claim response requires a team scope.")

    window = extract_recent_match_window(message)
    frame = fetch_recent_team_matches(connection, scope.team, limit=window)
    if frame.empty:
        raise ValueError(f"No recent match history available for {scope.team}.")

    normalized = normalize_text(message)
    all_wins = bool((frame["result"] == "W").all())
    wins = int((frame["result"] == "W").sum())
    draws = int((frame["result"] == "D").sum())
    losses = int((frame["result"] == "L").sum())

    if "won all" in normalized or "won every" in normalized:
        if all_wins:
            answer = f"Yes. {scope.team} won all of its last {len(frame)} matches."
        else:
            answer = (
                f"No. {scope.team} did not win all of its last {len(frame)} matches. "
                f"Across that run, it went {wins} wins, {draws} draws, and {losses} losses."
            )
    else:
        answer = (
            f"In its last {len(frame)} matches, {scope.team} recorded {wins} wins, {draws} draws, and {losses} losses."
        )

    table = frame[[
        "date",
        "season",
        "league",
        "venue",
        "opponent",
        "goals_for",
        "goals_against",
        "result",
    ]]
    return {
        "answer": answer,
        "tool_calls": [],
        "highlights": [
            metric("Window", f"Last {len(frame)} matches", scope.team),
            metric("Wins", str(wins), "Matches won in the run"),
            metric("Draws / losses", f"{draws} / {losses}", "Recent run"),
        ],
        "table": table_payload(table, float_digits=0),
        "suggested_prompts": [
            f"What is {scope.team}'s current league position?",
            f"How has {scope.team} performed this season?",
            f"Show {scope.team}'s recent scoring trend.",
        ],
        "charts": [],
        "hypothesis": None,
        "sources": [],
        "executive_summary": [],
        "data_mode": "lookup",
        "out_of_context": False,
        "is_conversational": False,
        "is_simple_response": True,
    }


def direct_football_clarification_payload(message: str, scope: QueryScope) -> dict:
    if scope.league and scope.country:
        answer = (
            f"I recognized {scope.league} in {scope.country}, but the question does not clearly ask for analytics yet. "
            "Ask for standings, trends, scoring, home advantage, correlations, or a specific count."
        )
        prompts = [
            f"Show the current standings for {scope.league}.",
            f"How has scoring changed over time in {scope.league}?",
            f"How many teams are there in {scope.league}?",
        ]
    elif scope.country:
        answer = (
            f"I recognized the football scope as {scope.country}, but I need a more specific task. "
            "Ask to compare leagues, inspect trends, show standings, or count teams."
        )
        prompts = [
            f"Compare leagues in {scope.country} on goals and cards.",
            f"Show the top league in {scope.country}.",
            f"How many leagues are tracked for {scope.country}?",
        ]
    else:
        answer = (
            "This looks football-related, but it is not specific enough for warehouse analysis. "
            "Ask for a definition, a count, or a concrete analytics task like standings, trends, or comparisons."
        )
        prompts = [
            "What is offside in football?",
            "How many teams are there in La Liga?",
            "Analyze Premier League home advantage.",
        ]

    return {
        "answer": answer,
        "tool_calls": [
            tool_call("domain_gate", "Domain Validation", "Confirmed the request is football-related."),
            tool_call("external_reasoner", "External Reasoner", "The request is football-related but too underspecified for warehouse analysis, so the assistant returned a clarification."),
        ],
        "highlights": [
            metric("Mode", "Football clarification", scope.label),
        ],
        "table": None,
        "suggested_prompts": prompts,
        "charts": [],
        "hypothesis": hypothesis_payload(
            "More football scope is needed before warehouse analysis",
            answer,
            [
                "The request is football-related.",
                "The request does not yet specify enough scope for a useful warehouse analysis run.",
            ],
        ),
        "sources": [],
        "executive_summary": [
            f"**Core finding:** {answer}",
            "**Mode:** The request stayed inside football scope, but it needs a narrower task before warehouse EDA is useful.",
        ],
        "data_mode": "external_fact",
        "out_of_context": False,
    }


def build_web_fallback_charts(bundle: dict) -> list[dict]:
    sources = bundle.get("sources", [])[:5]
    charts: list[dict] = []

    if sources:
        charts.append(
            bar_chart(
                "External source relevance",
                "Shows which crawled football sources contributed the strongest evidence after search and retrieval ranking.",
                [source["title"][:36] for source in sources],
                [{"name": "Relevance score", "data": [round(float(source["score"]), 3) for source in sources]}],
                y_label="Relevance",
            )
        )
    return charts


def infer_web_answer_from_bundle(message: str, bundle: dict, domain: DomainCheck) -> str:
    normalized_message = normalize_text(message)
    snippets = bundle.get("snippets", [])
    combined_text = " ".join(snippet.get("excerpt", "") for snippet in snippets[:4])
    combined_normalized = normalize_text(combined_text)

    league_markers = [
        "premier league",
        "la liga",
        "bundesliga",
        "serie a",
        "ligue 1",
        "mls",
        "champions league",
    ]
    detected_league = next(
        (league for league in league_markers if contains_phrase(combined_normalized, normalize_text(league))),
        None,
    )

    if ("revenue" in normalized_message or "richest" in normalized_message) and detected_league:
        return (
            f"The warehouse does not track revenue, but a quick web search shows that {detected_league.title()} "
            "is the league most commonly identified as generating the highest revenue."
        )
    if ("profit" in normalized_message or "profitable" in normalized_message or "earnings" in normalized_message) and detected_league:
        return (
            f"The warehouse does not track profitability, but a quick web search suggests that {detected_league.title()} "
            "is the league most often cited in discussions of football profitability."
        )

    top_source = bundle["sources"][0]
    top_excerpt = bundle["snippets"][0]["excerpt"] if bundle.get("snippets") else ""
    return (
        f"The warehouse does not cover {domain.external_label or 'this football topic'}, but a quick web search shows "
        f"{clean_external_text(top_excerpt, limit=220) or top_source['title']}."
    )


def summarize_web_bundle(message: str, bundle: dict, domain: DomainCheck) -> tuple[str, list[str], dict | None]:
    sources = bundle.get("sources", [])[:3]
    snippets = bundle.get("snippets", [])[:3]
    top_source = sources[0] if sources else {"title": "External source", "score": 0}
    evidence_lines = [
        f"{snippet.get('title', 'Source')}: {clean_external_text(snippet.get('excerpt', ''), limit=180)}"
        for snippet in snippets
    ]

    if completion is not None:
        system_prompt = """You summarize external football web evidence.

Return strict JSON only with this shape:
{
  "answer": "one direct answer sentence that starts with 'The warehouse does not track ... but web evidence suggests ...'",
  "summary_points": ["short bullet", "short bullet"],
  "hypothesis": {
    "title": "short title",
    "statement": "one sentence",
    "evidence": ["bullet 1", "bullet 2"]
  }
}

Rules:
- Use only the provided evidence.
- Keep it short and concrete.
- Do not mention NFL or non-association-football leagues unless the evidence explicitly requires it.
- If evidence is mixed, say that directly."""
        try:
            response = completion(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": (
                            f"Question: {message}\n"
                            f"Requested external label: {domain.external_label}\n"
                            f"Evidence:\n" + "\n".join(f"- {line}" for line in evidence_lines)
                        ),
                    },
                ],
                temperature=0,
                timeout=MODEL_TIMEOUT_SECONDS,
                api_base=LITELLM_API_BASE or None,
                api_key=LITELLM_API_KEY or None,
            )
            parsed = extract_json_dict(response.choices[0].message.content or "")
            if parsed and parsed.get("answer"):
                hypothesis = parsed.get("hypothesis")
                return (
                    str(parsed["answer"]),
                    [str(item) for item in parsed.get("summary_points", [])],
                    hypothesis if isinstance(hypothesis, dict) else None,
                )
        except Exception:
            pass

    answer = infer_web_answer_from_bundle(message, bundle, domain)
    summary_points = [
        f"Warehouse coverage is missing for {domain.external_label or 'this topic'}, so the answer uses live web evidence.",
        f"Top supporting source: {top_source['title']}.",
    ]
    hypothesis = {
        "title": "External evidence supports a non-warehouse answer",
        "statement": answer,
        "evidence": [
            f"{source['title']}: relevance {source['score']:.3f}" for source in sources[:2]
        ],
    }
    return answer, summary_points, hypothesis


def build_web_fallback_payload(message: str, domain: DomainCheck) -> dict:
    query = domain.external_query or f"{message} football soccer"
    try:
        bundle = build_web_fallback_bundle(message, search_query=query)
    except Exception as exc:
        return {
            "answer": (
                f"The request is in football scope, but the warehouse does not cover {domain.external_label or 'this topic'} "
                "and live web retrieval was unavailable at runtime."
            ),
            "tool_calls": [
                tool_call("domain_gate", "Domain Validation", "Confirmed the request is football-related."),
                tool_call("web_search", "Web Search / Crawl", f"Tried to retrieve external football evidence for {domain.external_label or 'the requested topic'} but the fallback search failed."),
            ],
            "highlights": [
                metric("Mode", "External fallback failed", str(exc)),
            ],
            "table": None,
            "suggested_prompts": PROMPT_CHIPS,
            "charts": [],
            "hypothesis": None,
            "sources": [],
            "data_mode": "web_fallback_failed",
            "out_of_context": False,
        }
    sources = bundle["sources"]
    snippets = bundle["snippets"]
    top_source = sources[0]
    top_excerpt = snippets[0]["excerpt"]
    answer, summary_points, web_hypothesis = summarize_web_bundle(message, bundle, domain)
    evidence = [f"{source['title']}: relevance {source['score']:.3f}" for source in sources[:2]]

    return {
        "answer": answer,
        "tool_calls": [
            tool_call("domain_gate", "Domain Validation", "Confirmed the request is football-related."),
            tool_call("web_search", "Web Search / Crawl", f"Warehouse coverage was missing, so external football sources were queried for {domain.external_label or 'the requested topic'}."),
            tool_call("rag", "Fallback RAG", "Ranked crawled text snippets by relevance before generating the conclusion."),
            tool_call("hypothesis", "Hypothesis Builder", "Converted the retrieved evidence into a grounded football hypothesis."),
        ],
        "highlights": [
            metric("Mode", "External fallback", domain.external_label or "No warehouse slice found"),
            metric("Sources used", format_number(len(sources)), "Crawled and ranked football sources"),
            metric("Top source", top_source["title"], top_source["source_type"]),
        ],
        "table": None,
        "suggested_prompts": [
            "Analyze La Liga home advantage.",
            "Compare Serie A and Bundesliga on goals and cards.",
        ],
        "charts": [],
        "hypothesis": web_hypothesis or hypothesis_payload(
            "External evidence supports a non-warehouse answer",
            answer,
            evidence,
        ),
        "sources": [
            source_item(source["title"], source["snippet"], source.get("url"), source["source_type"])
            for source in sources
        ],
        "executive_summary": [
            f"**Mode:** The warehouse did not cover **{domain.external_label or 'this topic'}**, so the answer switched to external retrieval.",
            *[f"**Summary:** {point}" for point in summary_points[:2]],
            *( [f"**Validation:** {compact_whitespace(top_excerpt)}"] if top_excerpt else [] ),
        ],
        "data_mode": "web_fallback",
        "out_of_context": False,
    }


def build_known_external_payload(message: str, domain: DomainCheck) -> dict | None:
    normalized_message = normalize_text(message)
    if not is_world_cup_titles_query(normalized_message):
        return None

    answer = (
        "Brazil has won the FIFA World Cup the most times, with 5 titles. "
        "Germany and Italy are next with 4 each. If you meant a different FIFA competition, the question needs to be narrowed."
    )
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("domain_gate", "Domain Validation", "Confirmed the request is football-related and outside the league warehouse scope."),
            tool_call("external_fact", "International Football Fact Resolver", "Matched the question to a known FIFA World Cup winners-by-country query."),
        ],
        "highlights": [
            metric("Top country", "Brazil", "Most FIFA World Cup titles"),
            metric("Titles", "5", "World Cup wins"),
            metric("Next closest", "Germany and Italy", "4 titles each"),
        ],
        "table": table_payload(
            pd.DataFrame(
                [
                    {"country": "Brazil", "world_cup_titles": 5},
                    {"country": "Germany", "world_cup_titles": 4},
                    {"country": "Italy", "world_cup_titles": 4},
                    {"country": "Argentina", "world_cup_titles": 3},
                    {"country": "France", "world_cup_titles": 2},
                ]
            ),
            float_digits=0,
        ),
        "suggested_prompts": [
            "Which countries have the most FIFA World Cup finals appearances?",
            "Compare Brazil and Germany in World Cup history.",
        ],
        "charts": [],
        "hypothesis": hypothesis_payload(
            "International football queries should bypass the league warehouse",
            (
                "This question is about FIFA World Cup winners by country, which is not represented by the club-league "
                "warehouse. It should be answered from international football facts instead of warehouse EDA."
            ),
            [
                "Brazil holds the all-time lead with 5 FIFA World Cup titles.",
                "Germany and Italy follow with 4 titles each.",
            ],
        ),
        "sources": [
            source_item(
                "FIFA World Cup overview",
                "Brazil are the most successful men’s World Cup nation with five titles.",
                "https://www.fifa.com/tournaments/mens/worldcup",
                source_type="external_fact",
            ),
            source_item(
                "FIFA World Cup records",
                "Germany and Italy are next on the all-time winners list with four titles each.",
                "https://en.wikipedia.org/wiki/FIFA_World_Cup_records_and_statistics",
                source_type="external_fact",
            ),
        ],
        "executive_summary": [
            "**Core finding:** Brazil has won the FIFA World Cup the most, with 5 titles.",
            "**Why this bypassed warehouse:** The question is about international tournament history, not club-league match data.",
            "**Runner-up countries:** Germany and Italy with 4 titles each.",
        ],
        "data_mode": "external_fact",
        "out_of_context": False,
    }


def should_use_direct_fact_answer(message: str, scope: QueryScope) -> bool:
    normalized = normalize_text(message)
    if any(contains_phrase(normalized, normalize_text(term)) for term in DIRECT_FACT_TIME_SENSITIVE_TERMS):
        return False
    if any(re.search(pattern, normalized) for pattern in PLAYER_FACT_PATTERNS):
        return True
    if scope.team and any(contains_phrase(normalized, normalize_text(term)) for term in UNSUPPORTED_TEAM_FACT_TERMS):
        return True
    if any(contains_phrase(normalized, normalize_text(term)) for term in UNSUPPORTED_GRAIN_TERMS):
        return True
    return False


def fallback_direct_fact_answer(message: str, scope: QueryScope) -> str:
    normalized = normalize_text(message)
    team_key = normalize_text(scope.team or "")
    if (
        scope.team
        and team_key in KNOWN_TEAM_FACTS
        and any(contains_phrase(normalized, normalize_text(term)) for term in ("jersey", "shirt", "kit"))
        and any(contains_phrase(normalized, normalize_text(term)) for term in ("color", "colour"))
    ):
        return KNOWN_TEAM_FACTS[team_key]["jersey_color"]
    if scope.team:
        return (
            f"This is a non-warehouse football fact about {scope.team}. "
            "I do not have a reliable direct fact answer available at runtime."
        )
    return "This is a non-warehouse football fact. I do not have a reliable direct fact answer available at runtime."


def build_direct_fact_payload(message: str, scope: QueryScope, domain: DomainCheck) -> dict:
    answer = fallback_direct_fact_answer(message, scope)
    if completion is not None:
        try:
            response = completion(
                model=MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You answer stable football fact questions directly and concisely. "
                            "Use general football knowledge. Do not talk about warehouses, datasets, or tools. "
                            "If uncertain, say you are not fully certain. Keep the answer to 1-3 sentences."
                        ),
                    },
                    {"role": "user", "content": message},
                ],
                temperature=0,
                timeout=MODEL_TIMEOUT_SECONDS,
                api_base=LITELLM_API_BASE or None,
                api_key=LITELLM_API_KEY or None,
            )
            candidate = compact_whitespace(response.choices[0].message.content or "")
            if candidate:
                answer = candidate
        except Exception:
            pass
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("domain_gate", "Domain Validation", "Confirmed the request is football-related and outside the warehouse schema."),
            tool_call("external_reasoner", "External Reasoner", "Answered the football question through the external/LLM path because the warehouse does not cover this fact."),
        ],
        "highlights": [
            metric("Mode", "External football answer", scope.label if not scope.is_global else (domain.external_label or "External football fact")),
        ],
        "table": None,
        "suggested_prompts": [
            "Which club does Ronaldo play for?",
            "What is Arsenal's home stadium?",
            "Which country has won the FIFA World Cup the most?",
        ],
        "charts": [],
        "hypothesis": hypothesis_payload(
            "This football question bypasses the warehouse",
            answer,
            [
                "The request is football-related.",
                "The requested fact is not represented in the team/league warehouse schema.",
            ],
        ),
        "sources": [],
        "executive_summary": [
            f"**Core finding:** {answer}",
            "**Mode:** The request is football-related but outside DuckDB coverage, so the answer came from the external/LLM path.",
        ],
        "data_mode": "external_fact",
        "out_of_context": False,
        "intent": "external_fact",
        "scope": scope.label if not scope.is_global else (domain.external_label or "external football fact"),
    }


def assess_answerability(message: str, scope: QueryScope, domain: DomainCheck) -> AnswerabilityCheck:
    normalized = normalize_text(message)
    warehouse_scope_present = bool(scope.team or scope.league or scope.country or scope.season)
    count_subject = classify_count_subject(message)
    asks_for_analysis = any(
        term in normalized
        for term in ("analyze", "analysis", "trend", "compare", "profile", "eda", "standings", "table", "correlation")
    )

    if any(re.search(pattern, normalized) for pattern in PLAYER_FACT_PATTERNS):
        return AnswerabilityCheck(
            mode="external_fact",
            reason="The question is about a player-level football fact, which is outside the team/league warehouse grain.",
        )

    if any(contains_phrase(normalized, normalize_text(term)) for term in UNSUPPORTED_GRAIN_TERMS) and not warehouse_scope_present:
        return AnswerabilityCheck(
            mode="external_fact",
            reason="The question asks about football entities or attributes not represented in the warehouse schema.",
        )

    if scope.team and any(contains_phrase(normalized, normalize_text(term)) for term in UNSUPPORTED_TEAM_FACT_TERMS):
        return AnswerabilityCheck(
            mode="external_fact",
            reason="The question asks for a descriptive team fact that is not represented in the warehouse schema.",
        )

    if domain.external_label:
        return AnswerabilityCheck(
            mode="external_fact",
            reason="The question is football-related but points to information outside the warehouse slice.",
        )

    if count_subject and not warehouse_scope_present:
        return AnswerabilityCheck(
            mode="clarify",
            reason="The question asks for a football count, but it needs a clearer league, country, team, or season scope first.",
        )

    if warehouse_scope_present or asks_for_analysis or count_subject:
        return AnswerabilityCheck(
            mode="warehouse",
            reason="The question can be answered from team, league, country, season, or match-level warehouse data.",
        )

    return AnswerabilityCheck(
        mode="clarify",
        reason="The question is football-related but does not clearly map to the warehouse schema or to a known external fact path.",
    )


def build_warehouse_charts(
    connection: duckdb.DuckDBPyConnection,
    scope: QueryScope,
    intent: str,
    message: str = "",
) -> list[dict]:
    if intent == "count_lookup":
        subject = classify_count_subject(message) or "count"
        if subject == "goals" and scope.team:
            trend = fetch_team_season_frame(connection, scope.team)
            if trend.empty:
                return []
            window = extract_recent_year_window(message) if message else 5
            recent = trend.tail(min(window, len(trend))).reset_index(drop=True)
            return [
                bar_chart(
                    f"{scope.team} goals by season",
                    "Shows the exact season totals behind the count answer.",
                    recent["season"].astype(str).tolist(),
                    [{"name": "Goals for", "data": serialize_numeric(recent["goals_for"], digits=0)}],
                    y_label="Goals",
                ),
                line_chart(
                    f"{scope.team} goals per match by season",
                    "Adds rate context so high totals are not confused with longer seasons.",
                    recent["season"].astype(str).tolist(),
                    [{"name": "Goals / match", "data": serialize_numeric(recent["goals_per_match"], digits=2)}],
                    y_label="Goals / match",
                ),
            ]
        if subject == "teams" and scope.league:
            target_scope = QueryScope(country=scope.country, league=scope.league, season=scope.season)
            season = scope.season or fetch_latest_season(connection, target_scope, prefer_hyphenated=False)
            count = fetch_distinct_team_count(connection, target_scope, season=season)
            return [
                bar_chart(
                    f"Team count in {scope.league} ({season})",
                    "Simple league-size view for the resolved latest season in scope.",
                    [scope.league],
                    [{"name": "Teams", "data": [count]}],
                    y_label="Teams",
                ),
            ]
        return []

    if intent == "team_recent_claim" and scope.team:
        frame = fetch_recent_team_matches(connection, scope.team, limit=extract_recent_match_window(message))
        if frame.empty:
            return []
        return [
            bar_chart(
                f"{scope.team} goals for vs against across the recent run",
                "Shows the scoreline pattern match by match for the requested recent window.",
                frame["date"].astype(str).tolist(),
                [
                    {"name": "Goals for", "data": serialize_numeric(frame["goals_for"], digits=0)},
                    {"name": "Goals against", "data": serialize_numeric(frame["goals_against"], digits=0)},
                ],
                y_label="Goals",
            ),
            bar_chart(
                f"{scope.team} recent result mix",
                "Summarizes how many wins, draws, and losses occurred in the selected run.",
                ["Wins", "Draws", "Losses"],
                [{
                    "name": "Matches",
                    "data": [
                        int((frame["result"] == "W").sum()),
                        int((frame["result"] == "D").sum()),
                        int((frame["result"] == "L").sum()),
                    ],
                }],
                y_label="Matches",
            ),
        ]

    if intent == "team_performance":
        trend = fetch_team_season_frame(connection, scope.team or "")
        if trend.empty:
            return []
        window = extract_recent_year_window(message) if message else 5
        recent = trend.tail(min(window, len(trend))).reset_index(drop=True)
        points_proxy = recent["wins"] * 3 + recent["draws"]
        goal_diff = recent["goals_for"] - recent["goals_against"]
        result_mix_heatmap = build_team_result_mix_heatmap(recent)
        team_pressure_map = heatmap_chart(
            f"{scope.team} season pressure map",
            "Shows which seasons stood out on win rate, scoring, concessions, and points proxy inside the selected team-history window.",
            ["Win rate %", "Goals for / match", "Goals against / match", "Points proxy"],
            recent["season"].astype(str).tolist(),
            [
                serialize_numeric(recent["win_rate"], digits=1),
                serialize_numeric(recent["goals_per_match"], digits=2),
                serialize_numeric(recent["goals_allowed_per_match"], digits=2),
                serialize_numeric(points_proxy, digits=0),
            ],
            value_label="value",
        )
        return [
            line_chart(
                f"{scope.team} win rate by season",
                "Shows whether the current season is above or below the club's recent historical range.",
                recent["season"].astype(str).tolist(),
                [{"name": "Win rate", "data": serialize_numeric(recent["win_rate"], digits=1)}],
                y_label="Win rate %",
            ),
            line_chart(
                f"{scope.team} goals for vs against",
                "Compares attacking output and defensive concession trends across the recent team-history window.",
                recent["season"].astype(str).tolist(),
                [
                    {"name": "Goals for / match", "data": serialize_numeric(recent["goals_per_match"], digits=2)},
                    {"name": "Goals against / match", "data": serialize_numeric(recent["goals_allowed_per_match"], digits=2)},
                ],
                y_label="Goals / match",
            ),
            bar_chart(
                f"{scope.team} wins, draws, and losses by season",
                "Breaks the season record into result counts so hypothesis generation can separate attacking strength from consistency.",
                recent["season"].astype(str).tolist(),
                [
                    {"name": "Wins", "data": serialize_numeric(recent["wins"], digits=0)},
                    {"name": "Draws", "data": serialize_numeric(recent["draws"], digits=0)},
                    {"name": "Losses", "data": serialize_numeric(recent["losses"], digits=0)},
                ],
                y_label="Matches",
            ),
            bar_chart(
                f"{scope.team} points proxy by season",
                "Summarizes end-product by season using wins and draws, which is more useful for hypotheses than raw form alone.",
                recent["season"].astype(str).tolist(),
                [{"name": "Points proxy", "data": serialize_numeric(points_proxy, digits=0)}],
                y_label="Points",
            ),
            bar_chart(
                f"{scope.team} goal difference by season",
                "Shows whether performance gains came from stronger attack, stronger defense, or both together.",
                recent["season"].astype(str).tolist(),
                [{"name": "Goal difference", "data": serialize_numeric(goal_diff, digits=0)}],
                y_label="Goal diff",
            ),
            team_pressure_map,
            *( [result_mix_heatmap] if result_mix_heatmap else [] ),
        ]

    trend = fetch_season_trend_frame(connection, scope)
    if intent == "home_advantage" and not trend.empty:
        home_edge = (trend["avg_home_goals"] - trend["avg_away_goals"]).round(2)
        venue_pressure_map = heatmap_chart(
            "Venue pressure map",
            "Puts the main venue-sensitive metrics on one surface so standout seasons are easier to spot than in separate lines alone.",
            ["Home win %", "Away win %", "Draw %", "Home goal edge"],
            trend["season"].astype(str).tolist(),
            [
                serialize_numeric(trend["home_win_rate"], digits=2),
                serialize_numeric(trend["away_win_rate"], digits=2),
                serialize_numeric(trend["draw_rate"], digits=2),
                serialize_numeric(home_edge, digits=2),
            ],
            value_label="value",
        )
        return [
            line_chart(
                "Home and away win rates by season",
                "Tracks whether the venue edge is widening, narrowing, or staying stable over time.",
                trend["season"].astype(str).tolist(),
                [
                    {"name": "Home win %", "data": serialize_numeric(trend["home_win_rate"], digits=2)},
                    {"name": "Away win %", "data": serialize_numeric(trend["away_win_rate"], digits=2)},
                    {"name": "Draw %", "data": serialize_numeric(trend["draw_rate"], digits=2)},
                ],
                y_label="Share %",
            ),
            line_chart(
                "Home and away goals by season",
                "Shows whether the venue effect is supported by scoring separation, not just result outcomes.",
                trend["season"].astype(str).tolist(),
                [
                    {"name": "Home goals", "data": serialize_numeric(trend["avg_home_goals"], digits=2)},
                    {"name": "Away goals", "data": serialize_numeric(trend["avg_away_goals"], digits=2)},
                ],
                y_label="Goals / match",
            ),
            line_chart(
                "Home scoring edge by season",
                "Measures the direct home-minus-away goal gap for each season.",
                trend["season"].astype(str).tolist(),
                [{"name": "Home goal edge", "data": serialize_numeric(home_edge, digits=2)}],
                y_label="Goals",
            ),
            venue_pressure_map,
        ]

    if intent == "scoring" and not trend.empty:
        metric_heatmap = build_metric_heatmap(trend)
        return [
            line_chart(
                "Goals per match by season",
                "Shows whether scoring levels are rising, flat, or falling across the selected history.",
                trend["season"].astype(str).tolist(),
                [
                    {"name": "Total goals", "data": serialize_numeric(trend["avg_total_goals"], digits=2)},
                    {"name": "Home goals", "data": serialize_numeric(trend["avg_home_goals"], digits=2)},
                    {"name": "Away goals", "data": serialize_numeric(trend["avg_away_goals"], digits=2)},
                ],
                y_label="Goals / match",
            ),
            line_chart(
                "Shots and goals by season",
                "Lets you see whether scoring changes are backed by chance volume.",
                trend["season"].astype(str).tolist(),
                [
                    {"name": "Shots", "data": serialize_numeric(trend["avg_shots"], digits=1)},
                    {"name": "Goals", "data": serialize_numeric(trend["avg_total_goals"], digits=2)},
                ],
                y_label="Per match",
            ),
            *( [metric_heatmap] if metric_heatmap else [] ),
        ]

    if intent == "league_compare":
        comparison_scope = QueryScope(country=scope.country, season=scope.season)
        latest_season = fetch_latest_season(connection, comparison_scope, prefer_hyphenated=comparison_scope.is_global)
        snapshot = fetch_latest_league_snapshot(
            connection,
            comparison_scope,
            latest_season,
            limit=None if comparison_scope.country else 8,
        )
        if snapshot.empty:
            return []
        labels = snapshot.apply(
            lambda row: row["league"] if scope.country else f"{row['country']} · {row['league']}",
            axis=1,
        ).tolist()
        return [
            bar_chart(
                f"Goals per match across the comparison set ({latest_season})",
                "Compares how open or conservative each visible league is on scoring output.",
                labels,
                [{"name": "Avg goals", "data": serialize_numeric(snapshot["avg_goals"], digits=2)}],
                y_label="Goals / match",
            ),
            bar_chart(
                f"Cards per match across the comparison set ({latest_season})",
                "Shows discipline and match-intensity differences across the visible leagues.",
                labels,
                [{"name": "Avg cards", "data": serialize_numeric(snapshot["avg_cards"], digits=2)}],
                y_label="Cards / match",
            ),
            bar_chart(
                f"Home-win rate across the comparison set ({latest_season})",
                "Adds venue dominance context to the same league set.",
                labels,
                [{"name": "Home win %", "data": serialize_numeric(snapshot["home_win_rate"], digits=1)}],
                y_label="Home win %",
            ),
        ]

    if intent == "correlation":
        frame = fetch_correlation_frame(connection, scope)
        if frame.empty:
            return []
        subset = frame[[
            "total_goals",
            "total_shots",
            "total_shots_on_target",
            "total_corners",
            "total_cards",
        ]].corr(numeric_only=True)
        strongest = correlation_response(connection, scope)["table"]
        strongest_frame = pd.DataFrame(strongest["rows"], columns=strongest["columns"])
        strongest_frame["correlation"] = strongest_frame["correlation"].astype(float)
        return [
            heatmap_chart(
                "Correlation heatmap",
                "Highlights which match-intensity metrics move together inside the selected slice.",
                subset.index.tolist(),
                subset.columns.tolist(),
                [[round(float(value), 3) for value in row] for row in subset.to_numpy()],
                value_label="corr",
            ),
            bar_chart(
                "Strongest correlation pairs",
                "Ranks the most informative metric relationships after removing duplicate pairs.",
                (strongest_frame["metric_a"] + " ↔ " + strongest_frame["metric_b"]).tolist(),
                [{"name": "Correlation", "data": serialize_numeric(strongest_frame["correlation"], digits=3)}],
                y_label="corr",
            ),
        ]

    if intent == "data_quality":
        quality = fetch_data_quality_frame(connection, scope)
        if quality.empty:
            return []
        missingness_pressure_map = heatmap_chart(
            "Missingness pressure map",
            "Puts the main completeness metrics on one surface so weak seasons stand out immediately.",
            ["HS missing %", "HST missing %", "Referee missing %", "Kickoff time missing %"],
            quality["season"].astype(str).tolist(),
            [
                serialize_numeric(quality["hs_missing_pct"], digits=1),
                serialize_numeric(quality["hst_missing_pct"], digits=1),
                serialize_numeric(quality["referee_missing_pct"], digits=1),
                serialize_numeric(quality["time_missing_pct"], digits=1),
            ],
            value_label="missing %",
        )
        return [
            line_chart(
                "Shot data missingness by season",
                "Shows how missingness in shot-level columns changes from older to more recent seasons.",
                quality["season"].astype(str).tolist(),
                [
                    {"name": "HS missing %", "data": serialize_numeric(quality["hs_missing_pct"], digits=1)},
                    {"name": "HST missing %", "data": serialize_numeric(quality["hst_missing_pct"], digits=1)},
                ],
                y_label="Missing %",
            ),
            line_chart(
                "Officiating and kickoff metadata missingness",
                "Tracks the completeness of referee names and kickoff-time metadata over time.",
                quality["season"].astype(str).tolist(),
                [
                    {"name": "Referee missing %", "data": serialize_numeric(quality["referee_missing_pct"], digits=1)},
                    {"name": "Kickoff time missing %", "data": serialize_numeric(quality["time_missing_pct"], digits=1)},
                ],
                y_label="Missing %",
            ),
            missingness_pressure_map,
        ]

    if intent == "overview":
        comparison_scope = QueryScope(country=scope.country, season=scope.season)
        latest_season = fetch_latest_season(connection, comparison_scope, prefer_hyphenated=comparison_scope.is_global)
        snapshot = fetch_latest_league_snapshot(
            connection,
            comparison_scope,
            latest_season,
            limit=None if comparison_scope.country else 8,
        )
        charts: list[dict] = []
        if not snapshot.empty and latest_season:
            labels = snapshot.apply(
                lambda row: row["league"] if scope.country else f"{row['country']} · {row['league']}",
                axis=1,
            ).tolist()
            charts.append(
            bar_chart(
                f"Latest scoring snapshot ({latest_season})",
                "Places the requested slice against the latest visible league comparison set.",
                labels,
                [{"name": "Avg goals", "data": serialize_numeric(snapshot["avg_goals"], digits=2)}],
                y_label="Goals / match",
            ),
            )
        if not trend.empty:
            charts.append(
                line_chart(
                    "Goals per match by season",
                    "High-level scoring trend for the selected slice.",
                    trend["season"].astype(str).tolist(),
                    [{"name": "Total goals", "data": serialize_numeric(trend["avg_total_goals"], digits=2)}],
                    y_label="Goals / match",
                )
            )
        return charts[:3]

    return []


def build_warehouse_hypothesis(connection: duckdb.DuckDBPyConnection, scope: QueryScope, intent: str) -> dict | None:
    if intent == "count_lookup":
        if scope.team:
            trend = fetch_team_season_frame(connection, scope.team)
            if trend.empty:
                return None
            window = trend.tail(min(5, len(trend))).reset_index(drop=True)
            return hypothesis_payload(
                f"{scope.team} scoring count should be read in seasonal context",
                f"The requested count for {scope.team} is grounded in {len(window)} tracked seasons of warehouse history.",
                [
                    f"Latest season in the comparison window: {window.iloc[-1]['season']}.",
                    f"Goals across the visible window: {int(window['goals_for'].sum())}.",
                    "The count is descriptive of the selected history rather than a forward projection.",
                ],
            )
        if scope.league:
            target_scope = QueryScope(country=scope.country, league=scope.league, season=scope.season)
            season = scope.season or fetch_latest_season(connection, target_scope, prefer_hyphenated=False)
            count = fetch_distinct_team_count(connection, target_scope, season=season)
            return hypothesis_payload(
                f"{scope.league} league size is directly resolved from the latest season in scope",
                f"The warehouse shows {count} distinct teams in {scope.league} for {season}.",
                [
                    f"Country scope: {scope.country or 'resolved from league name'}.",
                    f"Season used: {season}.",
                    "This is a direct warehouse count, not an inferred estimate.",
                ],
            )
        return None

    if intent == "team_recent_claim" and scope.team:
        frame = fetch_recent_team_matches(connection, scope.team, limit=10)
        if frame.empty:
            return None
        wins = int((frame["result"] == "W").sum())
        draws = int((frame["result"] == "D").sum())
        losses = int((frame["result"] == "L").sum())
        return hypothesis_payload(
            f"{scope.team} recent form can be described directly from the latest match window",
            f"In the recent run, {scope.team} posted {wins} wins, {draws} draws, and {losses} losses.",
            [
                f"Recent window size: {len(frame)} matches.",
                f"Goals for vs against in that run: {int(frame['goals_for'].sum())} vs {int(frame['goals_against'].sum())}.",
                "This is a descriptive recent-form read, not a forecast.",
            ],
        )

    if intent == "team_performance":
        trend = fetch_team_season_frame(connection, scope.team or "")
        if trend.empty:
            return None
        recent = trend.tail(min(10, len(trend))).reset_index(drop=True)
        latest = recent.iloc[-1]
        baseline = recent.iloc[:-1]
        baseline_win_rate = float(baseline["win_rate"].mean()) if not baseline.empty else float(latest["win_rate"])
        return hypothesis_payload(
            f"{scope.team} is {'outperforming' if float(latest['win_rate']) >= baseline_win_rate else 'running below'} its recent norm",
            (
                f"The current {scope.team} season can be judged against its recent historical band using win rate and scoring balance."
            ),
            [
                f"Latest season: {latest['season']} with {latest['win_rate']:.1f}% win rate.",
                f"Prior average across the comparison window: {baseline_win_rate:.1f}%.",
                f"Goals for vs against this season: {latest['goals_per_match']:.2f} scored and {latest['goals_allowed_per_match']:.2f} conceded per match.",
            ],
        )

    if intent == "home_advantage":
        trend = fetch_season_trend_frame(connection, scope)
        if trend.empty:
            return None
        early, recent = split_windows(trend)
        early_home = float(early["home_win_rate"].mean())
        recent_home = float(recent["home_win_rate"].mean())
        early_away = float(early["away_win_rate"].mean())
        recent_away = float(recent["away_win_rate"].mean())
        return hypothesis_payload(
            "Home edge persists, but away sides have narrowed the margin",
            (
                f"In {scope.label}, the venue edge still exists, but recent seasons show a weaker separation between home and away outcomes "
                f"than the earliest comparison window."
            ),
            [
                f"Home-win average moved from {early_home:.1f}% to {recent_home:.1f}%.",
                f"Away-win average moved from {early_away:.1f}% to {recent_away:.1f}%.",
                f"The supporting chart tests whether that compression is steady or season-specific.",
            ],
        )

    if intent == "league_compare":
        comparison_scope = QueryScope(country=scope.country, season=scope.season)
        latest_season = fetch_latest_season(connection, comparison_scope, prefer_hyphenated=comparison_scope.is_global)
        snapshot = fetch_latest_league_snapshot(
            connection,
            comparison_scope,
            latest_season,
            limit=None if comparison_scope.country else 8,
        )
        if snapshot.empty:
            return None
        top_goals = snapshot.sort_values("avg_goals", ascending=False).iloc[0]
        low_cards = snapshot.sort_values("avg_cards", ascending=True).iloc[0]
        return hypothesis_payload(
            "Scoring intensity and discipline do not peak in the same league",
            (
                f"In the latest comparison set for {scope.country or 'the warehouse'}, the most open scoring environment "
                f"is not the same league as the cleanest discipline profile."
            ),
            [
                f"Highest scoring: {top_goals['league']} at {top_goals['avg_goals']:.2f} goals per match.",
                f"Most disciplined: {low_cards['league']} at {low_cards['avg_cards']:.2f} cards per match.",
                f"That split suggests stylistic differences rather than one league leading every metric.",
            ],
        )

    if intent == "correlation":
        frame = fetch_correlation_frame(connection, scope)
        if frame.empty:
            return None
        correlation = frame.corr(numeric_only=True)
        total_goal_corr = float(correlation.loc["total_goals", "total_shots_on_target"])
        return hypothesis_payload(
            "Shot quality metrics explain scoring better than broad match-chaos metrics",
            (
                f"For {scope.label}, scoring is more tightly linked to shot-quality volume than to corners or cards."
            ),
            [
                f"Correlation(total_goals, total_shots_on_target) = {total_goal_corr:.3f}.",
                "The heatmap shows whether that relationship dominates the broader metric set.",
                "If corner and card correlations stay lower, finishing volume is the stronger explanatory factor.",
            ],
        )

    if intent == "data_quality":
        quality = fetch_data_quality_frame(connection, scope)
        if quality.empty:
            return None
        early, recent = split_windows(quality)
        return hypothesis_payload(
            "Modern seasons support richer analysis because metadata completeness improved",
            (
                f"The more recent seasons in {scope.label} are materially more analysis-ready than the earliest seasons."
            ),
            [
                f"Home-shot missingness moves from {early['hs_missing_pct'].mean():.1f}% to {recent['hs_missing_pct'].mean():.1f}%.",
                f"Kickoff-time missingness in the recent window averages {recent['time_missing_pct'].mean():.1f}%.",
                "That improvement determines which EDA layers are trustworthy across the full timeline.",
            ],
        )

    trend = fetch_season_trend_frame(connection, scope)
    if trend.empty:
        return None
    early, recent = split_windows(trend)
    return hypothesis_payload(
        "Scoring volume has shifted with shot volume, not independently of it",
        (
            f"For {scope.label}, movement in goals per match should be read together with movement in shot volume."
        ),
        [
            f"Goals per match moved from {early['avg_total_goals'].mean():.2f} to {recent['avg_total_goals'].mean():.2f}.",
            f"Shots per match moved from {early['avg_shots'].mean():.1f} to {recent['avg_shots'].mean():.1f}.",
            "The charts test whether finishing changes came from chance creation or conversion alone.",
        ],
    )


def aggregate_specialist_task(duckdb_path: str, scope: QueryScope) -> dict:
    connection = open_connection(duckdb_path)
    try:
        trend = fetch_season_trend_frame(connection, scope)
        if trend.empty:
            raise ValueError(f"No aggregate-ready data available for {scope.label}.")
        early, recent = split_windows(trend)
        latest = trend.iloc[-1]
        return {
            "key": "aggregate",
            "label": "Trend Detector Sub-agent",
            "summary": (
                f"Scanned {len(trend)} seasons for {scope.label}; latest season {latest['season']} has "
                f"{latest['avg_total_goals']:.2f} goals and {latest['home_win_rate']:.1f}% home-win rate."
            ),
            "chart": line_chart(
                "Aggregate specialist: goals and home-win trend",
                "The trend detector checks whether scoring and venue advantage move together across seasons.",
                trend["season"].astype(str).tolist(),
                [
                    {"name": "Goals per match", "data": serialize_numeric(trend["avg_total_goals"], digits=2)},
                    {"name": "Home win rate", "data": serialize_numeric(trend["home_win_rate"], digits=2)},
                ],
                y_label="Goals / % rate",
            ),
            "table": table_payload(
                trend.tail(min(6, len(trend)))[["season", "avg_total_goals", "avg_shots", "home_win_rate"]],
                float_digits=2,
            ),
            "highlights": [
                metric("Latest season", str(latest["season"]), scope.label),
                metric("Goal shift", f"{recent['avg_total_goals'].mean() - early['avg_total_goals'].mean():+.2f}", "Recent vs early window"),
            ],
        }
    finally:
        connection.close()


def segment_specialist_task(duckdb_path: str, scope: QueryScope) -> dict:
    connection = open_connection(duckdb_path)
    try:
        comparison_scope = QueryScope(country=scope.country, season=scope.season)
        latest_season = fetch_latest_season(connection, comparison_scope, prefer_hyphenated=comparison_scope.is_global)
        snapshot = fetch_latest_league_snapshot(
            connection,
            comparison_scope,
            latest_season,
            limit=None if comparison_scope.country else 8,
        )
        if snapshot.empty:
            raise ValueError(f"No segmentation-ready data available for {scope.label}.")
        labels = snapshot.apply(
            lambda row: row["league"] if scope.country else f"{row['country']} · {row['league']}",
            axis=1,
        ).tolist()
        top_row = snapshot.sort_values("avg_goals", ascending=False).iloc[0]
        return {
            "key": "segment",
            "label": "Comparison Analyst Sub-agent",
            "summary": (
                f"Segmented the latest comparison set for {scope.country or 'the warehouse'} in {latest_season}; "
                f"{top_row['league']} leads the visible scoring table."
            ),
            "chart": bar_chart(
                f"Comparison specialist: scoring by league ({latest_season})",
                "The comparison analyst groups the visible leagues so the answer can place the target slice in context.",
                labels,
                [{"name": "Goals per match", "data": serialize_numeric(snapshot["avg_goals"], digits=2)}],
                y_label="Goals / match",
            ),
            "table": table_payload(snapshot, float_digits=2),
            "highlights": [
                metric("Comparison season", latest_season, scope.country or "Warehouse snapshot"),
                metric("Top scoring league", top_row["league"], f"{top_row['avg_goals']:.2f} goals/match"),
            ],
        }
    finally:
        connection.close()


def correlation_specialist_task(duckdb_path: str, scope: QueryScope) -> dict:
    connection = open_connection(duckdb_path)
    try:
        frame = fetch_correlation_frame(connection, scope)
        if frame.empty:
            raise ValueError(f"No correlation-ready data available for {scope.label}.")
        subset = frame[[
            "total_goals",
            "total_shots",
            "total_shots_on_target",
            "total_corners",
            "total_cards",
        ]].corr(numeric_only=True)
        pairs: list[dict] = []
        columns = list(subset.columns)
        for left_index, left_name in enumerate(columns):
            for right_name in columns[left_index + 1 :]:
                value = subset.loc[left_name, right_name]
                pairs.append(
                    {
                        "metric_a": left_name,
                        "metric_b": right_name,
                        "correlation": round(float(value), 3),
                        "abs_correlation": abs(float(value)),
                    }
                )
        strongest = pd.DataFrame(pairs).sort_values("abs_correlation", ascending=False).head(6)
        top_pair = strongest.iloc[0]
        return {
            "key": "correlation",
            "label": "Relationship Analyst Sub-agent",
            "summary": (
                f"Computed pairwise metric relationships for {scope.label}; strongest pair is "
                f"{top_pair['metric_a']} ↔ {top_pair['metric_b']} at {top_pair['correlation']:.3f}."
            ),
            "chart": heatmap_chart(
                "Correlation specialist: metric relationship heatmap",
                "The relationship analyst checks which match-intensity variables travel together inside the selected slice.",
                subset.index.tolist(),
                subset.columns.tolist(),
                [[round(float(value), 3) for value in row] for row in subset.to_numpy()],
                value_label="corr",
            ),
            "table": table_payload(strongest[["metric_a", "metric_b", "correlation"]], float_digits=3),
            "highlights": [
                metric("Top pair", f"{top_pair['metric_a']} ↔ {top_pair['metric_b']}", f"corr {top_pair['correlation']:.3f}"),
                metric("Rows used", format_number(len(frame)), scope.label),
            ],
        }
    finally:
        connection.close()


def quality_specialist_task(duckdb_path: str, scope: QueryScope) -> dict:
    connection = open_connection(duckdb_path)
    try:
        quality = fetch_data_quality_frame(connection, scope)
        if quality.empty:
            raise ValueError(f"No data-quality profile available for {scope.label}.")
        early, recent = split_windows(quality)
        return {
            "key": "quality",
            "label": "Coverage Analyst Sub-agent",
            "summary": (
                f"Measured column completeness across {len(quality)} seasons for {scope.label}; "
                f"home-shot missingness moves from {early['hs_missing_pct'].mean():.1f}% to {recent['hs_missing_pct'].mean():.1f}%."
            ),
            "chart": line_chart(
                "Coverage specialist: missingness by season",
                "The coverage analyst checks whether the relevant metrics are complete enough to support the conclusion.",
                quality["season"].astype(str).tolist(),
                [
                    {"name": "HS missing %", "data": serialize_numeric(quality["hs_missing_pct"], digits=1)},
                    {"name": "HST missing %", "data": serialize_numeric(quality["hst_missing_pct"], digits=1)},
                ],
                y_label="Missing %",
            ),
            "table": table_payload(
                quality.tail(min(6, len(quality)))[["season", "hs_missing_pct", "hst_missing_pct", "referee_missing_pct", "time_missing_pct"]],
                float_digits=1,
            ),
            "highlights": [
                metric("Recent shot missingness", f"{recent['hs_missing_pct'].mean():.1f}%", scope.label),
                metric("Recent kickoff-time missingness", f"{recent['time_missing_pct'].mean():.1f}%", "Recent window"),
            ],
        }
    finally:
        connection.close()


def extract_json_dict(value: str) -> dict | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None


def profile_scope_data(connection: duckdb.DuckDBPyConnection, scope: QueryScope) -> dict:
    where_clause, params = scope_clause(scope)
    row_count, season_count = fetch_scope_metrics(connection, scope)
    schema_rows = connection.execute("DESCRIBE matches").fetchall()
    sample = connection.execute(
        f"""
        SELECT *
        FROM matches
        WHERE {where_clause}
        LIMIT 400
        """,
        params,
    ).df()

    numeric_columns: list[dict] = []
    categorical_columns: list[dict] = []
    datetime_columns: list[dict] = []
    text_columns: list[dict] = []
    top_missing: list[dict] = []

    for column_name, column_type, *_ in schema_rows:
        identifier = sql_identifier(str(column_name))
        null_count = connection.execute(
            f"SELECT count(*) FROM matches WHERE {where_clause} AND {identifier} IS NULL",
            params,
        ).fetchone()[0]
        null_rate = (float(null_count) / row_count * 100) if row_count else 0.0
        distinct_count = connection.execute(
            f"SELECT count(DISTINCT {identifier}) FROM matches WHERE {where_clause}",
            params,
        ).fetchone()[0]
        profile_row = {
            "name": str(column_name),
            "type": str(column_type),
            "null_rate": round(null_rate, 1),
            "distinct_count": int(distinct_count or 0),
        }
        top_missing.append(profile_row)

        normalized_type = str(column_type).casefold()
        if any(token in normalized_type for token in ("int", "double", "float", "decimal", "bigint")):
            stats = connection.execute(
                f"""
                SELECT
                    min({identifier}),
                    quantile_cont({identifier}, 0.25),
                    median({identifier}),
                    quantile_cont({identifier}, 0.75),
                    max({identifier}),
                    stddev_pop({identifier})
                FROM matches
                WHERE {where_clause}
                  AND {identifier} IS NOT NULL
                """,
                params,
            ).fetchone()
            numeric_columns.append(
                {
                    **profile_row,
                    "min": round(float(stats[0]), 3) if stats and stats[0] is not None else None,
                    "p25": round(float(stats[1]), 3) if stats and stats[1] is not None else None,
                    "median": round(float(stats[2]), 3) if stats and stats[2] is not None else None,
                    "p75": round(float(stats[3]), 3) if stats and stats[3] is not None else None,
                    "max": round(float(stats[4]), 3) if stats and stats[4] is not None else None,
                    "stddev": round(float(stats[5]), 3) if stats and stats[5] is not None else None,
                }
            )
        elif column_name == "date":
            parsed_rate = connection.execute(
                f"""
                SELECT avg(CASE WHEN try_strptime(date, '%d/%m/%Y') IS NOT NULL THEN 1 ELSE 0 END) * 100
                FROM matches
                WHERE {where_clause}
                """,
                params,
            ).fetchone()[0]
            datetime_columns.append(
                {
                    **profile_row,
                    "parse_success_rate": round(float(parsed_rate or 0), 1),
                }
            )
        elif profile_row["distinct_count"] <= 30:
            top_values = connection.execute(
                f"""
                SELECT {identifier} AS value, count(*) AS rows
                FROM matches
                WHERE {where_clause}
                  AND {identifier} IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC, 1
                LIMIT 5
                """,
                params,
            ).fetchall()
            categorical_columns.append(
                {
                    **profile_row,
                    "top_values": [{"value": str(value), "rows": int(rows)} for value, rows in top_values],
                }
            )
        else:
            sample_values = []
            if column_name in sample.columns:
                sample_values = [
                    str(value)
                    for value in sample[column_name].dropna().astype(str).head(3).tolist()
                ]
            text_columns.append({**profile_row, "sample_values": sample_values})

    return {
        "row_count": row_count,
        "season_count": season_count,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "datetime_columns": datetime_columns,
        "text_columns": text_columns,
        "top_missing": sorted(top_missing, key=lambda item: item["null_rate"], reverse=True)[:6],
    }


def profile_tool_payload(profile: dict, scope: QueryScope) -> dict:
    numeric_count = len(profile["numeric_columns"])
    categorical_count = len(profile["categorical_columns"])
    datetime_count = len(profile["datetime_columns"])
    top_missing = profile["top_missing"][:5]
    summary = (
        f"Profiled {profile['row_count']:,} rows for {scope.label} across {profile['season_count']} seasons. "
        f"Detected {numeric_count} numeric, {categorical_count} low-cardinality categorical, and "
        f"{datetime_count} datetime-like columns before choosing EDA branches."
    )
    frame = pd.DataFrame(
        [
            {
                "column": item["name"],
                "type": item["type"],
                "null_rate_pct": item["null_rate"],
                "distinct": item["distinct_count"],
            }
            for item in top_missing
        ]
    )
    return {
        "key": "profile",
        "label": "Schema Profiler",
        "summary": summary,
        "chart": bar_chart(
            "Highest-null columns in scope",
            "The profile step ranks the sparsest columns before downstream EDA decides what is safe to use.",
            [item["name"] for item in top_missing],
            [{"name": "Null rate %", "data": [item["null_rate"] for item in top_missing]}],
            y_label="Null %",
        ) if top_missing else None,
        "table": table_payload(frame, float_digits=1) if not frame.empty else None,
        "highlights": [
            metric("Rows in scope", format_number(profile["row_count"]), f"{profile['season_count']} seasons"),
            metric("Numeric columns", format_number(numeric_count), "Available for percentile and variance scans"),
            metric("Datetime columns", format_number(datetime_count), "Available for trend analysis"),
        ],
        "evidence_objects": [
            {
                "source_step": "profile",
                "kind": "highlight",
                "label": "Rows in scope",
                "value": format_number(profile["row_count"]),
                "detail": f"{profile['season_count']} seasons",
            },
            *[
                {
                    "source_step": "profile",
                    "kind": "missing_column",
                    "label": item["name"],
                    "value": f"{item['null_rate']:.1f}%",
                    "detail": f"{item['distinct_count']} distinct",
                }
                for item in top_missing[:2]
            ],
        ],
    }


def run_framework_planner_decision(
    profile: dict,
    intent: str,
    completed_steps: list[str],
    prior_summaries: list[str],
) -> str | None:
    model = build_agno_model(temperature=0)
    if model is None:
        return None
    planner_prompt = (
        "You are the EDA Planner Agent. Pick the single best next step from the allowed set. "
        "Avoid repeating completed steps and prefer the smallest useful plan."
    )
    planner_agent = AgnoAgent(
        model=model,
        name="EDA Planner Agent",
        instructions=planner_prompt,
        response_model=EdaPlannerDecision,
        use_json_mode=True,
    )
    planner_input = {
        "intent": intent,
        "completed_steps": completed_steps,
        "profile": {
            "row_count": profile["row_count"],
            "season_count": profile["season_count"],
            "numeric_columns": [
                {
                    "name": item["name"],
                    "null_rate": item["null_rate"],
                    "stddev": item.get("stddev"),
                }
                for item in profile["numeric_columns"][:10]
            ],
            "categorical_columns": profile["categorical_columns"][:8],
            "datetime_columns": profile["datetime_columns"][:4],
            "top_missing": profile["top_missing"][:6],
        },
        "prior_summaries": prior_summaries,
        "allowed_steps": ["trend", "segment", "correlation", "quality", "distribution", "stop"],
    }
    try:
        response = planner_agent.run(json.dumps(planner_input))
        content = extract_agno_content(response)
    except Exception:
        return None
    if isinstance(content, EdaPlannerDecision):
        next_step = content.next_step
    elif isinstance(content, dict):
        next_step = str(content.get("next_step", "")).strip()
    else:
        next_step = ""
    return None if next_step == "stop" else next_step if next_step in {"trend", "segment", "correlation", "quality", "distribution"} else None


def suggest_eda_step(profile: dict, intent: str, completed_steps: list[str], prior_summaries: list[str]) -> str | None:
    if framework_agents_enabled():
        framework_step = run_framework_planner_decision(profile, intent, completed_steps, prior_summaries)
        if framework_step:
            return framework_step
    elif completion is not None:
        planner_prompt = """You choose the next EDA step.

Allowed steps:
- trend
- segment
- correlation
- quality
- distribution
- stop

Return strict JSON only: {"next_step":"one_allowed_step","reason":"short sentence"}"""
        planner_input = {
            "intent": intent,
            "completed_steps": completed_steps,
            "profile": {
                "row_count": profile["row_count"],
                "season_count": profile["season_count"],
                "numeric_columns": [
                    {
                        "name": item["name"],
                        "null_rate": item["null_rate"],
                        "stddev": item.get("stddev"),
                    }
                    for item in profile["numeric_columns"][:10]
                ],
                "categorical_columns": profile["categorical_columns"][:8],
                "datetime_columns": profile["datetime_columns"][:4],
                "top_missing": profile["top_missing"][:6],
            },
            "prior_summaries": prior_summaries,
        }
        try:
            response = completion(
                model=MODEL,
                messages=[
                    {"role": "system", "content": planner_prompt},
                    {"role": "user", "content": str(planner_input)},
                ],
                temperature=0,
                timeout=MODEL_TIMEOUT_SECONDS,
                api_base=LITELLM_API_BASE or None,
                api_key=LITELLM_API_KEY or None,
            )
            parsed = extract_json_dict(response.choices[0].message.content or "")
            next_step = (parsed or {}).get("next_step")
            if next_step in {"trend", "segment", "correlation", "quality", "distribution", "stop"}:
                return None if next_step == "stop" else str(next_step)
        except Exception:
            pass

    if "trend" not in completed_steps and profile["datetime_columns"] and profile["season_count"] > 1:
        return "trend"
    if "quality" not in completed_steps and any(item["null_rate"] >= 5 for item in profile["top_missing"]):
        return "quality"
    high_variance_numeric = [
        item for item in profile["numeric_columns"]
        if item.get("stddev") is not None and item["null_rate"] < 40
    ]
    if "distribution" not in completed_steps and high_variance_numeric:
        return "distribution"
    if "segment" not in completed_steps and (profile["categorical_columns"] or intent in {"league_compare", "overview"}):
        return "segment"
    if "correlation" not in completed_steps and len(high_variance_numeric) >= 3:
        return "correlation"
    return None


def distribution_specialist_payload(scope: QueryScope, profile: dict) -> dict:
    candidates = [
        item
        for item in profile["numeric_columns"]
        if item.get("stddev") not in (None, 0) and item["null_rate"] < 40
    ]
    if not candidates:
        raise ValueError(f"No distribution-ready numeric columns available for {scope.label}.")
    candidates.sort(key=lambda item: (item.get("stddev") or 0, item["distinct_count"]), reverse=True)
    chosen = candidates[:3]
    frame = pd.DataFrame(
        [
            {
                "column": item["name"],
                "min": item.get("min"),
                "p25": item.get("p25"),
                "median": item.get("median"),
                "p75": item.get("p75"),
                "max": item.get("max"),
                "stddev": item.get("stddev"),
                "null_rate_pct": item["null_rate"],
            }
            for item in chosen
        ]
    )
    widest = chosen[0]
    return {
        "key": "distribution",
        "label": "Distribution Analyst",
        "summary": (
            f"Ran percentile and spread checks for {scope.label}. "
            f"{widest['name']} has the widest usable spread with stddev {widest.get('stddev', 0):.2f}."
        ),
        "chart": bar_chart(
            "Highest-variance numeric columns",
            "The distribution step looks for columns where percentile spread or variance justifies deeper analysis.",
            [item["name"] for item in chosen],
            [{"name": "Stddev", "data": [round(float(item.get("stddev") or 0), 3) for item in chosen]}],
            y_label="Stddev",
        ),
        "table": table_payload(frame, float_digits=2),
        "highlights": [
            metric("Widest spread", widest["name"], f"stddev {widest.get('stddev', 0):.2f}"),
            metric("Median", format_number(widest.get("median"), 2), f"{widest['name']}"),
        ],
    }


def segment_specialist_payload(
    duckdb_path: str,
    scope: QueryScope,
    profile: dict,
) -> dict:
    categorical = [
        item
        for item in profile["categorical_columns"]
        if item["name"] in {"ftr", "league", "country", "season"}
    ]
    if categorical:
        chosen = categorical[0]
        frame = pd.DataFrame(chosen.get("top_values", []))
        return {
            "key": "segment",
            "label": "Category Frequency Scan",
            "summary": (
                f"Ranked low-cardinality values for {chosen['name']} inside {scope.label} to anchor the next branch."
            ),
            "chart": bar_chart(
                f"Top values for {chosen['name']}",
                "Low-cardinality categories are summarized before broader comparisons are attempted.",
                [str(item["value"]) for item in chosen.get("top_values", [])],
                [{"name": "Rows", "data": [int(item["rows"]) for item in chosen.get("top_values", [])]}],
                y_label="Rows",
            ) if chosen.get("top_values") else None,
            "table": table_payload(frame, float_digits=0) if not frame.empty else None,
            "highlights": [
                metric("Category column", chosen["name"], f"{chosen['distinct_count']} distinct values"),
            ],
        }
    return segment_specialist_task(duckdb_path, scope)


def eda_step_error_payload(step_name: str, exc: Exception) -> dict:
    return {
        "key": step_name,
        "label": f"{step_name.title()} Analyst",
        "summary": f"{step_name.title()} branch skipped: {exc}",
        "chart": None,
        "table": None,
        "highlights": [],
    }


def run_framework_specialist_agent(
    step_name: str,
    runner,
    scope: QueryScope,
    intent: str,
) -> dict:
    payload_cache: dict[str, dict] = {}

    def specialist_tool(**_: object) -> dict:
        if "payload" not in payload_cache:
            payload = runner()
            payload["evidence_objects"] = payload.get("evidence_objects") or build_payload_evidence_objects(step_name, payload)
            payload_cache["payload"] = payload
        payload = payload_cache["payload"]
        return {
            "label": payload.get("label"),
            "summary": payload.get("summary"),
            "highlights": payload.get("highlights", []),
            "table": payload.get("table"),
            "evidence_objects": payload.get("evidence_objects", []),
        }

    specialist_tool.__name__ = f"run_{step_name}_specialist_tool"
    model = build_agno_model(temperature=0)
    if model is None:
        payload = runner()
        payload["evidence_objects"] = payload.get("evidence_objects") or build_payload_evidence_objects(step_name, payload)
        return payload

    agent = AgnoAgent(
        model=model,
        name=f"{step_name.title()} Specialist Agent",
        instructions=SPECIALIST_AGENT_INSTRUCTIONS[step_name],
        tools=[specialist_tool],
        response_model=SpecialistDigest,
        use_json_mode=True,
    )
    prompt = (
        f"Intent: {intent}\n"
        f"Scope: {scope.label}\n"
        "Call the specialist tool exactly once. Then return JSON only with a grounded claim, "
        "2-3 evidence points, and caveats. Keep it faithful to the tool output."
    )
    try:
        response = agent.run(prompt)
        digest_content = extract_agno_content(response)
    except Exception:
        digest_content = None
    payload = payload_cache.get("payload")
    if payload is None:
        payload = runner()
        payload["evidence_objects"] = payload.get("evidence_objects") or build_payload_evidence_objects(step_name, payload)

    digest: SpecialistDigest | None = None
    if isinstance(digest_content, SpecialistDigest):
        digest = digest_content
    elif isinstance(digest_content, dict):
        try:
            digest = SpecialistDigest.model_validate(digest_content)
        except Exception:
            digest = None
    if digest is not None:
        payload["agent_framework"] = "agno"
        payload["agent_role"] = agent.name
        payload["agent_claim"] = digest.claim
        payload["agent_evidence"] = digest.evidence_points
        payload["agent_caveats"] = digest.caveats
    return payload


def plan_dynamic_eda_steps(profile: dict, intent: str, max_steps: int = 4) -> list[str]:
    planned_steps: list[str] = []
    completed_steps = ["profile"]
    prior_summaries = [f"Profiled {profile['row_count']:,} rows across {profile['season_count']} seasons."]
    for _ in range(max_steps):
        next_step = suggest_eda_step(profile, intent, completed_steps, prior_summaries)
        if not next_step or next_step in completed_steps or next_step in planned_steps:
            break
        planned_steps.append(next_step)
        completed_steps.append(next_step)
        prior_summaries.append(f"Planned specialist step: {next_step}.")
    return planned_steps


def run_dynamic_eda(
    connection: duckdb.DuckDBPyConnection,
    duckdb_path: str,
    scope: QueryScope,
    intent: str,
) -> dict[str, dict]:
    profile = profile_scope_data(connection, scope)
    results: dict[str, dict] = {"profile": profile_tool_payload(profile, scope)}
    planned_steps = plan_dynamic_eda_steps(profile, intent, max_steps=4)
    if not planned_steps:
        return results

    step_runners = {
        "trend": lambda: aggregate_specialist_task(duckdb_path, scope),
        "segment": lambda: segment_specialist_payload(duckdb_path, scope, profile),
        "correlation": lambda: correlation_specialist_task(duckdb_path, scope),
        "quality": lambda: quality_specialist_task(duckdb_path, scope),
        "distribution": lambda: distribution_specialist_payload(scope, profile),
    }

    def execute_step(step_name: str) -> dict:
        runner = step_runners[step_name]
        if framework_agents_enabled():
            return run_framework_specialist_agent(step_name, runner, scope, intent)
        payload = runner()
        payload["evidence_objects"] = payload.get("evidence_objects") or build_payload_evidence_objects(step_name, payload)
        return payload

    with ThreadPoolExecutor(max_workers=min(len(planned_steps), 4)) as executor:
        future_map = {
            executor.submit(execute_step, step_name): step_name
            for step_name in planned_steps
            if step_name in step_runners
        }
        for future in as_completed(future_map):
            step_name = future_map[future]
            try:
                results[step_name] = future.result()
            except Exception as exc:
                results[step_name] = eda_step_error_payload(step_name, exc)
    return results


def build_dynamic_hypothesis(
    connection: duckdb.DuckDBPyConnection,
    scope: QueryScope,
    intent: str,
    profile: dict,
    results: dict[str, dict],
    message: str = "",
) -> dict | None:
    candidates: list[dict] = []

    def add_candidate(
        title: str,
        statement: str,
        what_the_data_shows: str,
        what_it_suggests: str,
        confidence: str,
        caveats: list[str],
        evidence_objects: list[dict],
    ) -> None:
        candidates.append(
            {
                "title": title,
                "statement": statement,
                "what_the_data_shows": what_the_data_shows,
                "what_it_suggests": what_it_suggests,
                "confidence": confidence,
                "caveats": caveats,
                "evidence_objects": evidence_objects,
                "evidence": [evidence_object_to_text(item) for item in evidence_objects[:4]],
            }
        )

    if intent == "team_performance" and scope.team:
        team_frame = fetch_team_season_frame(connection, scope.team)
        if not team_frame.empty:
            window = extract_recent_year_window(message) if message else 5
            recent = team_frame.tail(min(window, len(team_frame))).reset_index(drop=True)
            latest = recent.iloc[-1]
            baseline = recent.iloc[:-1]
            baseline_win_rate = float(baseline["win_rate"].mean()) if not baseline.empty else float(latest["win_rate"])
            baseline_goal_diff = float((baseline["goals_for"] - baseline["goals_against"]).mean()) if not baseline.empty else float(latest["goals_for"] - latest["goals_against"])
            latest_goal_diff = float(latest["goals_for"] - latest["goals_against"])
            team_evidence = [
                {
                    "source_step": "team_performance",
                    "kind": "season_comparison",
                    "label": f"{scope.team} latest season",
                    "value": str(latest["season"]),
                    "detail": f"win_rate={latest['win_rate']:.1f}%",
                },
                {
                    "source_step": "team_performance",
                    "kind": "baseline",
                    "label": "Comparison window average win rate",
                    "value": f"{baseline_win_rate:.1f}%",
                    "detail": f"{max(0, len(recent) - 1)} prior seasons",
                },
                {
                    "source_step": "team_performance",
                    "kind": "goal_balance",
                    "label": "Latest goals for/against per match",
                    "value": f"{latest['goals_per_match']:.2f} / {latest['goals_allowed_per_match']:.2f}",
                    "detail": "attack / defense",
                },
            ]
            add_candidate(
                title=f"Current {scope.team} season is above or below its recent baseline",
                statement=f"{scope.team} posted a {latest['win_rate']:.1f}% win rate in {latest['season']} versus a {baseline_win_rate:.1f}% average across the comparison window.",
                what_the_data_shows=f"The latest season can be measured directly against the previous {max(0, len(recent) - 1)} tracked seasons in the requested window.",
                what_it_suggests="This is the primary read on whether the club is improving, stable, or slipping.",
                confidence="high" if len(recent) >= 3 else "medium",
                caveats=[
                    "The comparison window is short when the user asks for only a few seasons.",
                    "League strength and manager changes are not modeled here.",
                ],
                evidence_objects=team_evidence,
            )
            add_candidate(
                title="Goal difference is separating strong seasons from merely decent ones",
                statement=f"Latest goal difference was {latest_goal_diff:.0f} versus a baseline of {baseline_goal_diff:.1f} across the same window.",
                what_the_data_shows="The scoring margin changes whether the season profile is driven by attack, defense, or both.",
                what_it_suggests="If goal difference rises with win rate, the improvement is more structural than just late-game variance.",
                confidence="medium",
                caveats=[
                    "Goal difference is descriptive, not causal.",
                    "Season totals are affected by matches played to date.",
                ],
                evidence_objects=team_evidence,
            )

    trend = fetch_season_trend_frame(connection, scope)
    if not trend.empty:
        early, recent = split_windows(trend)
        goals_delta = float(recent["avg_total_goals"].mean() - early["avg_total_goals"].mean())
        home_delta = float(recent["home_win_rate"].mean() - early["home_win_rate"].mean())
        trend_evidence = list((results.get("trend") or {}).get("evidence_objects") or [])
        if not trend_evidence:
            trend_evidence = [
                {
                    "source_step": "trend",
                    "kind": "window_shift",
                    "label": "Goals per match shift",
                    "value": f"{goals_delta:+.2f}",
                    "detail": "recent minus early window",
                },
                {
                    "source_step": "trend",
                    "kind": "window_shift",
                    "label": "Home-win rate shift",
                    "value": f"{home_delta:+.1f}",
                    "detail": "points",
                },
            ]
        add_candidate(
            title="Recent seasons differ structurally from the early window",
            statement=f"Goals per match moved {goals_delta:+.2f} and home-win rate moved {home_delta:+.1f} points in {scope.label}.",
            what_the_data_shows=f"Early vs recent windows diverge on scoring and venue outcomes across {len(trend)} tracked seasons.",
            what_it_suggests="The current environment is not just noise-free continuity; recent seasons should be analyzed as a different regime.",
            confidence="high" if len(trend) >= 6 else "medium",
            caveats=[
                "This is trend evidence, not causal proof.",
                "A few extreme seasons could still drive the average shift.",
            ],
            evidence_objects=trend_evidence,
        )

    correlation_result = results.get("correlation") or {}
    if correlation_result.get("table"):
        corr_rows = correlation_result["table"]["rows"]
        if corr_rows:
            top_pair = corr_rows[0]
            corr_evidence = list(correlation_result.get("evidence_objects") or [])
            if not corr_evidence:
                corr_evidence = [
                    {
                        "source_step": "correlation",
                        "kind": "correlation_pair",
                        "label": f"{top_pair[0]} vs {top_pair[1]}",
                        "value": str(top_pair[2]),
                        "detail": "correlation coefficient",
                    }
                ]
            add_candidate(
                title="Chance-quality metrics lead the strongest relationships",
                statement=f"{top_pair[0]} and {top_pair[1]} are the strongest linked variables at corr {top_pair[2]}.",
                what_the_data_shows="The correlation scan found a repeatable association inside the scoped match rows.",
                what_it_suggests="Variables tied to shot creation or finishing likely explain more variation than broad chaos metrics.",
                confidence="medium",
                caveats=[
                    "Correlation does not establish causation.",
                    "Coverage gaps can distort coefficient strength.",
                ],
                evidence_objects=corr_evidence,
            )

    if any(item["null_rate"] >= 10 for item in profile["top_missing"]):
        sparse = profile["top_missing"][0]
        quality_evidence = list((results.get("quality") or {}).get("evidence_objects") or [])
        quality_evidence.insert(
            0,
            {
                "source_step": "profile",
                "kind": "missing_column",
                "label": sparse["name"],
                "value": f"{sparse['null_rate']:.1f}%",
                "detail": "null rate in selected slice",
            },
        )
        add_candidate(
            title="Coverage quality constrains deeper causal claims",
            statement=f"{sparse['name']} is {sparse['null_rate']:.1f}% null in the selected slice.",
            what_the_data_shows="Some potentially explanatory columns remain sparse enough to weaken deeper attribution.",
            what_it_suggests="The safest conclusions are descriptive and comparative rather than causal.",
            confidence="medium",
            caveats=[
                "Missingness may cluster in older seasons rather than the whole slice.",
                "A narrower recent-season cut could support stronger analysis.",
            ],
            evidence_objects=quality_evidence[:4],
        )

    if not candidates:
        return None

    confidence_order = {"high": 3, "medium": 2, "low": 1}
    candidates.sort(key=lambda item: confidence_order.get(item["confidence"], 0), reverse=True)
    primary = candidates[0]
    return {
        "title": primary["title"],
        "statement": primary["statement"],
        "evidence": primary["evidence"][:4],
        "evidence_objects": primary["evidence_objects"][:4],
        "confidence": primary["confidence"],
        "correlation_note": "The EDA identifies patterns and associations. It does not prove causal drivers without additional experimental or external data.",
        "candidates": candidates[:3],
        "next_checks": [
            "Check whether the same pattern holds in only the latest 3 seasons.",
            "Test whether the conclusion survives after excluding sparse columns or early seasons.",
        ],
    }


def enrich_warehouse_payload(
    connection: duckdb.DuckDBPyConnection,
    duckdb_path: str,
    message: str,
    scope: QueryScope,
    intent: str,
    payload: dict,
    domain: DomainCheck,
) -> dict:
    rows, seasons = fetch_scope_metrics(connection, scope)
    latest_season = fetch_latest_season(connection, scope, prefer_hyphenated=scope.is_global)
    curated_charts = build_warehouse_charts(connection, scope, intent, message)
    hypothesis = build_warehouse_hypothesis(connection, scope, intent)
    sources = external_validation_sources(scope)[:3]
    if not sources:
        sources = warehouse_sources(scope, rows, seasons, latest_season)[:2]

    payload["tool_calls"] = []
    payload["highlights"] = payload.get("highlights", [])[:4]
    payload["charts"] = curated_charts
    payload["hypothesis"] = hypothesis
    payload["sources"] = sources
    payload["executive_summary"] = build_warehouse_executive_summary(payload)
    payload["data_mode"] = "warehouse"
    payload["out_of_context"] = False
    payload["is_simple_response"] = False
    payload["is_conversational"] = False
    return payload


def requires_web_fallback(
    connection: duckdb.DuckDBPyConnection,
    message: str,
    scope: QueryScope,
    domain: DomainCheck,
) -> bool:
    if requires_external_football_info(message):
        return True
    if domain.external_label and scope.is_global:
        return True
    if scope.is_global:
        return False
    rows, _ = fetch_scope_metrics(connection, scope)
    return rows == 0


def requires_analytics_pipeline(message: str, scope: QueryScope, intent: str) -> bool:
    if intent in {"team_performance", "home_advantage", "correlation", "data_quality", "league_compare", "scoring", "overview"}:
        return True
    normalized = message.casefold()
    return bool(scope.team or scope.league or scope.country or any(
        term in normalized
        for term in ("analyze", "analysis", "trend", "compare", "profile", "eda", "standings", "table", "latest", "show")
    ))


def heuristic_intent(message: str, team_present: bool = False) -> str:
    normalized = message.casefold()
    if classify_count_subject(message):
        return "count_lookup"
    if team_present and any(term in normalized for term in ("season", "seasons", "year", "years")) and any(
        term in normalized for term in ("perform", "performing", "compared", "past", "last")
    ):
        return "team_performance"
    if team_present and any(term in normalized for term in ("last match", "last matches", "last game", "last games", "recent form", "won all", "won every", "last ten games", "last five games")):
        return "team_recent_claim"
    if team_present and any(term in normalized for term in ("perform", "performing", "compared", "past", "last", "years", "year")):
        return "team_performance"
    if any(term in normalized for term in ("home advantage", "home win", "away win", "away goals", "draw rate")):
        return "home_advantage"
    if any(term in normalized for term in ("correlation", "correlate", "relationship", "related")):
        return "correlation"
    if any(term in normalized for term in ("missing", "quality", "coverage", "null")):
        return "data_quality"
    if any(term in normalized for term in ("compare", "league", "cards", "fouls", "corners", "shots", "standings", "table")):
        return "league_compare"
    if any(term in normalized for term in ("goals", "scoring", "score", "attack", "trend")):
        return "scoring"
    return "overview"


def detect_intent(message: str, team_present: bool = False) -> str:
    if completion is None:
        return heuristic_intent(message, team_present=team_present)

    completion_kwargs = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": INTENT_CLASSIFIER_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Question: {message}\n"
                    f"Team detected: {'yes' if team_present else 'no'}"
                ),
            },
        ],
        "temperature": 0,
        "timeout": MODEL_TIMEOUT_SECONDS,
    }
    if LITELLM_API_BASE:
        completion_kwargs["api_base"] = LITELLM_API_BASE
    if LITELLM_API_KEY:
        completion_kwargs["api_key"] = LITELLM_API_KEY

    try:
        response = completion(**completion_kwargs)
        content = (response.choices[0].message.content or "").strip()
        parsed = re.search(r'\{\s*"intent"\s*:\s*"([^"]+)"\s*\}', content)
        if parsed:
            intent = parsed.group(1).strip()
            if intent in INTENT_OPTIONS:
                return intent
    except Exception:
        pass

    return heuristic_intent(message, team_present=team_present)


def chat_response(message: str, duckdb_path: str = DEFAULT_DUCKDB_PATH) -> dict:
    simple_term = resolve_simple_football_term(message)
    if simple_term:
        payload = simple_football_knowledge_payload(message, simple_term)
        payload["intent"] = "external_fact"
        payload["scope"] = "football knowledge"
        return payload

    connection = open_connection(duckdb_path)
    try:
        domain = validate_domain(connection, message)
        if not domain.is_football:
            payload = out_of_context_payload(message, domain)
            payload["intent"] = "out_of_context"
            payload["scope"] = "outside football analytics"
            return payload

        known_external_payload = build_known_external_payload(message, domain)
        if known_external_payload is not None:
            known_external_payload["intent"] = "external_fact"
            known_external_payload["scope"] = domain.external_label or "external football fact"
            return known_external_payload

        scope = resolve_scope(connection, message)
        answerability = assess_answerability(message, scope, domain)

        if answerability.mode == "external_fact":
            if should_use_direct_fact_answer(message, scope):
                return build_direct_fact_payload(message, scope, domain)
            payload = build_web_fallback_payload(message, domain)
            payload["intent"] = "external_fact"
            payload["scope"] = domain.external_label or "external football fact"
            return payload

        if answerability.mode == "clarify":
            payload = direct_football_clarification_payload(message, scope)
            payload["intent"] = "external_fact"
            payload["scope"] = scope.label
            return payload

        intent = detect_intent(message, team_present=scope.team is not None)
        if intent == "count_lookup":
            payload = count_lookup_payload(connection, message, scope)
        elif intent == "team_recent_claim":
            payload = recent_team_claim_response(connection, message, scope)
        elif not requires_analytics_pipeline(message, scope, intent):
            payload = direct_football_clarification_payload(message, scope)
            payload["intent"] = "external_fact"
            payload["scope"] = scope.label
            return payload
        else:
            if intent == "team_performance":
                payload = team_performance_response(connection, scope, message)
            elif intent == "home_advantage":
                payload = home_advantage_response(connection, scope)
            elif intent == "league_compare":
                payload = league_comparison_response(connection, scope)
            elif intent == "correlation":
                payload = correlation_response(connection, scope)
            elif intent == "data_quality":
                payload = data_quality_response(connection, scope)
            elif intent == "scoring":
                payload = scoring_trend_response(connection, scope)
            else:
                payload = general_overview_response(connection, scope)

        fallback_mode = requires_web_fallback(connection, message, scope, domain)
        if fallback_mode:
            payload = build_web_fallback_payload(message, domain)
            payload["intent"] = "external_fact"
            payload["scope"] = domain.external_label or scope.label
            return payload

        payload = enrich_warehouse_payload(connection, duckdb_path, message, scope, intent, payload, domain)
        payload["intent"] = intent
        payload["scope"] = scope.label
        return payload
    finally:
        connection.close()
