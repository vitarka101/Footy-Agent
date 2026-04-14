from __future__ import annotations

import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
from google.cloud import storage

from football_web_fallback import build_web_fallback_bundle

DEFAULT_DUCKDB_PATH = "football_data.duckdb"
DEFAULT_DUCKDB_GCS_URI = ""
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
}
EXTERNAL_FOOTBALL_HINTS = {
    "india": {"label": "India football", "query": "India football league national team results analysis"},
    "indian": {"label": "India football", "query": "India football league national team results analysis"},
    "indian super league": {"label": "Indian Super League", "query": "Indian Super League standings clubs football analysis"},
    "isl": {"label": "Indian Super League", "query": "Indian Super League standings clubs football analysis"},
    "a league": {"label": "A-League", "query": "A-League Australia football standings analysis"},
    "saudi pro league": {"label": "Saudi Pro League", "query": "Saudi Pro League football standings clubs analysis"},
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

    @property
    def is_global(self) -> bool:
        return self.country is None and self.league is None and self.season is None

    @property
    def label(self) -> str:
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


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def contains_phrase(text: str, phrase: str) -> bool:
    return f" {phrase} " in f" {text} "


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
    country = find_country(normalized_message, countries_by_norm)
    league_candidate = find_league(normalized_message, country, leagues_by_norm)

    if league_candidate is not None:
        country = country or league_candidate.country or None
        league = league_candidate.league
    else:
        league = None

    season_match = SEASON_PATTERN.search(message)
    season = season_match.group(0) if season_match else None
    return QueryScope(country=country, league=league, season=season)


def find_external_focus(normalized_message: str) -> tuple[str | None, str | None]:
    for alias, payload in sorted(EXTERNAL_FOOTBALL_HINTS.items(), key=lambda item: len(item[0]), reverse=True):
        if contains_phrase(normalized_message, normalize_text(alias)):
            return payload["label"], payload["query"]
    return None, None


def validate_domain(connection: duckdb.DuckDBPyConnection, message: str) -> DomainCheck:
    normalized_message = normalize_text(message)
    countries_by_norm, leagues_by_norm = build_reference_catalog(connection)
    football_vocab = [
        term
        for term in FOOTBALL_DOMAIN_TERMS
        if contains_phrase(normalized_message, normalize_text(term))
    ]
    matched_terms = list(football_vocab)
    country = find_country(normalized_message, countries_by_norm)
    league_candidate = find_league(normalized_message, country, leagues_by_norm)
    external_label, external_query = find_external_focus(normalized_message)

    if country and (football_vocab or league_candidate):
        matched_terms.append(country)
    if league_candidate:
        matched_terms.append(league_candidate.league)
    if external_label and football_vocab:
        matched_terms.append(external_label)

    deduped_terms = tuple(dict.fromkeys(matched_terms))
    if deduped_terms:
        return DomainCheck(
            is_football=True,
            reason="Matched football entities or football-analysis vocabulary in the request.",
            matched_terms=deduped_terms,
            external_label=external_label,
            external_query=external_query,
        )

    return DomainCheck(
        is_football=False,
        reason="The request did not match football entities, leagues, countries, or football-analysis vocabulary.",
        matched_terms=(),
    )


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
        "This assistant is scoped to football and soccer analytics. Ask about leagues, standings, goals, "
        "shots, cards, teams, or seasons and I will fetch the data, run EDA, and then answer."
    )
    return {
        "answer": answer,
        "tool_calls": [
            tool_call("domain_gate", "Domain Validation", domain.reason),
            tool_call("out_of_context", "Out of Context Handling", "Stopped before retrieval because the request is outside football analytics."),
        ],
        "highlights": [
            metric("Status", "Out of context", "No football-domain signal detected"),
        ],
        "table": None,
        "suggested_prompts": PROMPT_CHIPS,
        "charts": [],
        "hypothesis": None,
        "sources": [],
        "data_mode": "none",
        "out_of_context": True,
    }


def build_web_fallback_charts(bundle: dict) -> list[dict]:
    sources = bundle.get("sources", [])[:5]
    keywords = bundle.get("keywords", [])[:8]
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

    if keywords:
        charts.append(
            bar_chart(
                "Keyword frequency across retrieved snippets",
                "Summarizes the football concepts that appeared most often in the retrieved evidence pack.",
                [item["keyword"] for item in keywords],
                [{"name": "Mentions", "data": [int(item["count"]) for item in keywords]}],
                y_label="Mentions",
            )
        )
    return charts


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

    answer = (
        f"The warehouse does not cover {domain.external_label or 'this football slice'}, so I switched to web retrieval. "
        f"I searched external football sources, crawled the highest-value pages, ranked the text snippets by relevance, "
        f"and based the answer on that evidence. The strongest source was {top_source['title']}."
    )
    evidence = [
        f"{source['title']}: relevance {source['score']:.3f}" for source in sources[:3]
    ]
    if top_excerpt:
        evidence.append(f"Top excerpt: {top_excerpt[:180]}...")

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
        "table": table_payload(
            pd.DataFrame(
                [
                    {"source": source["title"], "type": source["source_type"], "score": source["score"]}
                    for source in sources[:5]
                ]
            ),
            float_digits=3,
        ),
        "suggested_prompts": [
            "Analyze La Liga home advantage.",
            "Compare Serie A and Bundesliga on goals and cards.",
        ],
        "charts": build_web_fallback_charts(bundle),
        "hypothesis": hypothesis_payload(
            "External football evidence suggests this topic needs a non-warehouse answer",
            (
                f"The assistant could not answer from DuckDB alone, so the current conclusion depends on live web evidence about "
                f"{domain.external_label or 'the requested football topic'}. The strongest support came from {top_source['title']} "
                f"plus {max(0, len(sources) - 1)} additional football sources."
            ),
            evidence,
        ),
        "sources": [
            source_item(source["title"], source["snippet"], source.get("url"), source["source_type"])
            for source in sources
        ],
        "data_mode": "web_fallback",
        "out_of_context": False,
    }


def build_warehouse_charts(connection: duckdb.DuckDBPyConnection, scope: QueryScope, intent: str) -> list[dict]:
    if intent == "home_advantage":
        trend = fetch_season_trend_frame(connection, scope)
        if trend.empty:
            return []
        return [
            line_chart(
                "Home vs away win rate by season",
                "Tracks whether venue advantage is holding, compressing, or reversing over time.",
                trend["season"].astype(str).tolist(),
                [
                    {"name": "Home win rate", "data": serialize_numeric(trend["home_win_rate"], digits=2)},
                    {"name": "Away win rate", "data": serialize_numeric(trend["away_win_rate"], digits=2)},
                ],
                y_label="Win rate %",
            ),
            line_chart(
                "Home vs away goals by season",
                "Shows whether the scoring edge for home teams remains larger than the away scoring rate.",
                trend["season"].astype(str).tolist(),
                [
                    {"name": "Home goals", "data": serialize_numeric(trend["avg_home_goals"], digits=2)},
                    {"name": "Away goals", "data": serialize_numeric(trend["avg_away_goals"], digits=2)},
                ],
                y_label="Goals / match",
            ),
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
                strongest_frame["metric_a"] + " ↔ " + strongest_frame["metric_b"],
                [{"name": "Correlation", "data": serialize_numeric(strongest_frame["correlation"], digits=3)}],
                y_label="corr",
            ),
        ]

    if intent == "data_quality":
        quality = fetch_data_quality_frame(connection, scope)
        if quality.empty:
            return []
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
        ]

    trend = fetch_season_trend_frame(connection, scope)
    if trend.empty:
        return []
    chart_set = [
        line_chart(
            "Goals per match by season",
            "Establishes the scoring trend before forming a conclusion about the selected slice.",
            trend["season"].astype(str).tolist(),
            [{"name": "Avg goals", "data": serialize_numeric(trend["avg_total_goals"], digits=2)}],
            y_label="Goals / match",
        ),
        line_chart(
            "Shots per match by season",
            "Shows whether changes in scoring are paired with changes in shot volume.",
            trend["season"].astype(str).tolist(),
            [{"name": "Avg shots", "data": serialize_numeric(trend["avg_shots"], digits=1)}],
            y_label="Shots / match",
        ),
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
        if not snapshot.empty:
            labels = snapshot.apply(
                lambda row: row["league"] if scope.country else f"{row['country']} · {row['league']}",
                axis=1,
            ).tolist()
            chart_set.insert(
                0,
                bar_chart(
                    f"Latest scoring snapshot ({latest_season})",
                    "Places the requested slice against the latest visible league comparison set.",
                    labels,
                    [{"name": "Avg goals", "data": serialize_numeric(snapshot["avg_goals"], digits=2)}],
                    y_label="Goals / match",
                ),
            )
    return chart_set


def build_warehouse_hypothesis(connection: duckdb.DuckDBPyConnection, scope: QueryScope, intent: str) -> dict | None:
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


def run_parallel_warehouse_specialists(duckdb_path: str, scope: QueryScope) -> dict[str, dict]:
    workers = {
        "aggregate": aggregate_specialist_task,
        "segment": segment_specialist_task,
        "correlation": correlation_specialist_task,
        "quality": quality_specialist_task,
    }
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=len(workers)) as executor:
        future_map = {
            executor.submit(worker, duckdb_path, scope): key for key, worker in workers.items()
        }
        for future in as_completed(future_map):
            key = future_map[future]
            try:
                results[key] = future.result()
            except Exception as exc:
                results[key] = {
                    "key": key,
                    "label": f"{key.title()} Specialist",
                    "summary": f"{key.title()} specialist could not complete its analysis: {exc}",
                    "chart": None,
                    "table": None,
                    "highlights": [],
                }
    return results


def enrich_warehouse_payload(
    connection: duckdb.DuckDBPyConnection,
    duckdb_path: str,
    scope: QueryScope,
    intent: str,
    payload: dict,
    domain: DomainCheck,
) -> dict:
    rows, seasons = fetch_scope_metrics(connection, scope)
    latest_season = fetch_latest_season(connection, scope, prefer_hyphenated=scope.is_global)
    specialists = run_parallel_warehouse_specialists(duckdb_path, scope)
    specialist_tool_calls = [
        tool_call(f"{key}_agent", specialists[key]["label"], specialists[key]["summary"])
        for key in SPECIALIST_ORDER
        if key in specialists
    ]
    specialist_charts = [
        specialists[key]["chart"]
        for key in SPECIALIST_ORDER
        if key in specialists and specialists[key].get("chart")
    ]
    merged_highlights = list(payload.get("highlights", []))
    seen_highlight_labels = {item["label"] for item in merged_highlights}
    for key in SPECIALIST_ORDER:
        for item in specialists.get(key, {}).get("highlights", []):
            if item["label"] in seen_highlight_labels:
                continue
            merged_highlights.append(item)
            seen_highlight_labels.add(item["label"])
            if len(merged_highlights) >= 6:
                break
        if len(merged_highlights) >= 6:
            break

    payload["tool_calls"] = [
        tool_call("domain_gate", "Domain Validation", f"Confirmed the request is football-related via: {', '.join(domain.matched_terms[:4])}."),
        tool_call("warehouse_fetch", "Warehouse Retrieval", f"Fetched {rows:,} rows from DuckDB for {scope.label}."),
        tool_call("parallel_eda", "Parallel EDA Orchestrator", f"Ran aggregate, segment, correlation, and quality specialists in parallel for {scope.label}."),
        *specialist_tool_calls,
        tool_call("hypothesis", "Hypothesis Builder", "Converted the observed EDA patterns into a grounded analytical claim."),
    ]
    payload["highlights"] = merged_highlights
    payload["charts"] = specialist_charts
    payload["hypothesis"] = build_warehouse_hypothesis(connection, scope, intent)
    payload["sources"] = warehouse_sources(scope, rows, seasons, latest_season)
    payload["data_mode"] = "warehouse"
    payload["out_of_context"] = False
    return payload


def requires_web_fallback(connection: duckdb.DuckDBPyConnection, scope: QueryScope, domain: DomainCheck) -> bool:
    if domain.external_label and scope.is_global:
        return True
    if scope.is_global:
        return False
    rows, _ = fetch_scope_metrics(connection, scope)
    return rows == 0


def detect_intent(message: str) -> str:
    normalized = message.casefold()
    if any(term in normalized for term in ("home advantage", "home win", "away win", "away goals", "draw rate")):
        return "home_advantage"
    if any(term in normalized for term in ("compare", "league", "cards", "fouls", "corners", "shots", "standings", "table")):
        return "league_compare"
    if any(term in normalized for term in ("correlation", "correlate", "relationship", "related")):
        return "correlation"
    if any(term in normalized for term in ("missing", "quality", "coverage", "null")):
        return "data_quality"
    if any(term in normalized for term in ("goals", "scoring", "score", "attack", "trend")):
        return "scoring"
    return "overview"


def chat_response(message: str, duckdb_path: str = DEFAULT_DUCKDB_PATH) -> dict:
    connection = open_connection(duckdb_path)
    try:
        domain = validate_domain(connection, message)
        if not domain.is_football:
            payload = out_of_context_payload(message, domain)
            payload["intent"] = "out_of_context"
            payload["scope"] = "outside football analytics"
            return payload

        scope = resolve_scope(connection, message)
        intent = detect_intent(message)
        fallback_mode = requires_web_fallback(connection, scope, domain)
        if fallback_mode:
            payload = build_web_fallback_payload(message, domain)
        else:
            if intent == "home_advantage":
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
            payload = enrich_warehouse_payload(connection, duckdb_path, scope, intent, payload, domain)
    finally:
        connection.close()

    payload["intent"] = intent
    payload["scope"] = domain.external_label or scope.label if fallback_mode else scope.label
    return payload
