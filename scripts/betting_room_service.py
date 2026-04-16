from __future__ import annotations

import csv
import json
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import duckdb
import requests

from scripts.football_ui_service import DEFAULT_DUCKDB_PATH

MAX_GOALS = 8
MODEL_NAMES = [
    "Maher",
    "Dixon-Coles",
    "Dixon-Coles TD",
    "Bivariate Poisson",
    "Negative Binomial",
]
LEAGUE_IDS = {
    "E0": {"country": "England", "league": "Premier League", "warehouse_league": "Premier League"},
    "SP1": {"country": "Spain", "league": "La Liga", "warehouse_league": "La Liga Primera Division"},
    "D1": {"country": "Germany", "league": "Bundesliga", "warehouse_league": "Bundesliga 1"},
    "I1": {"country": "Italy", "league": "Serie A", "warehouse_league": "Serie A"},
    "F1": {"country": "France", "league": "Ligue 1", "warehouse_league": "Le Championnat"},
    "N1": {"country": "Netherlands", "league": "Eredivisie", "warehouse_league": "Eredivisie"},
    "P1": {"country": "Portugal", "league": "Primeira Liga", "warehouse_league": "Liga I"},
    "B1": {"country": "Belgium", "league": "Jupiler Pro League", "warehouse_league": "Jupiler League"},
    "SC0": {"country": "Scotland", "league": "Premiership", "warehouse_league": "Premier League"},
    "T1": {"country": "Turkey", "league": "Super Lig", "warehouse_league": "Futbol Ligi 1"},
}
REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "betting_room"
DATA_DIR = ARTIFACTS_DIR / "data"
REPORTS_DIR = ARTIFACTS_DIR / "reports"
CACHE_TTL_SECONDS = 12 * 60 * 60
BOOKMAKER_COLUMNS = (
    ("B365", "Bet365"),
    ("Avg", "Average Market"),
    ("PS", "Pinnacle"),
)


@dataclass(frozen=True)
class BettingMatch:
    season: str
    date: str
    home: str
    away: str
    hg: int
    ag: int
    result: str
    odds_home: float | None = None
    odds_draw: float | None = None
    odds_away: float | None = None
    odds_source: str | None = None


def tool_call(name: str, label: str, summary: str, *, function_name: str, duration_ms: int | None = None) -> dict:
    payload = {
        "name": name,
        "label": label,
        "summary": summary,
        "function_name": function_name,
    }
    if duration_ms is not None:
        payload["duration_ms"] = duration_ms
    return payload


def metric(label: str, value: str, detail: str) -> dict:
    return {"label": label, "value": value, "detail": detail}


def format_number(value: float | int | None, digits: int = 0) -> str:
    if value is None:
        return "n/a"
    if digits == 0:
        return f"{int(round(float(value))):,}"
    return f"{float(value):,.{digits}f}"


def season_options(years_back: int = 8) -> list[str]:
    current_year = datetime.now(UTC).year
    names: list[str] = []
    for start_year in range(current_year - years_back, current_year):
        names.append(f"{start_year}/{start_year + 1}")
    return list(reversed(names))


def normalize_season_name(season_name: str) -> str:
    return (season_name or "").strip().replace("-", "/")


def alternate_season_name(season_name: str) -> str:
    normalized = normalize_season_name(season_name)
    return normalized.replace("/", "-")


def season_name_to_id(season_name: str) -> str:
    start, end = normalize_season_name(season_name).split("/")
    return f"{start[-2:]}{end[-2:]}"


def normalize_result(hg: int, ag: int, fallback: str = "") -> str:
    if fallback in {"H", "D", "A"}:
        return fallback
    if hg > ag:
        return "H"
    if hg < ag:
        return "A"
    return "D"


def parse_float(value: str | None) -> float | None:
    if value in (None, "", "NA", "null"):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def extract_odds(row: dict[str, str]) -> tuple[float | None, float | None, float | None, str | None]:
    for prefix, source in BOOKMAKER_COLUMNS:
        home = parse_float(row.get(f"{prefix}H"))
        draw = parse_float(row.get(f"{prefix}D"))
        away = parse_float(row.get(f"{prefix}A"))
        if home and draw and away:
            return home, draw, away, source
    return None, None, None, None


def parse_csv_matches(csv_text: str, league_id: str, season_name: str) -> list[BettingMatch]:
    rows: list[BettingMatch] = []
    reader = csv.DictReader(csv_text.splitlines())
    for row in reader:
        home_team = (row.get("HomeTeam") or row.get("HT") or "").strip()
        away_team = (row.get("AwayTeam") or row.get("AT") or "").strip()
        if not home_team or not away_team:
            continue
        try:
            hg = int(row.get("FTHG") or row.get("HG") or "")
            ag = int(row.get("FTAG") or row.get("AG") or "")
        except ValueError:
            continue
        odds_home, odds_draw, odds_away, odds_source = extract_odds(row)
        rows.append(
            BettingMatch(
                season=season_name,
                date=(row.get("Date") or "").strip(),
                home=home_team,
                away=away_team,
                hg=hg,
                ag=ag,
                result=normalize_result(hg, ag, (row.get("FTR") or row.get("Res") or "").strip()),
                odds_home=odds_home,
                odds_draw=odds_draw,
                odds_away=odds_away,
                odds_source=odds_source,
            )
        )
    return rows


def cache_path_for(league_id: str, season_name: str) -> Path:
    safe_season = normalize_season_name(season_name).replace("/", "-")
    return DATA_DIR / f"{league_id}_{safe_season}.json"


def read_cached_matches(path: Path) -> list[BettingMatch] | None:
    if not path.exists():
        return None
    age_seconds = time.time() - path.stat().st_mtime
    if age_seconds > CACHE_TTL_SECONDS:
        return None
    payload = json.loads(path.read_text())
    return [BettingMatch(**item) for item in payload]


def write_cached_matches(path: Path, matches: list[BettingMatch]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([match.__dict__ for match in matches], indent=2))


def fetch_runtime_csv(url: str) -> str:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return response.text


def fetch_external_season_matches(league_id: str, season_name: str, force_refresh: bool = False) -> tuple[list[BettingMatch], str]:
    cache_path = cache_path_for(league_id, season_name)
    if not force_refresh:
        cached = read_cached_matches(cache_path)
        if cached is not None:
            return cached, "artifact_cache"

    season_id = season_name_to_id(season_name)
    url = f"https://www.football-data.co.uk/mmz4281/{season_id}/{league_id}.csv"
    csv_text = fetch_runtime_csv(url)
    matches = parse_csv_matches(csv_text, league_id, season_name)
    if not matches:
        raise ValueError(f"No matches parsed from {url}")
    write_cached_matches(cache_path, matches)
    return matches, "football-data.co.uk"


def fetch_duckdb_season_matches(league_id: str, season_name: str, duckdb_path: str = DEFAULT_DUCKDB_PATH) -> list[BettingMatch]:
    mapping = LEAGUE_IDS[league_id]
    normalized = normalize_season_name(season_name)
    alternate = alternate_season_name(season_name)
    connection = duckdb.connect(duckdb_path, read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT
                season,
                coalesce(date, '') AS date,
                hometeam,
                awayteam,
                fthg,
                ftag,
                ftr,
                NULL AS odds_home,
                NULL AS odds_draw,
                NULL AS odds_away,
                NULL AS odds_source
            FROM matches
            WHERE country = ?
              AND league = ?
              AND season IN (?, ?)
              AND fthg IS NOT NULL
              AND ftag IS NOT NULL
            ORDER BY try_strptime(date, '%d/%m/%Y') NULLS LAST, hometeam, awayteam
            """,
            [mapping["country"], mapping["warehouse_league"], normalized, alternate],
        ).fetchall()
    finally:
        connection.close()

    return [
        BettingMatch(
            season=row[0],
            date=row[1],
            home=row[2],
            away=row[3],
            hg=int(row[4]),
            ag=int(row[5]),
            result=normalize_result(int(row[4]), int(row[5]), row[6] or ""),
        )
        for row in rows
    ]


def collect_match_data_tool(
    league_id: str,
    season_name: str,
    history_depth: int = 4,
    force_refresh: bool = False,
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
) -> dict:
    if league_id not in LEAGUE_IDS:
        raise ValueError(f"Unsupported league id: {league_id}")
    all_seasons = season_options(max(history_depth + 2, 8))
    if season_name not in all_seasons:
        all_seasons.insert(0, season_name)
    season_index = all_seasons.index(season_name)
    requested = all_seasons[season_index : season_index + history_depth]
    if len(requested) < history_depth:
        requested = all_seasons[max(0, season_index - history_depth + 1) : season_index + 1]

    season_payloads: dict[str, list[BettingMatch]] = {}
    sources_used: list[str] = []
    fallback_used = False
    for requested_season in requested:
        try:
            matches, source_name = fetch_external_season_matches(league_id, requested_season, force_refresh=force_refresh)
        except Exception:
            matches = fetch_duckdb_season_matches(league_id, requested_season, duckdb_path=duckdb_path)
            source_name = "duckdb_fallback"
            fallback_used = True
        if matches:
            season_payloads[requested_season] = matches
            sources_used.append(source_name)

    if season_name not in season_payloads:
        raise ValueError(f"No match data found for {league_id} {season_name}.")

    current_matches = season_payloads[season_name]
    training_matches: list[BettingMatch] = []
    for name, matches in season_payloads.items():
        if name != season_name:
            training_matches.extend(matches)
    combined_matches = training_matches + current_matches
    teams = sorted({match.home for match in current_matches} | {match.away for match in current_matches})

    return {
        "league_id": league_id,
        "league": LEAGUE_IDS[league_id]["league"],
        "country": LEAGUE_IDS[league_id]["country"],
        "season": season_name,
        "teams": teams,
        "current_matches": current_matches,
        "training_matches": training_matches,
        "all_matches": combined_matches,
        "sources_used": sorted(set(sources_used)),
        "fallback_used": fallback_used,
        "history_depth": len(season_payloads),
    }


_LOG_FACTORIAL_CACHE = [0.0, 0.0]


def log_factorial(value: int) -> float:
    if value < 0:
        return 0.0
    if value < len(_LOG_FACTORIAL_CACHE):
        return _LOG_FACTORIAL_CACHE[value]
    for index in range(len(_LOG_FACTORIAL_CACHE), value + 1):
        _LOG_FACTORIAL_CACHE.append(_LOG_FACTORIAL_CACHE[index - 1] + math.log(index))
    return _LOG_FACTORIAL_CACHE[value]


def log_gamma(value: float) -> float:
    if value < 0.5:
        return math.log(math.pi / math.sin(math.pi * value)) - log_gamma(1 - value)
    z = value - 1
    coeffs = [
        0.99999999999980993,
        676.5203681218851,
        -1259.1392167224028,
        771.32342877765313,
        -176.61502916214059,
        12.507343278686905,
        -0.13857109526572012,
        9.9843695780195716e-6,
        1.5056327351493116e-7,
    ]
    total = coeffs[0]
    for idx in range(1, 9):
        total += coeffs[idx] / (z + idx)
    t_value = z + 7.5
    return 0.5 * math.log(2 * math.pi) + (z + 0.5) * math.log(t_value) - t_value + math.log(total)


def poisson_pmf(k_value: int, lam: float) -> float:
    if lam <= 0:
        return 1.0 if k_value == 0 else 0.0
    return math.exp(-lam + k_value * math.log(lam) - log_factorial(k_value))


def neg_bin_pmf(k_value: int, r_value: float, p_value: float) -> float:
    if r_value <= 0 or p_value <= 0 or p_value >= 1:
        return 0.0
    return math.exp(
        log_gamma(k_value + r_value)
        - log_factorial(k_value)
        - log_gamma(r_value)
        + r_value * math.log(p_value)
        + k_value * math.log(1 - p_value)
    )


def random_poisson(lam: float, rng: random.Random) -> int:
    if lam <= 0:
        return 0
    limit = math.exp(-min(lam, 500))
    k_value = 0
    prod = 1.0
    while prod > limit:
        k_value += 1
        prod *= rng.random()
    return k_value - 1


def lower_incomplete_gamma(a_value: float, x_value: float) -> float:
    if x_value <= 0:
        return 0.0
    acc = 0.0
    term = 1.0 / a_value
    for n_value in range(1, 200):
        acc += term
        term *= x_value / (a_value + n_value)
        if abs(term) < 1e-12:
            break
    return acc * math.exp(-x_value + a_value * math.log(x_value) - log_gamma(a_value))


def chi_sq_cdf(x_value: float, degrees: int) -> float:
    if x_value <= 0:
        return 0.0
    return lower_incomplete_gamma(degrees / 2, x_value / 2)


def chi_sq_p_value(x_value: float, degrees: int) -> float:
    return 1 - chi_sq_cdf(x_value, degrees)


def poisson_goodness_of_fit_test(goals: list[int]) -> dict | None:
    sample_size = len(goals)
    if sample_size < 10:
        return None
    lam = sum(goals) / sample_size
    max_bin = 6
    observed = [0] * (max_bin + 1)
    for goal in goals:
        observed[min(goal, max_bin)] += 1
    cumulative_prob = 0.0
    expected: list[float] = []
    for k_value in range(max_bin):
        probability = poisson_pmf(k_value, lam)
        expected.append(sample_size * probability)
        cumulative_prob += probability
    expected.append(sample_size * (1 - cumulative_prob))
    bins = [f"{idx}" for idx in range(max_bin)] + [f"{max_bin}+"]
    while len(expected) > 2 and expected[-1] < 5:
        observed[-2] += observed.pop()
        expected[-2] += expected.pop()
        bins.pop()
        bins[-1] += "+"
    chi_stat = 0.0
    for idx, expected_value in enumerate(expected):
        if expected_value > 0:
            chi_stat += (observed[idx] - expected_value) ** 2 / expected_value
    degrees = max(1, len(observed) - 2)
    return {
        "statistic": chi_stat,
        "df": degrees,
        "p_value": chi_sq_p_value(chi_stat, degrees),
        "lambda": lam,
        "observed": observed,
        "expected": [round(value, 1) for value in expected],
        "bins": bins,
    }


def independence_test(matches: list[BettingMatch]) -> dict | None:
    if len(matches) < 20:
        return None
    max_goal = 5
    table = [[0 for _ in range(max_goal + 1)] for _ in range(max_goal + 1)]
    for match in matches:
        table[min(match.hg, max_goal)][min(match.ag, max_goal)] += 1
    total = len(matches)
    row_sums = [sum(row) for row in table]
    col_sums = [sum(table[row][col] for row in range(max_goal + 1)) for col in range(max_goal + 1)]
    chi_stat = 0.0
    for row in range(max_goal + 1):
        for col in range(max_goal + 1):
            expected = (row_sums[row] * col_sums[col]) / total
            if expected > 0:
                chi_stat += (table[row][col] - expected) ** 2 / expected
    degrees = max_goal * max_goal
    return {"statistic": chi_stat, "df": degrees, "p_value": chi_sq_p_value(chi_stat, degrees)}


def dispersion_test(goals: list[int]) -> dict | None:
    sample_size = len(goals)
    if sample_size < 10:
        return None
    mean = sum(goals) / sample_size
    variance = sum((goal - mean) ** 2 for goal in goals) / max(sample_size - 1, 1)
    ratio = variance / mean if mean > 0 else 0
    statistic = (sample_size - 1) * variance / mean if mean > 0 else 0
    degrees = sample_size - 1
    p_over = chi_sq_p_value(statistic, degrees)
    p_under = chi_sq_cdf(statistic, degrees)
    return {
        "statistic": statistic,
        "df": degrees,
        "p_value": min(2 * min(p_over, p_under), 1),
        "p_value_over": p_over,
        "mean": mean,
        "variance": variance,
        "ratio": ratio,
    }


def estimate_params(matches: list[BettingMatch], weights: list[float] | None = None) -> dict:
    teams = sorted({match.home for match in matches} | {match.away for match in matches})
    team_idx = {team: idx for idx, team in enumerate(teams)}
    attack = [1.0] * len(teams)
    defense = [1.0] * len(teams)
    home_adv = 0.25
    match_weights = weights or [1.0] * len(matches)
    for _ in range(30):
        attack_num = [0.0] * len(teams)
        attack_den = [0.0] * len(teams)
        defense_num = [0.0] * len(teams)
        defense_den = [0.0] * len(teams)
        weighted_hg = 0.0
        weighted_ag = 0.0
        weighted_matches = 0.0
        for idx, match in enumerate(matches):
            home_idx = team_idx[match.home]
            away_idx = team_idx[match.away]
            weight = match_weights[idx]
            exp_home_adv = math.exp(home_adv)
            attack_num[home_idx] += match.hg * weight
            attack_den[home_idx] += defense[away_idx] * exp_home_adv * weight
            attack_num[away_idx] += match.ag * weight
            attack_den[away_idx] += defense[home_idx] * weight
            defense_num[home_idx] += match.ag * weight
            defense_den[home_idx] += attack[away_idx] * weight
            defense_num[away_idx] += match.hg * weight
            defense_den[away_idx] += attack[home_idx] * exp_home_adv * weight
            weighted_hg += match.hg * weight
            weighted_ag += match.ag * weight
            weighted_matches += weight
        for team_idx_value in range(len(teams)):
            if attack_den[team_idx_value] > 0:
                attack[team_idx_value] = attack_num[team_idx_value] / attack_den[team_idx_value]
            if defense_den[team_idx_value] > 0:
                defense[team_idx_value] = defense_num[team_idx_value] / defense_den[team_idx_value]
        log_mean = sum(math.log(max(value, 1e-6)) for value in attack) / len(teams)
        scale = math.exp(log_mean)
        attack = [value / scale for value in attack]
        defense = [value * scale for value in defense]
        if weighted_matches > 0 and weighted_ag > 0:
            home_adv = math.log(max(weighted_hg / weighted_ag, 0.5))
    return {
        "teams": teams,
        "team_idx": team_idx,
        "attack": attack,
        "defense": defense,
        "home_adv": home_adv,
    }


def dixon_coles_tau(x_value: int, y_value: int, lambda_home: float, lambda_away: float, rho: float) -> float:
    if x_value == 0 and y_value == 0:
        return 1 - lambda_home * lambda_away * rho
    if x_value == 0 and y_value == 1:
        return 1 + lambda_home * rho
    if x_value == 1 and y_value == 0:
        return 1 + lambda_away * rho
    if x_value == 1 and y_value == 1:
        return 1 - rho
    return 1.0


def estimate_rho(matches: list[BettingMatch], params: dict) -> float:
    adjustment = 0.0
    count = 0
    for match in matches:
        if match.hg <= 1 and match.ag <= 1:
            if match.hg == 0 and match.ag == 0:
                adjustment += 1
            elif match.hg == 1 and match.ag == 1:
                adjustment += 1
            else:
                adjustment -= 0.5
            count += 1
    if count == 0:
        return 0.0
    return max(-0.5, min(0.5, adjustment / (count * 5)))


def bivariate_poisson_pmf(x_value: int, y_value: int, l1: float, l2: float, l3: float) -> float:
    probability = 0.0
    for k_value in range(min(x_value, y_value) + 1):
        probability += (
            poisson_pmf(x_value - k_value, l1)
            * poisson_pmf(y_value - k_value, l2)
            * poisson_pmf(k_value, l3)
        )
    return probability


def estimate_lambda_three(matches: list[BettingMatch]) -> float:
    mean_home = sum(match.hg for match in matches) / len(matches)
    mean_away = sum(match.ag for match in matches) / len(matches)
    covariance = sum((match.hg - mean_home) * (match.ag - mean_away) for match in matches) / max(len(matches) - 1, 1)
    return max(0.01, min(0.5, covariance * 0.3 if covariance > 0 else 0.05))


def estimate_dispersion(goals: list[int], mean_value: float) -> float:
    if len(goals) < 2:
        return 50.0
    variance = sum((goal - mean_value) ** 2 for goal in goals) / max(len(goals) - 1, 1)
    if variance <= mean_value:
        return 50.0
    return max(1.0, mean_value * mean_value / (variance - mean_value))


def build_matrix(
    lambda_home: float,
    lambda_away: float,
    kind: Literal["independent", "dixoncoles", "bivariate", "negbin"],
    rho: float = 0.0,
    l3: float = 0.0,
    r_home: float = 50.0,
    r_away: float = 50.0,
) -> dict:
    matrix: list[list[float]] = []
    p_home = 0.0
    p_draw = 0.0
    p_away = 0.0
    for i_value in range(MAX_GOALS + 1):
        row: list[float] = []
        for j_value in range(MAX_GOALS + 1):
            if kind == "negbin":
                prob_home = r_home / (r_home + lambda_home)
                prob_away = r_away / (r_away + lambda_away)
                probability = neg_bin_pmf(i_value, r_home, prob_home) * neg_bin_pmf(j_value, r_away, prob_away)
            elif kind == "bivariate":
                probability = bivariate_poisson_pmf(i_value, j_value, lambda_home, lambda_away, l3)
            else:
                probability = poisson_pmf(i_value, lambda_home) * poisson_pmf(j_value, lambda_away)
                if kind == "dixoncoles":
                    probability *= dixon_coles_tau(i_value, j_value, lambda_home, lambda_away, rho)
            row.append(probability)
            if i_value > j_value:
                p_home += probability
            elif i_value == j_value:
                p_draw += probability
            else:
                p_away += probability
        matrix.append(row)
    total = p_home + p_draw + p_away or 1.0
    p_home /= total
    p_draw /= total
    p_away /= total
    max_probability = -1.0
    best_home = 0
    best_away = 0
    for i_value, row in enumerate(matrix):
        for j_value, probability in enumerate(row):
            if probability > max_probability:
                max_probability = probability
                best_home, best_away = i_value, j_value
    return {
        "lambda_home": lambda_home + l3 if kind == "bivariate" else lambda_home,
        "lambda_away": lambda_away + l3 if kind == "bivariate" else lambda_away,
        "p_home": p_home,
        "p_draw": p_draw,
        "p_away": p_away,
        "matrix": matrix,
        "most_likely": [best_home, best_away],
        "most_likely_prob": max_probability,
    }


def predict_from_params(
    params: dict,
    model_name: str,
    home_team: str,
    away_team: str,
    train_matches: list[BettingMatch],
    rho: float,
    lambda_three: float,
    r_home: float,
    r_away: float,
) -> dict | None:
    home_idx = params["team_idx"].get(home_team)
    away_idx = params["team_idx"].get(away_team)
    if home_idx is None or away_idx is None:
        return None
    lambda_home = max(0.1, params["attack"][home_idx] * params["defense"][away_idx] * math.exp(params["home_adv"]))
    lambda_away = max(0.1, params["attack"][away_idx] * params["defense"][home_idx])
    if model_name == "Maher":
        return build_matrix(lambda_home, lambda_away, "independent")
    if model_name in {"Dixon-Coles", "Dixon-Coles TD"}:
        return build_matrix(lambda_home, lambda_away, "dixoncoles", rho=rho)
    if model_name == "Bivariate Poisson":
        return build_matrix(max(0.05, lambda_home - lambda_three), max(0.05, lambda_away - lambda_three), "bivariate", l3=lambda_three)
    if model_name == "Negative Binomial":
        return build_matrix(lambda_home, lambda_away, "negbin", r_home=r_home, r_away=r_away)
    return build_matrix(lambda_home, lambda_away, "independent")


def predict_match_tool(
    model_name: str,
    train_matches: list[BettingMatch],
    home_team: str,
    away_team: str,
    xi: float = 0.005,
) -> dict | None:
    if not train_matches:
        return None
    weights = None
    if model_name == "Dixon-Coles TD":
        total = len(train_matches)
        weights = [math.exp(-xi * (total - idx)) for idx in range(total)]
    params = estimate_params(train_matches, weights=weights)
    rho = estimate_rho(train_matches, params)
    lambda_three = estimate_lambda_three(train_matches)
    mean_home = sum(match.hg for match in train_matches) / len(train_matches)
    mean_away = sum(match.ag for match in train_matches) / len(train_matches)
    r_home = estimate_dispersion([match.hg for match in train_matches], mean_home)
    r_away = estimate_dispersion([match.ag for match in train_matches], mean_away)
    prediction = predict_from_params(params, model_name, home_team, away_team, train_matches, rho, lambda_three, r_home, r_away)
    if prediction is None:
        return None
    prediction["rho"] = rho
    prediction["lambda_three"] = lambda_three
    prediction["r_home"] = r_home
    prediction["r_away"] = r_away
    return prediction


def compute_table(matches: list[BettingMatch]) -> list[dict]:
    table: dict[str, dict] = {}
    for match in matches:
        home = table.setdefault(match.home, {"team": match.home, "p": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0, "pts": 0})
        away = table.setdefault(match.away, {"team": match.away, "p": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0, "pts": 0})
        home["p"] += 1
        away["p"] += 1
        home["gf"] += match.hg
        home["ga"] += match.ag
        away["gf"] += match.ag
        away["ga"] += match.hg
        if match.hg > match.ag:
            home["w"] += 1
            home["pts"] += 3
            away["l"] += 1
        elif match.hg < match.ag:
            away["w"] += 1
            away["pts"] += 3
            home["l"] += 1
        else:
            home["d"] += 1
            away["d"] += 1
            home["pts"] += 1
            away["pts"] += 1
    return sorted(
        table.values(),
        key=lambda row: (row["pts"], row["gf"] - row["ga"], row["gf"]),
        reverse=True,
    )


def simulate_league_tool(
    matches: list[BettingMatch],
    teams: list[str],
    model_name: str,
    train_pct: float,
    xi: float = 0.005,
) -> list[dict]:
    train_count = max(1, int(len(matches) * train_pct))
    train_matches = matches[:train_count]
    weights = None
    if model_name == "Dixon-Coles TD":
        weights = [math.exp(-xi * (train_count - idx)) for idx in range(train_count)]
    params = estimate_params(train_matches, weights=weights)
    rho = estimate_rho(train_matches, params)
    lambda_three = estimate_lambda_three(train_matches)
    mean_home = sum(match.hg for match in train_matches) / len(train_matches)
    mean_away = sum(match.ag for match in train_matches) / len(train_matches)
    r_home = estimate_dispersion([match.hg for match in train_matches], mean_home)
    r_away = estimate_dispersion([match.ag for match in train_matches], mean_away)

    predicted_matches: list[BettingMatch] = []
    for home_team in teams:
        for away_team in teams:
            if home_team == away_team:
                continue
            existing = next((match for match in train_matches if match.home == home_team and match.away == away_team), None)
            if existing is not None:
                predicted_matches.append(existing)
                continue
            prediction = predict_from_params(params, model_name, home_team, away_team, train_matches, rho, lambda_three, r_home, r_away)
            if prediction is None:
                continue
            home_goals = round(prediction["lambda_home"])
            away_goals = round(prediction["lambda_away"])
            predicted_matches.append(
                BettingMatch(
                    season=train_matches[-1].season,
                    date="",
                    home=home_team,
                    away=away_team,
                    hg=home_goals,
                    ag=away_goals,
                    result=normalize_result(home_goals, away_goals),
                )
            )
    return compute_table(predicted_matches)


def find_real_result(matches: list[BettingMatch], home_team: str, away_team: str) -> BettingMatch | None:
    for match in matches:
        if match.home == home_team and match.away == away_team:
            return match
    return None


def run_assumption_tests_tool(train_matches: list[BettingMatch]) -> dict:
    home_goals = [match.hg for match in train_matches]
    away_goals = [match.ag for match in train_matches]
    home_win_rate = sum(1 for match in train_matches if match.result == "H") / len(train_matches) * 100 if train_matches else 0
    return {
        "home_goal_gof": poisson_goodness_of_fit_test(home_goals),
        "away_goal_gof": poisson_goodness_of_fit_test(away_goals),
        "independence": independence_test(train_matches),
        "home_dispersion": dispersion_test(home_goals),
        "away_dispersion": dispersion_test(away_goals),
        "home_win_rate": home_win_rate,
        "mean_home_goals": sum(home_goals) / len(home_goals) if home_goals else 0,
        "mean_away_goals": sum(away_goals) / len(away_goals) if away_goals else 0,
    }


def evaluate_value_bet_tool(prediction: dict, market_match: BettingMatch | None) -> dict | None:
    if market_match is None or not (market_match.odds_home and market_match.odds_draw and market_match.odds_away):
        return None
    implied = {
        "home": 1 / market_match.odds_home,
        "draw": 1 / market_match.odds_draw,
        "away": 1 / market_match.odds_away,
    }
    total = sum(implied.values()) or 1.0
    market_probs = {key: value / total for key, value in implied.items()}
    model_probs = {
        "home": prediction["p_home"],
        "draw": prediction["p_draw"],
        "away": prediction["p_away"],
    }
    edges = {key: model_probs[key] - market_probs[key] for key in model_probs}
    best_side = max(edges, key=edges.get)
    best_edge = edges[best_side]
    fair_odds = 1 / max(model_probs[best_side], 1e-6)
    return {
        "bookmaker": market_match.odds_source,
        "market_probs": market_probs,
        "model_probs": model_probs,
        "edges": edges,
        "best_side": best_side,
        "best_edge": best_edge,
        "fair_odds": fair_odds,
        "recommended": best_edge >= 0.03,
    }


def build_hypothesis_tool(
    collection: dict,
    prediction: dict,
    assumptions: dict,
    market_edge: dict | None,
    home_team: str,
    away_team: str,
    model_name: str,
) -> dict:
    evidence = [
        f"{model_name} projects {home_team} at {prediction['lambda_home']:.2f} expected goals and {away_team} at {prediction['lambda_away']:.2f}.",
        f"Win / draw / loss probabilities are {prediction['p_home']*100:.1f}%, {prediction['p_draw']*100:.1f}%, and {prediction['p_away']*100:.1f}%.",
        f"The most likely exact score is {prediction['most_likely'][0]}-{prediction['most_likely'][1]} at {prediction['most_likely_prob']*100:.1f}%.",
        f"Training data covers {len(collection['training_matches']):,} historical matches across {collection['history_depth']} seasons.",
    ]
    caveats = [
        "Poisson-family models are descriptive and can miss tactical or injury-driven shifts.",
        "Model edges below roughly 3 percentage points are weak once bookmaker margin and lineup uncertainty are considered.",
    ]
    if assumptions.get("home_dispersion") and assumptions["home_dispersion"]["ratio"] > 1.15:
        caveats.append("Home-goal variance is above the Poisson mean, so exact-score confidence should be discounted.")
    if market_edge and market_edge["recommended"]:
        side_label = {"home": f"{home_team} win", "draw": "Draw", "away": f"{away_team} win"}[market_edge["best_side"]]
        statement = (
            f"The strongest betting angle is {side_label}: the model probability is {market_edge['model_probs'][market_edge['best_side']]*100:.1f}% "
            f"versus a market-implied {market_edge['market_probs'][market_edge['best_side']]*100:.1f}%."
        )
        evidence.append(
            f"Best market edge is {market_edge['best_edge']*100:.1f} points on {side_label}, implying fair odds of {market_edge['fair_odds']:.2f}."
        )
        title = "Model shows a potential value edge versus market odds"
        confidence = "medium"
    else:
        side_label = home_team if prediction["p_home"] >= max(prediction["p_draw"], prediction["p_away"]) else away_team
        statement = (
            f"The safest data-backed lean is toward {side_label}, but the page should present it as a probability edge rather than a guaranteed betting pick."
        )
        title = "Model favors one side, but the edge is modest"
        confidence = "medium" if prediction["most_likely_prob"] < 0.2 else "high"
    return {
        "title": title,
        "statement": statement,
        "confidence": confidence,
        "evidence": evidence,
        "caveats": caveats,
        "next_checks": [
            "Compare the same fixture under a second model such as Dixon-Coles or Bivariate Poisson.",
            "Re-run after excluding stale seasons or after refreshing current-season data.",
        ],
    }


def write_analysis_artifact(payload: dict, league_id: str, season_name: str, home_team: str, away_team: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    slug = f"{league_id}_{season_name.replace('/', '-')}_{home_team.replace(' ', '_')}_vs_{away_team.replace(' ', '_')}.md"
    path = REPORTS_DIR / slug
    hypothesis = payload["hypothesis"]
    lines = [
        f"# Betting Room Report: {home_team} vs {away_team}",
        "",
        f"- League: {payload['league_label']}",
        f"- Season: {season_name}",
        f"- Model: {payload['selected_model']}",
        "",
        f"## Hypothesis",
        "",
        f"**{hypothesis['title']}**",
        "",
        hypothesis["statement"],
        "",
        "## Evidence",
        "",
        *[f"- {item}" for item in hypothesis["evidence"]],
        "",
        "## Caveats",
        "",
        *[f"- {item}" for item in hypothesis["caveats"]],
    ]
    path.write_text("\n".join(lines))
    return path


def to_table(rows: list[dict], columns: list[str]) -> dict:
    return {
        "columns": columns,
        "rows": [[row.get(column) for column in columns] for row in rows],
    }


def options_payload(league_id: str = "E0", season_name: str | None = None, duckdb_path: str = DEFAULT_DUCKDB_PATH) -> dict:
    season_name = season_name or season_options()[0]
    collection = collect_match_data_tool(league_id, season_name, history_depth=2, duckdb_path=duckdb_path)
    return {
        "league_options": [
            {
                "value": code,
                "label": f"{mapping['country']} · {mapping['league']}",
            }
            for code, mapping in LEAGUE_IDS.items()
        ],
        "season_options": [{"value": season, "label": season} for season in season_options()],
        "team_options": [{"value": team, "label": team} for team in collection["teams"]],
        "selected_league": league_id,
        "selected_season": season_name,
        "league_label": f"{collection['country']} · {collection['league']}",
        "source_summary": f"Loaded {len(collection['current_matches'])} matches for {season_name} using {', '.join(collection['sources_used'])}.",
    }


def run_betting_analysis(
    league_id: str,
    season_name: str,
    home_team: str,
    away_team: str,
    model_name: str,
    *,
    train_pct: float = 0.7,
    xi: float = 0.005,
    force_refresh: bool = False,
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
) -> dict:
    """Run the full betting-room analysis pipeline for one selected fixture.

    The flow collects match data, builds a training set, fans out into
    probability, assumption-test, and league-context specialists, evaluates any
    bookmaker edge, synthesizes a betting hypothesis, and writes a persistent
    markdown artifact for the run.
    """
    started = time.perf_counter()
    tool_calls: list[dict] = []

    collect_start = time.perf_counter()
    collection = collect_match_data_tool(league_id, season_name, force_refresh=force_refresh, duckdb_path=duckdb_path)
    tool_calls.append(
        tool_call(
            "collect_match_data",
            "Collect Runtime Match Data",
            (
                f"Retrieved {len(collection['all_matches']):,} normalized matches for {collection['country']} {collection['league']} "
                f"across {collection['history_depth']} seasons using {', '.join(collection['sources_used'])}."
            ),
            function_name="collect_match_data_tool",
            duration_ms=round((time.perf_counter() - collect_start) * 1000),
        )
    )

    if home_team == away_team:
        raise ValueError("Home and away teams must be different.")
    if home_team not in collection["teams"] or away_team not in collection["teams"]:
        raise ValueError("Selected teams are not available in the chosen league/season.")

    current_matches = collection["current_matches"]
    train_count = max(1, int(len(current_matches) * train_pct))
    season_train = current_matches[:train_count]
    historical_train = collection["training_matches"][- max(40, train_count * 2) :]
    train_matches = historical_train + season_train

    def probability_specialist() -> dict:
        specialist_start = time.perf_counter()
        prediction = predict_match_tool(model_name, train_matches, home_team, away_team, xi=xi)
        if prediction is None:
            raise ValueError("Could not compute model probabilities for the selected fixture.")
        return {
            "result": prediction,
            "tool_call": tool_call(
                "probability_model",
                "Probability Specialist",
                (
                    f"Ran {model_name} via repo-derived estimation functions to build the full score matrix for {home_team} vs {away_team}."
                ),
                function_name="predict_match_tool",
                duration_ms=round((time.perf_counter() - specialist_start) * 1000),
            ),
            "summary": f"Most likely score is {prediction['most_likely'][0]}-{prediction['most_likely'][1]} at {prediction['most_likely_prob']*100:.1f}%.",
        }

    def assumption_specialist() -> dict:
        specialist_start = time.perf_counter()
        assumptions = run_assumption_tests_tool(train_matches)
        return {
            "result": assumptions,
            "tool_call": tool_call(
                "assumption_tests",
                "Assumption Test Specialist",
                "Computed Poisson goodness-of-fit, goal dispersion, and home-away independence tests over the training set.",
                function_name="run_assumption_tests_tool",
                duration_ms=round((time.perf_counter() - specialist_start) * 1000),
            ),
            "summary": f"Home-win rate in the training set is {assumptions['home_win_rate']:.1f}%.",
        }

    def league_specialist() -> dict:
        specialist_start = time.perf_counter()
        predicted_table = simulate_league_tool(current_matches, collection["teams"], model_name, train_pct, xi=xi)
        actual_table = compute_table(current_matches)
        return {
            "result": {"predicted_table": predicted_table[:10], "actual_table": actual_table[:10]},
            "tool_call": tool_call(
                "league_simulation",
                "League Simulation Specialist",
                "Simulated the full league table using the same model parameters to contextualize the selected fixture.",
                function_name="simulate_league_tool",
                duration_ms=round((time.perf_counter() - specialist_start) * 1000),
            ),
            "summary": f"Generated predicted and actual top-10 tables for {collection['league']} {season_name}.",
        }

    specialist_results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(probability_specialist): "probability",
            executor.submit(assumption_specialist): "assumptions",
            executor.submit(league_specialist): "league",
        }
        for future in as_completed(futures):
            key = futures[future]
            specialist_results[key] = future.result()

    probability_payload = specialist_results["probability"]["result"]
    assumptions_payload = specialist_results["assumptions"]["result"]
    league_payload = specialist_results["league"]["result"]
    tool_calls.extend(
        [
            specialist_results["probability"]["tool_call"],
            specialist_results["assumptions"]["tool_call"],
            specialist_results["league"]["tool_call"],
        ]
    )

    market_start = time.perf_counter()
    market_match = find_real_result(current_matches, home_team, away_team)
    market_edge = evaluate_value_bet_tool(probability_payload, market_match)
    tool_calls.append(
        tool_call(
            "market_edge",
            "Market Edge Specialist",
            (
                f"Compared model probabilities against {market_edge['bookmaker']} odds for the live fixture."
                if market_edge
                else "No bookmaker odds were present in the source row, so the market-edge comparison was skipped."
            ),
            function_name="evaluate_value_bet_tool",
            duration_ms=round((time.perf_counter() - market_start) * 1000),
        )
    )

    simulation_start = time.perf_counter()
    rng = random.Random(f"{league_id}:{season_name}:{home_team}:{away_team}:{model_name}")
    simulated_score = {
        "home": random_poisson(probability_payload["lambda_home"], rng),
        "away": random_poisson(probability_payload["lambda_away"], rng),
    }
    tool_calls.append(
        tool_call(
            "simulate_match",
            "Monte Carlo Match Sampler",
            f"Sampled one simulated scoreline from the model intensities: {simulated_score['home']}-{simulated_score['away']}.",
            function_name="random_poisson",
            duration_ms=round((time.perf_counter() - simulation_start) * 1000),
        )
    )

    hypothesis_start = time.perf_counter()
    hypothesis = build_hypothesis_tool(collection, probability_payload, assumptions_payload, market_edge, home_team, away_team, model_name)
    tool_calls.append(
        tool_call(
            "betting_hypothesis",
            "Betting Hypothesis Builder",
            "Synthesized the probability model, statistical tests, and market comparison into a data-backed betting thesis.",
            function_name="build_hypothesis_tool",
            duration_ms=round((time.perf_counter() - hypothesis_start) * 1000),
        )
    )

    actual_result = find_real_result(current_matches, home_team, away_team)
    score_rows = []
    for home_goals in range(min(9, len(probability_payload["matrix"]))):
        row = {"home_goals": home_goals}
        for away_goals in range(min(9, len(probability_payload["matrix"][home_goals]))):
            row[str(away_goals)] = round(probability_payload["matrix"][home_goals][away_goals] * 100, 1)
        score_rows.append(row)

    payload = {
        "league_id": league_id,
        "league_label": f"{collection['country']} · {collection['league']}",
        "selected_season": season_name,
        "selected_model": model_name,
        "home_team": home_team,
        "away_team": away_team,
        "actual_result": {"home": actual_result.hg, "away": actual_result.ag} if actual_result else None,
        "simulated_result": simulated_score,
        "probabilities": {
            "home": round(probability_payload["p_home"] * 100, 1),
            "draw": round(probability_payload["p_draw"] * 100, 1),
            "away": round(probability_payload["p_away"] * 100, 1),
        },
        "expected_goals": {
            "home": round(probability_payload["lambda_home"], 2),
            "away": round(probability_payload["lambda_away"], 2),
        },
        "most_likely_score": {
            "home": probability_payload["most_likely"][0],
            "away": probability_payload["most_likely"][1],
            "probability": round(probability_payload["most_likely_prob"] * 100, 1),
        },
        "score_matrix": {
            "max_goals": MAX_GOALS,
            "rows": score_rows,
        },
        "assumptions": assumptions_payload,
        "market_edge": market_edge,
        "predicted_table": league_payload["predicted_table"],
        "actual_table": league_payload["actual_table"],
        "highlights": [
            metric("Training matches", format_number(len(train_matches)), f"{collection['history_depth']} seasons"),
            metric("Expected goals", f"{probability_payload['lambda_home']:.2f} vs {probability_payload['lambda_away']:.2f}", f"{home_team} vs {away_team}"),
            metric("Strongest edge", f"{market_edge['best_edge']*100:.1f} pts" if market_edge else "No odds row", market_edge["best_side"] if market_edge else "Market comparison unavailable"),
        ],
        "tool_calls": tool_calls,
        "hypothesis": hypothesis,
        "executive_summary": [
            f"The page fetched real match data at runtime for {collection['country']} {collection['league']} {season_name}.",
            specialist_results["probability"]["summary"],
            specialist_results["assumptions"]["summary"],
            hypothesis["statement"],
        ],
        "tables": {
            "score_matrix": to_table(score_rows, ["home_goals"] + [str(idx) for idx in range(9)]),
            "predicted_table": to_table(league_payload["predicted_table"], ["team", "p", "w", "d", "l", "gf", "ga", "pts"]),
            "actual_table": to_table(league_payload["actual_table"], ["team", "p", "w", "d", "l", "gf", "ga", "pts"]),
        },
        "data_mode": "external_runtime" if not collection["fallback_used"] else "external_runtime_with_fallback",
        "runtime_ms": round((time.perf_counter() - started) * 1000),
        "source_methods": ["web_csv_fetch", "artifact_cache" if "artifact_cache" in collection["sources_used"] else "duckdb_fallback"],
    }
    artifact_path = write_analysis_artifact(payload, league_id, season_name, home_team, away_team)
    payload["artifact_path"] = str(artifact_path.relative_to(REPO_ROOT))
    return payload
