#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import csv
import io
import logging
import math
import os
import re
import sys
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from google.cloud import storage
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.football-data.co.uk/"
DATA_INDEX_URL = urljoin(BASE_URL, "data.php")
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_OBJECT_NAME = "league_data.parquet"
SEASON_RE = re.compile(r"^Season\s+(\d{4})/(\d{4})$")
COUNTRY_LINK_RE = re.compile(r"^(?P<country>.+?) Football Results$", re.IGNORECASE)
UNSAFE_PATH_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")
NON_ALNUM_COLUMN_CHARS_RE = re.compile(r"[^a-z0-9_]+")
MULTI_UNDERSCORE_RE = re.compile(r"_+")
NULL_TOKENS = frozenset({"", "na", "n/a", "null", "none", "nil", "-", "--"})

CANONICAL_COLUMN_ALIASES = {
    "home": "hometeam",
    "away": "awayteam",
    "hg": "fthg",
    "ag": "ftag",
    "res": "ftr",
}

STRING_COLUMNS = frozenset(
    {
        "div",
        "country",
        "league",
        "season",
        "source_url",
        "source_type",
        "date",
        "time",
        "hometeam",
        "awayteam",
        "referee",
        "ftr",
        "htr",
    }
)

INTEGER_COLUMNS = frozenset(
    {
        "fthg",
        "ftag",
        "hthg",
        "htag",
        "attendance",
        "hs",
        "as",
        "hst",
        "ast",
        "hc",
        "ac",
        "hf",
        "af",
        "hy",
        "ay",
        "hr",
        "ar",
        "bb1x2",
        "bbou",
        "bbah",
    }
)

LOGGER = logging.getLogger("football-data-to-gcs")


@dataclass(frozen=True)
class CountryPage:
    country: str
    url: str
    league_hint: str | None = None


@dataclass(frozen=True)
class LeagueDataset:
    country: str
    league: str | None
    season: str | None
    csv_url: str
    source_type: str = "seasonal"


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_lookup_key(value: str) -> str:
    return normalize_space(value).casefold()


def sanitize_path_segment(value: str) -> str:
    cleaned = normalize_space(value)
    cleaned = cleaned.casefold()
    cleaned = cleaned.replace(" ", "_")
    cleaned = cleaned.replace("/", "-").replace("\\", "-")
    cleaned = UNSAFE_PATH_CHARS_RE.sub("", cleaned)
    cleaned = cleaned.strip(" .")
    return cleaned or "unknown"


def parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def normalize_season_value(value: str) -> str:
    normalized = normalize_space(value)
    season = parse_season_heading(f"Season {normalized}")
    return season or normalized


def normalize_column_name(value: str) -> str:
    cleaned = normalize_space(value)
    replacements = (
        (">=", "_gte_"),
        ("<=", "_lte_"),
        (">", "_gt_"),
        ("<", "_lt_"),
        ("=", "_eq_"),
        ("%", "_pct_"),
        ("+", "_plus_"),
        ("&", "_and_"),
        ("@", "_at_"),
        ("#", "_num_"),
    )

    for old, new in replacements:
        cleaned = cleaned.replace(old, new)

    cleaned = cleaned.casefold()
    cleaned = cleaned.replace(".", "_")
    cleaned = NON_ALNUM_COLUMN_CHARS_RE.sub("_", cleaned)
    cleaned = MULTI_UNDERSCORE_RE.sub("_", cleaned)
    cleaned = cleaned.strip("_")

    if not cleaned:
        cleaned = "col"
    if cleaned[0].isdigit():
        cleaned = f"col_{cleaned}"

    return CANONICAL_COLUMN_ALIASES.get(cleaned, cleaned)


def standardize_column_names(column_names: list[str]) -> list[str]:
    standardized_names: list[str] = []
    counts: dict[str, int] = collections.defaultdict(int)

    for column_name in column_names:
        normalized = normalize_column_name(column_name)
        counts[normalized] += 1
        if counts[normalized] > 1:
            normalized = f"{normalized}_{counts[normalized]}"
        standardized_names.append(normalized)

    return standardized_names


def standardize_table_columns(table: pa.Table) -> pa.Table:
    renamed_columns = standardize_column_names(list(table.column_names))
    if renamed_columns == list(table.column_names):
        return table
    return table.rename_columns(renamed_columns)


def normalize_string_value(value: object) -> str | None:
    if value is None:
        return None
    cleaned = normalize_space(str(value))
    if cleaned.casefold() in NULL_TOKENS:
        return None
    return cleaned or None


def normalize_float_value(value: object) -> float | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return float(value)

    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)

    cleaned = normalize_space(str(value))
    if cleaned.casefold() in NULL_TOKENS:
        return None

    cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def normalize_int_value(value: object) -> int | None:
    numeric_value = normalize_float_value(value)
    if numeric_value is None:
        return None
    if numeric_value.is_integer():
        return int(numeric_value)
    return None


def standardize_table_schema(table: pa.Table) -> pa.Table:
    arrays: list[pa.Array] = []

    for column_name in table.column_names:
        values = table[column_name].to_pylist()
        if column_name in STRING_COLUMNS:
            arrays.append(pa.array([normalize_string_value(value) for value in values], type=pa.string()))
        elif column_name in INTEGER_COLUMNS:
            arrays.append(pa.array([normalize_int_value(value) for value in values], type=pa.int64()))
        else:
            arrays.append(pa.array([normalize_float_value(value) for value in values], type=pa.float64()))

    return pa.table(arrays, names=table.column_names)


def add_or_replace_string_column(table: pa.Table, column_name: str, value: str) -> pa.Table:
    column_values = [value] * len(table)
    column_array = pa.array(column_values, type=pa.string())
    column_index = table.column_names.index(column_name) if column_name in table.column_names else -1

    if column_index >= 0:
        table = table.remove_column(column_index)
        table = table.add_column(column_index, column_name, column_array)
        return table

    return table.append_column(column_name, column_array)


def attach_partition_columns(
    table: pa.Table,
    country: str,
    league: str,
    season: str,
    source_url: str,
    source_type: str,
) -> pa.Table:
    enriched = table
    enriched = add_or_replace_string_column(enriched, "country", country)
    enriched = add_or_replace_string_column(enriched, "league", league)
    enriched = add_or_replace_string_column(enriched, "season", season)
    enriched = add_or_replace_string_column(enriched, "source_url", source_url)
    enriched = add_or_replace_string_column(enriched, "source_type", source_type)
    return enriched


def env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def parse_bool(value: str) -> bool:
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def env_bool(default: bool, *names: str) -> bool:
    value = env_first(*names)
    if value is None:
        return default
    return parse_bool(value)


def env_int(default: int, *names: str) -> int:
    value = env_first(*names)
    if value is None:
        return default
    return int(value)


def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "football-data-gcs-downloader/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
        }
    )
    return session


def fetch_text(session: requests.Session, url: str, timeout: int) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def fetch_bytes(session: requests.Session, url: str, timeout: int) -> bytes:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


def csv_to_parquet_bytes(csv_content: bytes) -> bytes:
    table = parse_table_from_csv_bytes(csv_content)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def decode_csv_text(csv_content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return csv_content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", csv_content, 0, 1, "Unable to decode CSV bytes.")


def normalize_csv_bytes(csv_content: bytes) -> bytes:
    decoded_text = decode_csv_text(csv_content).replace("\r\n", "\n").replace("\r", "\n")
    reader = csv.reader(io.StringIO(decoded_text))

    normalized_rows: list[list[str]] = []
    header: list[str] | None = None
    expected_columns = 0

    for row in reader:
        trimmed_row = list(row)
        while trimmed_row and not trimmed_row[-1].strip():
            trimmed_row.pop()

        if not any(cell.strip() for cell in trimmed_row):
            continue

        if header is None:
            header = trimmed_row
            expected_columns = len(header)
            normalized_rows.append(header)
            continue

        if len(trimmed_row) < expected_columns:
            trimmed_row.extend([""] * (expected_columns - len(trimmed_row)))
        elif len(trimmed_row) > expected_columns:
            extra_values = trimmed_row[expected_columns:]
            if any(cell.strip() for cell in extra_values):
                header.extend(
                    f"extra_col_{index}"
                    for index in range(expected_columns + 1, len(trimmed_row) + 1)
                )
                expected_columns = len(header)
                normalized_rows[0] = header
                for normalized_row in normalized_rows[1:]:
                    normalized_row.extend([""] * (expected_columns - len(normalized_row)))
            else:
                trimmed_row = trimmed_row[:expected_columns]

        normalized_rows.append(trimmed_row)

    if not normalized_rows:
        raise ValueError("CSV file is empty after normalization.")

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerows(normalized_rows)
    return output.getvalue().encode("utf-8")


def parse_table_from_csv_bytes(csv_content: bytes) -> pa.Table:
    normalized_bytes = normalize_csv_bytes(csv_content)
    table = pacsv.read_csv(pa.BufferReader(normalized_bytes))
    table = standardize_table_columns(table)
    return standardize_table_schema(table)


def find_csv_links(page_url: str, soup: BeautifulSoup) -> list[str]:
    csv_links: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        csv_url = urljoin(page_url, anchor["href"])
        if not csv_url.lower().endswith(".csv"):
            continue
        if csv_url.casefold() in seen:
            continue
        csv_links.append(csv_url)
        seen.add(csv_url.casefold())

    return csv_links


def extract_league_hint(anchor: Tag) -> str | None:
    parts: list[str] = []

    for sibling in anchor.next_siblings:
        if isinstance(sibling, Tag):
            if sibling.name == "a":
                break
            text = normalize_space(sibling.get_text(" ", strip=True))
        else:
            text = normalize_space(str(sibling))

        if text:
            parts.append(text)

    if not parts:
        return None

    hint = normalize_space(" ".join(parts))
    return hint or None


def parse_country_pages(index_html: str) -> list[CountryPage]:
    soup = BeautifulSoup(index_html, "html.parser")
    pages: list[CountryPage] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        label = normalize_space(anchor.get_text(" ", strip=True))
        match = COUNTRY_LINK_RE.match(label)
        if not match:
            continue

        country = normalize_space(match.group("country"))
        key = normalize_lookup_key(country)
        if key in seen:
            continue

        pages.append(
            CountryPage(
                country=country,
                url=urljoin(DATA_INDEX_URL, anchor["href"]),
                league_hint=extract_league_hint(anchor),
            )
        )
        seen.add(key)

    if not pages:
        raise ValueError("Could not find any country pages on the Football-Data index page.")

    return pages


def parse_season_heading(value: str) -> str | None:
    match = SEASON_RE.match(value)
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)}"


def parse_country_page(country: str, page_url: str, html: str, league_hint: str | None) -> list[LeagueDataset]:
    soup = BeautifulSoup(html, "html.parser")
    root: Tag | BeautifulSoup = soup.body or soup
    current_season: str | None = None
    datasets: list[LeagueDataset] = []
    seen: set[tuple[str, str, str]] = set()

    for node in root.descendants:
        if isinstance(node, NavigableString):
            season = parse_season_heading(normalize_space(str(node)))
            if season:
                current_season = season
            continue

        if not isinstance(node, Tag) or node.name != "a" or not current_season:
            continue

        href = node.get("href")
        if not href:
            continue

        csv_url = urljoin(page_url, href)
        if not csv_url.lower().endswith(".csv"):
            continue

        league = normalize_space(node.get_text(" ", strip=True))
        if not league:
            continue

        key = (
            normalize_lookup_key(current_season),
            normalize_lookup_key(league),
            csv_url.casefold(),
        )
        if key in seen:
            continue

        datasets.append(
            LeagueDataset(
                country=country,
                league=league,
                season=current_season,
                csv_url=csv_url,
                source_type="seasonal",
            )
        )
        seen.add(key)

    if datasets:
        return datasets

    generic_csv_links = find_csv_links(page_url, soup)
    if generic_csv_links:
        return [
            LeagueDataset(
                country=country,
                league=league_hint,
                season=None,
                csv_url=csv_url,
                source_type="combined",
            )
            for csv_url in generic_csv_links
        ]

    LOGGER.warning("Skipping %s (%s): no CSV datasets were found.", country, page_url)
    return []


def filter_country_pages(pages: Iterable[CountryPage], countries: set[str]) -> list[CountryPage]:
    pages = list(pages)
    if not countries:
        return pages

    selected = [page for page in pages if normalize_lookup_key(page.country) in countries]
    found = {normalize_lookup_key(page.country) for page in selected}
    missing = sorted(countries - found)

    if missing:
        available = ", ".join(page.country for page in pages)
        raise ValueError(
            "Unknown countries requested: "
            + ", ".join(missing)
            + f". Available countries: {available}"
        )

    return selected


def filter_datasets(datasets: Iterable[LeagueDataset], seasons: set[str]) -> list[LeagueDataset]:
    datasets = list(datasets)
    if not seasons:
        return datasets
    return [
        dataset
        for dataset in datasets
        if dataset.season is None or dataset.season in seasons
    ]


def build_object_name(prefix: str, dataset: LeagueDataset, object_name: str) -> str:
    return build_object_name_from_values(
        prefix=prefix,
        country=dataset.country,
        league=dataset.league or dataset.country,
        season=dataset.season or "unknown",
        object_name=object_name,
    )


def build_object_name_from_values(
    prefix: str,
    country: str,
    league: str,
    season: str,
    object_name: str,
) -> str:
    parts = [
        f"country={sanitize_path_segment(country)}",
        f"league={sanitize_path_segment(league)}",
        f"season={sanitize_path_segment(season)}",
        object_name,
    ]
    if prefix:
        parts.insert(0, prefix.strip("/"))
    return "/".join(parts)


def build_storage_client(project_id: str | None, credentials_file: str | None) -> storage.Client:
    if credentials_file:
        return storage.Client.from_service_account_json(credentials_file, project=project_id)
    return storage.Client(project=project_id)


def upload_blob_content(
    bucket: storage.Bucket | None,
    object_path: str,
    content: bytes,
    metadata: dict[str, str],
    dry_run: bool,
    skip_existing: bool,
    client: storage.Client | None,
) -> bool:
    if dry_run:
        LOGGER.info("DRY RUN %s", object_path)
        return True

    if bucket is None or client is None:
        raise ValueError("A storage bucket and client are required unless dry-run is enabled.")

    blob = bucket.blob(object_path)
    if skip_existing and blob.exists(client=client):
        LOGGER.info("SKIP %s", object_path)
        return False

    blob.metadata = metadata
    blob.upload_from_string(content, content_type="application/octet-stream")
    LOGGER.info("UPLOADED %s", object_path)
    return True


def resolve_column_name(table: pa.Table, expected_name: str) -> str | None:
    lookup = {column_name.casefold(): column_name for column_name in table.column_names}
    return lookup.get(expected_name.casefold())


def process_combined_dataset(
    bucket: storage.Bucket | None,
    dataset: LeagueDataset,
    csv_content: bytes,
    prefix: str,
    object_name: str,
    dry_run: bool,
    skip_existing: bool,
    client: storage.Client | None,
    season_filters: set[str],
) -> int:
    table = parse_table_from_csv_bytes(csv_content)
    country_column_name = resolve_column_name(table, "Country")
    league_column_name = resolve_column_name(table, "League")
    season_column_name = resolve_column_name(table, "Season")

    if season_column_name is None:
        raise ValueError(
            "Combined CSV does not include a Season column, so it cannot be partitioned "
            "into country/league/season folders."
        )

    country_values = (
        table[country_column_name].to_pylist()
        if country_column_name is not None
        else [dataset.country] * len(table)
    )
    league_values = (
        table[league_column_name].to_pylist()
        if league_column_name is not None
        else [dataset.league or dataset.country] * len(table)
    )
    season_values = table[season_column_name].to_pylist()

    grouped_indices: dict[tuple[str, str, str], list[int]] = collections.defaultdict(list)

    for row_index, (country_value, league_value, season_value) in enumerate(
        zip(country_values, league_values, season_values, strict=True)
    ):
        if season_value is None:
            continue

        country_name = normalize_space(str(country_value or dataset.country))
        league_name = normalize_space(str(league_value or dataset.league or dataset.country))
        season_name = normalize_season_value(str(season_value))

        if season_filters and season_name not in season_filters:
            continue

        grouped_indices[(country_name, league_name, season_name)].append(row_index)

    if not grouped_indices:
        LOGGER.warning(
            "Skipping %s (%s): no rows matched the requested season filters in the combined file.",
            dataset.country,
            dataset.csv_url,
        )
        return 0

    processed = 0
    for (country_name, league_name, season_name), row_indices in grouped_indices.items():
        object_path = build_object_name_from_values(
            prefix=prefix,
            country=country_name,
            league=league_name,
            season=season_name,
            object_name=object_name,
        )

        if dry_run:
            LOGGER.info("DRY RUN %s <- %s", object_path, dataset.csv_url)
            processed += 1
            continue

        partition = table.take(pa.array(row_indices, type=pa.int64()))
        partition = attach_partition_columns(
            table=partition,
            country=country_name,
            league=league_name,
            season=season_name,
            source_url=dataset.csv_url,
            source_type="combined",
        )
        sink = pa.BufferOutputStream()
        pq.write_table(partition, sink, compression="snappy")
        metadata = {
            "source_url": dataset.csv_url,
            "source_format": "csv",
            "stored_format": "parquet",
            "source_type": "combined",
            "country": country_name,
            "league": league_name,
            "season": season_name,
        }
        if upload_blob_content(
            bucket=bucket,
            object_path=object_path,
            content=sink.getvalue().to_pybytes(),
            metadata=metadata,
            dry_run=False,
            skip_existing=skip_existing,
            client=client,
        ):
            processed += 1

    return processed


def process_dataset(
    session: requests.Session,
    bucket: storage.Bucket | None,
    dataset: LeagueDataset,
    prefix: str,
    object_name: str,
    timeout: int,
    dry_run: bool,
    skip_existing: bool,
    client: storage.Client | None,
    season_filters: set[str],
) -> int:
    csv_content = fetch_bytes(session, dataset.csv_url, timeout)
    if dataset.source_type == "combined":
        return process_combined_dataset(
            bucket=bucket,
            dataset=dataset,
            csv_content=csv_content,
            prefix=prefix,
            object_name=object_name,
            dry_run=dry_run,
            skip_existing=skip_existing,
            client=client,
            season_filters=season_filters,
        )

    object_path = build_object_name(prefix, dataset, object_name)
    table = parse_table_from_csv_bytes(csv_content)
    table = attach_partition_columns(
        table=table,
        country=dataset.country,
        league=dataset.league or dataset.country,
        season=dataset.season or "unknown",
        source_url=dataset.csv_url,
        source_type="seasonal",
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    parquet_content = sink.getvalue().to_pybytes()
    metadata = {
        "source_url": dataset.csv_url,
        "source_format": "csv",
        "stored_format": "parquet",
        "source_type": "seasonal",
        "country": dataset.country,
        "league": dataset.league or dataset.country,
        "season": dataset.season or "unknown",
    }
    return int(
        upload_blob_content(
            bucket=bucket,
            object_path=object_path,
            content=parquet_content,
            metadata=metadata,
            dry_run=dry_run,
            skip_existing=skip_existing,
            client=client,
        )
    )


def parse_args() -> argparse.Namespace:
    try:
        bucket_default = env_first("FOOTBALL_DATA_BUCKET")
        bucket_prefix_default = env_first("FOOTBALL_DATA_BUCKET_PREFIX") or ""
        countries_default = env_first("FOOTBALL_DATA_COUNTRIES")
        seasons_default = env_first("FOOTBALL_DATA_SEASONS")
        project_default = env_first("FOOTBALL_DATA_PROJECT_ID", "GOOGLE_CLOUD_PROJECT", "GCP_PROJECT")
        credentials_default = env_first(
            "FOOTBALL_DATA_CREDENTIALS_FILE",
            "GOOGLE_APPLICATION_CREDENTIALS",
        )
        object_name_default = env_first("FOOTBALL_DATA_OBJECT_NAME") or DEFAULT_OBJECT_NAME
        timeout_default = env_int(DEFAULT_TIMEOUT_SECONDS, "FOOTBALL_DATA_TIMEOUT")
        log_level_default = env_first("FOOTBALL_DATA_LOG_LEVEL") or "INFO"
        skip_existing_default = env_bool(False, "FOOTBALL_DATA_SKIP_EXISTING")
        dry_run_default = env_bool(False, "FOOTBALL_DATA_DRY_RUN")
    except ValueError as exc:
        raise SystemExit(f"Invalid environment configuration: {exc}") from exc

    parser = argparse.ArgumentParser(
        description=(
            "Download league CSV files from football-data.co.uk and upload them to "
            "Google Cloud Storage using the path country/league/season/league_data.parquet. "
            "CLI flags take precedence over FOOTBALL_DATA_* environment variables."
        )
    )
    parser.add_argument(
        "--bucket",
        default=bucket_default,
        help="Target Google Cloud Storage bucket name. Env: FOOTBALL_DATA_BUCKET.",
    )
    parser.add_argument(
        "--bucket-prefix",
        default=bucket_prefix_default,
        help=(
            "Optional prefix to prepend inside the bucket, for example raw/football-data. "
            "Env: FOOTBALL_DATA_BUCKET_PREFIX."
        ),
    )
    parser.add_argument(
        "--countries",
        default=countries_default,
        help=(
            "Optional comma-separated country filter, for example England,Spain,USA. "
            "Env: FOOTBALL_DATA_COUNTRIES."
        ),
    )
    parser.add_argument(
        "--seasons",
        default=seasons_default,
        help=(
            "Optional comma-separated season filter using YYYY-YYYY, for example "
            "2025-2026,2024-2025. Env: FOOTBALL_DATA_SEASONS."
        ),
    )
    parser.add_argument(
        "--project-id",
        default=project_default,
        help=(
            "Optional Google Cloud project ID. Env: FOOTBALL_DATA_PROJECT_ID, "
            "GOOGLE_CLOUD_PROJECT, or GCP_PROJECT."
        ),
    )
    parser.add_argument(
        "--credentials-file",
        default=credentials_default,
        help=(
            "Optional path to a service account JSON key. If omitted, Application Default "
            "Credentials are used. Env: FOOTBALL_DATA_CREDENTIALS_FILE or "
            "GOOGLE_APPLICATION_CREDENTIALS."
        ),
    )
    parser.add_argument(
        "--object-name",
        default=object_name_default,
        help=(
            "File name to use inside each season folder. Defaults to league_data.parquet. "
            "Env: FOOTBALL_DATA_OBJECT_NAME."
        ),
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=skip_existing_default,
        help="Skip uploads when the target object already exists in the bucket.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=dry_run_default,
        help="Print the bucket paths that would be written without downloading or uploading any files.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=timeout_default,
        help=(
            f"HTTP timeout in seconds. Defaults to {DEFAULT_TIMEOUT_SECONDS}. "
            "Env: FOOTBALL_DATA_TIMEOUT."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=log_level_default,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity. Defaults to INFO. Env: FOOTBALL_DATA_LOG_LEVEL.",
    )

    args = parser.parse_args()
    if not args.dry_run and not args.bucket:
        parser.error("--bucket is required unless --dry-run is used.")

    if "/" in args.object_name or "\\" in args.object_name:
        parser.error("--object-name must be a file name, not a path.")

    return args


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s %(message)s",
    )

    country_filters = {normalize_lookup_key(value) for value in parse_csv_list(args.countries)}
    season_filters = {normalize_season_value(value) for value in parse_csv_list(args.seasons)}

    session = build_session()
    client: storage.Client | None = None
    bucket: storage.Bucket | None = None

    if not args.dry_run:
        client = build_storage_client(args.project_id, args.credentials_file)
        bucket = client.bucket(args.bucket)

    index_html = fetch_text(session, DATA_INDEX_URL, args.timeout)
    country_pages = filter_country_pages(parse_country_pages(index_html), country_filters)
    LOGGER.info("Found %s country pages to process.", len(country_pages))

    datasets: list[LeagueDataset] = []
    for page in country_pages:
        page_html = fetch_text(session, page.url, args.timeout)
        page_datasets = filter_datasets(
            parse_country_page(page.country, page.url, page_html, page.league_hint),
            season_filters,
        )
        LOGGER.info("Discovered %s datasets for %s.", len(page_datasets), page.country)
        datasets.extend(page_datasets)

    if not datasets:
        LOGGER.warning("No datasets matched the supplied filters.")
        return 0

    LOGGER.info("Preparing to process %s datasets.", len(datasets))

    failures: list[str] = []
    processed = 0
    for dataset in datasets:
        try:
            processed += process_dataset(
                session=session,
                bucket=bucket,
                dataset=dataset,
                prefix=args.bucket_prefix,
                object_name=args.object_name,
                timeout=args.timeout,
                dry_run=args.dry_run,
                skip_existing=args.skip_existing,
                client=client,
                season_filters=season_filters,
            )
        except Exception as exc:  # pragma: no cover - defensive runtime logging
            failures.append(
                f"{dataset.country} | {dataset.league} | {dataset.season} | {dataset.csv_url} | {exc}"
            )
            LOGGER.error(
                "FAILED %s / %s / %s: %s",
                dataset.country,
                dataset.league,
                dataset.season,
                exc,
            )

    LOGGER.info("Completed %s successful uploads.", processed)
    if failures:
        LOGGER.error("Encountered %s failures.", len(failures))
        for failure in failures:
            LOGGER.error("%s", failure)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
