#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import logging
import sys
import threading
from datetime import date, datetime, timedelta
from urllib.parse import urljoin

import pyarrow as pa
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from historical_football_data_to_gcs import (
    DATA_INDEX_URL,
    DEFAULT_DUCKDB_TABLE,
    DEFAULT_OBJECT_NAME,
    DEFAULT_TIMEOUT_SECONDS,
    LeagueDataset,
    build_duckdb_connection,
    build_session,
    build_storage_client,
    discover_datasets,
    env_bool,
    env_first,
    env_int,
    fetch_bytes,
    fetch_text,
    filter_country_pages,
    find_csv_links,
    normalize_lookup_key,
    normalize_season_value,
    normalize_space,
    parse_csv_list,
    parse_country_pages,
    parse_season_heading,
    parse_table_from_csv_bytes,
    process_dataset,
    resolve_column_name,
    season_sort_key,
)

LOGGER = logging.getLogger("football-data-recent-refresh")
DEFAULT_LOOKBACK_DAYS = 2
DEFAULT_WORKERS = 8
DATE_FORMATS = (
    "%d/%m/%Y",
    "%d/%m/%y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d-%m-%y",
)
THREAD_LOCAL = threading.local()


def parse_match_date(value: object) -> date | None:
    if value is None:
        return None

    cleaned = normalize_space(str(value))
    if not cleaned:
        return None

    candidates = [cleaned]
    if " " in cleaned:
        candidates.append(cleaned.split(" ", maxsplit=1)[0])

    for candidate in candidates:
        for date_format in DATE_FORMATS:
            try:
                return datetime.strptime(candidate, date_format).date()
            except ValueError:
                continue

    return None


def parse_latest_country_page(
    country: str,
    page_url: str,
    html: str,
    league_hint: str | None,
) -> list[LeagueDataset]:
    soup = BeautifulSoup(html, "html.parser")
    root: Tag | BeautifulSoup = soup.body or soup
    latest_season: str | None = None

    for node in root.descendants:
        if not isinstance(node, NavigableString):
            continue

        season = parse_season_heading(normalize_space(str(node)))
        if season and (
            latest_season is None or season_sort_key(season) > season_sort_key(latest_season)
        ):
            latest_season = season

    if latest_season is None:
        generic_csv_links = find_csv_links(page_url, soup)
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

    datasets: list[LeagueDataset] = []
    seen: set[tuple[str, str]] = set()
    current_season: str | None = None

    for node in root.descendants:
        if isinstance(node, NavigableString):
            season = parse_season_heading(normalize_space(str(node)))
            if season:
                current_season = season
            continue

        if not isinstance(node, Tag) or node.name != "a" or current_season != latest_season:
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

        key = (normalize_lookup_key(league), csv_url.casefold())
        if key in seen:
            continue

        datasets.append(
            LeagueDataset(
                country=country,
                league=league,
                season=latest_season,
                csv_url=csv_url,
                source_type="seasonal",
            )
        )
        seen.add(key)

    if datasets:
        return datasets

    generic_csv_links = find_csv_links(page_url, soup)
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


def discover_latest_datasets(
    session,
    timeout: int,
    country_filters: set[str],
    workers: int,
) -> list[LeagueDataset]:
    index_html = fetch_text(session, DATA_INDEX_URL, timeout)
    country_pages = filter_country_pages(parse_country_pages(index_html), country_filters)
    LOGGER.info("Found %s country pages to process.", len(country_pages))

    if not country_pages:
        return []

    def fetch_latest_page(page) -> tuple[str, list[LeagueDataset]]:
        worker_session = build_session()
        try:
            page_html = fetch_text(worker_session, page.url, timeout)
            return (
                page.country,
                parse_latest_country_page(page.country, page.url, page_html, page.league_hint),
            )
        finally:
            worker_session.close()

    datasets: list[LeagueDataset] = []
    max_workers = min(workers, len(country_pages))
    if max_workers <= 1:
        for page in country_pages:
            country_name, page_datasets = fetch_latest_page(page)
            LOGGER.info("Discovered %s latest-season datasets for %s.", len(page_datasets), country_name)
            datasets.extend(page_datasets)
        return datasets

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(fetch_latest_page, page): page.country
            for page in country_pages
        }
        for future in concurrent.futures.as_completed(future_to_page):
            country_name = future_to_page[future]
            page_country, page_datasets = future.result()
            LOGGER.info(
                "Discovered %s latest-season datasets for %s.",
                len(page_datasets),
                page_country or country_name,
            )
            datasets.extend(page_datasets)

    return datasets


def get_worker_session():
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = build_session()
        THREAD_LOCAL.session = session
    return session


def get_worker_storage_target(
    project_id: str | None,
    credentials_file: str | None,
    bucket_name: str | None,
):
    if not bucket_name:
        return None, None

    storage_key = (project_id, credentials_file, bucket_name)
    cached_key = getattr(THREAD_LOCAL, "storage_key", None)
    client = getattr(THREAD_LOCAL, "storage_client", None)
    bucket = getattr(THREAD_LOCAL, "storage_bucket", None)

    if client is None or bucket is None or cached_key != storage_key:
        client = build_storage_client(project_id, credentials_file)
        bucket = client.bucket(bucket_name)
        THREAD_LOCAL.storage_key = storage_key
        THREAD_LOCAL.storage_client = client
        THREAD_LOCAL.storage_bucket = bucket

    return client, bucket


def recent_seasons_for_dataset(
    dataset: LeagueDataset,
    table: pa.Table,
    start_date: date,
    end_date: date,
) -> set[str]:
    date_column_name = resolve_column_name(table, "date")
    if date_column_name is None:
        LOGGER.warning("Skipping %s: no date column found in %s", dataset.csv_url, dataset.country)
        return set()

    date_values = table[date_column_name].to_pylist()
    season_values = table[resolve_column_name(table, "season")].to_pylist() if resolve_column_name(table, "season") else []
    matched_seasons: set[str] = set()
    has_recent_rows = False

    for row_index, date_value in enumerate(date_values):
        parsed_date = parse_match_date(date_value)
        if parsed_date is None or parsed_date < start_date or parsed_date > end_date:
            continue

        has_recent_rows = True
        if dataset.source_type == "combined":
            if not season_values:
                continue
            season_value = season_values[row_index]
            if season_value is None:
                continue
            matched_seasons.add(normalize_season_value(str(season_value)))

    if dataset.source_type == "combined":
        return matched_seasons

    if has_recent_rows and dataset.season is not None:
        return {dataset.season}
    return set()


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
        duckdb_path_default = env_first("FOOTBALL_DATA_DUCKDB_PATH")
        duckdb_table_default = env_first("FOOTBALL_DATA_DUCKDB_TABLE") or DEFAULT_DUCKDB_TABLE
        timeout_default = env_int(DEFAULT_TIMEOUT_SECONDS, "FOOTBALL_DATA_TIMEOUT")
        workers_default = env_int(DEFAULT_WORKERS, "FOOTBALL_DATA_WORKERS")
        log_level_default = env_first("FOOTBALL_DATA_LOG_LEVEL") or "INFO"
        dry_run_default = env_bool(False, "FOOTBALL_DATA_DRY_RUN")
        lookback_days_default = env_int(DEFAULT_LOOKBACK_DAYS, "FOOTBALL_DATA_LOOKBACK_DAYS")
    except ValueError as exc:
        raise SystemExit(f"Invalid environment configuration: {exc}") from exc

    parser = argparse.ArgumentParser(
        description=(
            "Refresh only recently active football-data.co.uk partitions. The script scans the "
            "latest seasonal datasets plus extra-league combined files, detects rows from the "
            f"last N days, and refreshes the affected GCS and DuckDB partitions."
        )
    )
    parser.add_argument("--bucket", default=bucket_default, help="Target GCS bucket. Env: FOOTBALL_DATA_BUCKET.")
    parser.add_argument(
        "--bucket-prefix",
        default=bucket_prefix_default,
        help="Optional GCS prefix. Env: FOOTBALL_DATA_BUCKET_PREFIX.",
    )
    parser.add_argument(
        "--countries",
        default=countries_default,
        help="Optional comma-separated country filter. Env: FOOTBALL_DATA_COUNTRIES.",
    )
    parser.add_argument(
        "--seasons",
        default=seasons_default,
        help="Optional comma-separated season filter. Env: FOOTBALL_DATA_SEASONS.",
    )
    parser.add_argument(
        "--project-id",
        default=project_default,
        help="Optional Google Cloud project ID. Env: FOOTBALL_DATA_PROJECT_ID, GOOGLE_CLOUD_PROJECT, GCP_PROJECT.",
    )
    parser.add_argument(
        "--credentials-file",
        default=credentials_default,
        help=(
            "Optional service account JSON key path. Env: FOOTBALL_DATA_CREDENTIALS_FILE "
            "or GOOGLE_APPLICATION_CREDENTIALS."
        ),
    )
    parser.add_argument(
        "--object-name",
        default=object_name_default,
        help="Target file name inside each partition. Env: FOOTBALL_DATA_OBJECT_NAME.",
    )
    parser.add_argument(
        "--duckdb-path",
        default=duckdb_path_default,
        help="Optional DuckDB database path for runtime updates. Env: FOOTBALL_DATA_DUCKDB_PATH.",
    )
    parser.add_argument(
        "--duckdb-table",
        default=duckdb_table_default,
        help="DuckDB target table. Env: FOOTBALL_DATA_DUCKDB_TABLE.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=lookback_days_default,
        help=f"Refresh partitions with rows in the last N days. Defaults to {DEFAULT_LOOKBACK_DAYS}.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=workers_default,
        help=f"Maximum parallel workers for discovery and refresh. Defaults to {DEFAULT_WORKERS}.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=dry_run_default,
        help="Print affected partitions without uploading or writing DuckDB.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=timeout_default,
        help=f"HTTP timeout in seconds. Defaults to {DEFAULT_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--log-level",
        default=log_level_default,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity. Defaults to INFO.",
    )

    args = parser.parse_args()
    if not args.dry_run and not args.bucket and not args.duckdb_path:
        parser.error("--bucket or --duckdb-path is required unless --dry-run is used.")

    if args.lookback_days < 1:
        parser.error("--lookback-days must be at least 1.")
    if args.workers < 1:
        parser.error("--workers must be at least 1.")

    return args


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s %(message)s",
    )

    country_filters = {normalize_lookup_key(value) for value in parse_csv_list(args.countries)}
    requested_seasons = {normalize_season_value(value) for value in parse_csv_list(args.seasons)}
    session = build_session()

    client = None
    bucket = None
    duckdb_connection = build_duckdb_connection(args.duckdb_path)
    duckdb_lock = threading.Lock() if duckdb_connection is not None else None

    if requested_seasons:
        datasets = discover_datasets(
            session=session,
            timeout=args.timeout,
            country_filters=country_filters,
            season_filters=requested_seasons,
        )
        candidate_datasets = datasets
    else:
        candidate_datasets = discover_latest_datasets(
            session=session,
            timeout=args.timeout,
            country_filters=country_filters,
            workers=args.workers,
        )

    if not candidate_datasets:
        LOGGER.warning("No datasets matched the supplied filters.")
        session.close()
        if duckdb_connection is not None:
            duckdb_connection.close()
        return 0

    end_date = date.today()
    start_date = end_date - timedelta(days=args.lookback_days - 1)
    LOGGER.info(
        "Evaluating %s candidate datasets for matches between %s and %s.",
        len(candidate_datasets),
        start_date.isoformat(),
        end_date.isoformat(),
    )
    LOGGER.info("Using up to %s parallel workers for refresh.", args.workers)

    refreshed = 0
    failures: list[str] = []

    def refresh_candidate(dataset: LeagueDataset) -> tuple[int, str | None]:
        worker_session = get_worker_session()
        worker_client, worker_bucket = (
            get_worker_storage_target(args.project_id, args.credentials_file, args.bucket)
            if args.bucket and not args.dry_run
            else (client, bucket)
        )
        csv_content = fetch_bytes(worker_session, dataset.csv_url, args.timeout)
        table = parse_table_from_csv_bytes(csv_content)
        recent_seasons = recent_seasons_for_dataset(dataset, table, start_date, end_date)

        if not recent_seasons:
            LOGGER.debug("No recent rows found for %s", dataset.csv_url)
            return 0, None

        LOGGER.info(
            "Refreshing %s / %s for seasons: %s",
            dataset.country,
            dataset.league or dataset.country,
            ", ".join(sorted(recent_seasons)),
        )
        refreshed_count = process_dataset(
            session=worker_session,
            bucket=worker_bucket,
            dataset=dataset,
            prefix=args.bucket_prefix,
            object_name=args.object_name,
            timeout=args.timeout,
            dry_run=args.dry_run,
            skip_existing=False,
            client=worker_client,
            season_filters=recent_seasons,
            duckdb_connection=duckdb_connection,
            duckdb_table=args.duckdb_table,
            csv_content=csv_content,
            duckdb_lock=duckdb_lock,
        )
        return refreshed_count, None

    try:
        max_workers = min(args.workers, len(candidate_datasets))
        if max_workers <= 1:
            for dataset in candidate_datasets:
                try:
                    refreshed_count, _ = refresh_candidate(dataset)
                    refreshed += refreshed_count
                except Exception as exc:  # pragma: no cover - defensive runtime logging
                    failures.append(f"{dataset.country} | {dataset.league} | {dataset.csv_url} | {exc}")
                    LOGGER.error(
                        "FAILED RECENT REFRESH %s / %s: %s",
                        dataset.country,
                        dataset.league,
                        exc,
                    )
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_dataset = {
                    executor.submit(refresh_candidate, dataset): dataset
                    for dataset in candidate_datasets
                }
                for future in concurrent.futures.as_completed(future_to_dataset):
                    dataset = future_to_dataset[future]
                    try:
                        refreshed_count, _ = future.result()
                        refreshed += refreshed_count
                    except Exception as exc:  # pragma: no cover - defensive runtime logging
                        failures.append(f"{dataset.country} | {dataset.league} | {dataset.csv_url} | {exc}")
                        LOGGER.error(
                            "FAILED RECENT REFRESH %s / %s: %s",
                            dataset.country,
                            dataset.league,
                            exc,
                        )
    finally:
        session.close()
        if duckdb_connection is not None:
            duckdb_connection.close()

    LOGGER.info("Refreshed %s recent partitions.", refreshed)
    if failures:
        LOGGER.error("Encountered %s failures.", len(failures))
        for failure in failures:
            LOGGER.error("%s", failure)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
