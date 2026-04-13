# Footy-Agent

This repo now includes a Python script that scrapes league CSV files from `football-data.co.uk`, converts them to Parquet, and uploads them to Google Cloud Storage using this object layout:

`country=<country> / league=<league> / season=<season> / league_data.parquet`

Example object path:

`country=england / league=premier_league / season=2025-2026 / league_data.parquet`

## Bucket Structure

The bucket hierarchy is:

`country=<country> / league=<league> / season=<season> /`

This uses Hive-style partition folders, which are well-suited for BigQuery and other query engines that recognize partition-style paths.

For example, England league folders would look like:

```text
country=england/
  league=premier_league/
    season=2025-2026/
      league_data.parquet
  league=championship/
    season=2025-2026/
      league_data.parquet
  league=league_1/
    season=2025-2026/
      league_data.parquet
  league=league_2/
    season=2025-2026/
      league_data.parquet
  league=conference/
    season=2025-2026/
      league_data.parquet
```

Other countries will have their own partition values based on the names used on `football-data.co.uk`, normalized to lowercase with underscores.

## Files

- `football_data_to_gcs.py`: Scrapes the Football-Data country index, discovers league CSV URLs by season, converts them to Parquet, and uploads them to a GCS bucket.
- `requirements.txt`: Python dependencies for the downloader.
- `COLUMN_DICTIONARY.md`: Reference for normalized column names and their meanings.
- `Dockerfile`: Container image for Docker and Google Cloud deployment.
- `.dockerignore`: Keeps local Python and Git artifacts out of the image.

## Assumption

This implementation assumes "Google Cloud" means Google Cloud Storage. The bucket will not create real folders; it will create object paths that behave like folders in the GCS console.

## Setup

Install dependencies directly:

```bash
python3 -m pip install -r requirements.txt
```

3. Authenticate to Google Cloud with one of these options:

Option A: Application Default Credentials

```bash
gcloud auth application-default login
```

Option B: service account JSON key

Use the `--credentials-file` flag when you run the script.

## Usage

Upload every discovered country, league, and season into a bucket:

```bash
python3 football_data_to_gcs.py --bucket your-gcs-bucket
```

Upload only selected countries and seasons:

```bash
python3 football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --countries England,Spain,USA \
  --seasons 2025-2026,2024-2025
```

Write into a prefix inside the bucket:

```bash
python3 football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --bucket-prefix raw/football-data
```

Dry run without downloading or uploading files:

```bash
python3 football_data_to_gcs.py \
  --dry-run \
  --countries England \
  --seasons 2025-2026
```

Use a service account key directly:

```bash
python3 football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --credentials-file /path/to/service-account.json \
  --project-id your-gcp-project
```

## Environment Variables

The script is container-friendly and can be configured through environment variables instead of CLI flags. CLI flags override environment variables when both are set.

- `FOOTBALL_DATA_BUCKET`
- `FOOTBALL_DATA_BUCKET_PREFIX`
- `FOOTBALL_DATA_COUNTRIES`
- `FOOTBALL_DATA_SEASONS`
- `FOOTBALL_DATA_PROJECT_ID`
- `FOOTBALL_DATA_CREDENTIALS_FILE`
- `FOOTBALL_DATA_OBJECT_NAME`
- `FOOTBALL_DATA_SKIP_EXISTING`
- `FOOTBALL_DATA_DRY_RUN`
- `FOOTBALL_DATA_TIMEOUT`
- `FOOTBALL_DATA_LOG_LEVEL`

It also respects standard Google Cloud auth environment variables:

- `GOOGLE_APPLICATION_CREDENTIALS`
- `GOOGLE_CLOUD_PROJECT`
- `GCP_PROJECT`

Example:

```bash
export FOOTBALL_DATA_BUCKET=your-gcs-bucket
export FOOTBALL_DATA_BUCKET_PREFIX=raw/football-data
export FOOTBALL_DATA_COUNTRIES=England,Spain,USA
python3 football_data_to_gcs.py
```

## Docker

Build the image:

```bash
docker build -t football-data-gcs .
```

Run it with environment variables:

```bash
docker run --rm \
  -e FOOTBALL_DATA_BUCKET=your-gcs-bucket \
  -e FOOTBALL_DATA_BUCKET_PREFIX=raw/football-data \
  -e FOOTBALL_DATA_COUNTRIES=England,Spain \
  -e FOOTBALL_DATA_SEASONS=2025-2026 \
  football-data-gcs
```

Run it with a mounted service account key locally:

```bash
docker run --rm \
  -e FOOTBALL_DATA_BUCKET=your-gcs-bucket \
  -e GOOGLE_APPLICATION_CREDENTIALS=/var/secrets/google/service-account.json \
  -v /path/to/service-account.json:/var/secrets/google/service-account.json:ro \
  football-data-gcs
```

Run a dry run in Docker:

```bash
docker run --rm \
  -e FOOTBALL_DATA_DRY_RUN=true \
  -e FOOTBALL_DATA_COUNTRIES=England \
  -e FOOTBALL_DATA_SEASONS=2025-2026 \
  football-data-gcs
```

## Google Cloud Deployment

This image is compatible with Google Cloud batch-style runtimes. `Cloud Run Jobs` is the natural fit because this script runs to completion and exits.

For Google Cloud deployment:

- Push the built image to Artifact Registry or another container registry Google Cloud can read.
- Attach a Google Cloud service account to the runtime instead of baking credentials into the image.
- Grant that service account permission to write to the target bucket, typically Storage object creation or admin permissions depending on your workflow.
- Set the `FOOTBALL_DATA_*` environment variables on the job.
- Do not rely on local key files in production unless you have a specific reason. Application Default Credentials from the attached service account are cleaner and safer.

## Useful Flags

- `--countries`: Comma-separated country filter.
- `--seasons`: Comma-separated season filter in `YYYY-YYYY` format.
- `--bucket-prefix`: Optional prefix inside the bucket.
- `--skip-existing`: Skip objects that already exist.
- `--object-name`: Change the file name from `league_data.parquet` if needed.
- `--dry-run`: Print target object paths only.
- `--timeout`: Override the HTTP timeout in seconds.
- `--log-level`: One of `DEBUG`, `INFO`, `WARNING`, `ERROR`.

## Notes

- The scraper starts from `https://www.football-data.co.uk/data.php`, then follows each country page and discovers season-specific CSV links.
- Each source CSV is converted in memory to Parquet before upload.
- Historical source files with ragged rows are normalized before conversion so older seasons with blank rows or inconsistent trailing commas still load correctly.
- Column names are normalized to lowercase snake_case for BigQuery compatibility. Special characters are converted to readable tokens where possible, for example `B365>2.5` becomes `b365_gt_2_5`.
- The stored Parquet schema is deterministic: identifier columns such as `country`, `league`, `season`, `date`, `hometeam`, and `awayteam` are written as strings; count-style match stats such as goals, shots, corners, fouls, and cards are written as integers; odds and handicap lines are written as `FLOAT64`-compatible numeric columns.
- Every Parquet file now includes `country`, `league`, `season`, `source_url`, and `source_type` as actual table columns in addition to being represented in the GCS object path.
- Season folder values are normalized from `YYYY/YYYY` on the site to `YYYY-YYYY` in GCS because `/` is a path separator.
- Country and league partition values are normalized for storage paths by converting them to lowercase, replacing spaces with underscores, and cleaning unsafe path characters.
- The GCS layout uses Hive-style partition folders: `country=<country>/league=<league>/season=<season>/...`.
- Some Football-Data country pages, especially in the extra leagues section, expose a single combined CSV instead of season-specific league links. The script now partitions those files by the source `League` and `Season` columns so they still upload into the same partitioned layout.
- The same script works in local Python, Docker, and Google Cloud because it uses environment variables plus standard Google Application Default Credentials.
