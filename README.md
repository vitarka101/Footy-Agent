# Footy-Agent

Footy Agent is now structured like a deployable single-app project:

- `scripts/app.py` is the FastAPI entrypoint
- `index.html` is the root frontend entrypoint
- `pyproject.toml` supports `uv`-based local/dev workflow
- `cloudbuild.yaml` deploys the app to Cloud Run
- the chat layer can run on `Vertex AI` in Google Cloud and `Ollama` locally

The repo still includes the two football-data ingestion scripts:

- `historical_football_data_to_gcs.py`: full historical backfill
- `football_data_to_gcs.py`: recent refresh script for cronjobs or on-demand backend execution

Both scripts convert source CSV files to Parquet and can write to Google Cloud Storage using this object layout:

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

- `scripts/app.py`: Main FastAPI application with model-backed chat, DuckDB bootstrap, and zip-style routes.
- `index.html`: Root landing page for the football analyst UI.
- `historical_football_data_to_gcs.py`: Full-history backfill into GCS and optional DuckDB.
- `football_data_to_gcs.py`: Incremental refresh script for the last 1-2 days of activity, suitable for cronjobs or backend-triggered refreshes.
- `football_eda.py`: Python EDA toolset for the England four-tier dataset in DuckDB, modeled on the reference `data.qmd` workflow.
- `scripts/football_ui_service.py`: Backend analytics helpers used by the web UI chat and dashboard. The chat layer resolves country and league names from the user question and can analyze the full warehouse.
- `ui/`: CSS and JS assets for the landing page.
- `pyproject.toml`: `uv` project definition for local and Docker workflows.
- `requirements.txt`: Pip-compatible dependency list.
- `COLUMN_DICTIONARY.md`: Reference for normalized column names and their meanings.
- `Dockerfile`: Container image for local Docker and Cloud Run deployment.
- `cloudbuild.yaml`: Cloud Build pipeline for build, push, and Cloud Run deployment.
- `.gcloudignore`: Files excluded from Cloud Build upload.
- `evals/`: Deterministic checks for routing and runtime helpers.
- `.dockerignore`: Keeps local Python and Git artifacts out of the image.

## Assumption

This implementation assumes "Google Cloud" means Google Cloud Storage. The bucket will not create real folders; it will create object paths that behave like folders in the GCS console.

## Why DuckDB

Because the dataset is well under 5 GB, the project also supports DuckDB as a practical runtime analytics layer.

- GCS remains the canonical storage layer for partitioned Parquet files.
- DuckDB provides a lightweight local analytical database that can be queried directly by a backend or UI without sending every request to BigQuery.
- BigQuery can still be used for cloud analytics or reporting, but DuckDB is a good fit for low-latency runtime queries on a dataset of this size.

## Assignment Mapping

### Step 1: Collect

This project satisfies Step 1 through real runtime web collection, not through hard-coded data in the prompt.

Implemented collection method:

- `Web search / crawling`: the ingestion scripts fetch `https://www.football-data.co.uk/data.php`, follow country pages, discover season CSV links, and download the source files at runtime.

Collect tool calls implemented in code:

- `python3 historical_football_data_to_gcs.py --bucket ...`
- `python3 football_data_to_gcs.py --bucket ...`
- `python3 historical_football_data_to_gcs.py --duckdb-path football_data.duckdb`
- `python3 football_data_to_gcs.py --duckdb-path football_data.duckdb`

What those collect tool calls do:

- retrieve non-trivial real-world football match data from `football-data.co.uk`
- parse and normalize the downloaded CSV files
- store the data in GCS Parquet partitions and/or DuckDB
- support both full historical backfill and recent incremental refresh

Why this satisfies the requirement:

- the source is real and external
- the scripts actively fetch data at runtime
- the dataset is much larger and broader than what should be loaded directly into model context
- the data are not hand-curated or embedded in the prompt

### Step 2: Explore and Analyze (EDA)

This project satisfies Step 2 through explicit analytical tool calls over the collected DuckDB data. The agent does not need to jump directly to an answer; it can first inspect, segment, aggregate, and analyze the stored match data.

Implemented EDA method categories:

- `Code execution`: Python executes pandas and DuckDB-based analysis over collected data
- `Statistical aggregation`: season and league level means, medians, and rates
- `Filtering and grouping`: result shares, league segmentation, and grouped comparisons
- `Correlation analysis`: exploratory relationships across numeric match metrics

EDA tool calls implemented in code:

- `python3 football_eda.py overview --duckdb-path football_data.duckdb --output-dir artifacts/eda`
- `python3 football_eda.py aggregate --duckdb-path football_data.duckdb --output-dir artifacts/eda`
- `python3 football_eda.py segment --duckdb-path football_data.duckdb --output-dir artifacts/eda`
- `python3 football_eda.py correlation --duckdb-path football_data.duckdb --output-dir artifacts/eda`
- `python3 football_eda.py missingness --duckdb-path football_data.duckdb --output-dir artifacts/eda`
- `python3 football_eda.py outliers --duckdb-path football_data.duckdb --output-dir artifacts/eda`

How these map to the assignment:

- `overview`: dataset profile and coverage inspection before analysis
- `aggregate`: qualifying statistical aggregation tool call
- `segment`: qualifying filtering and grouping tool call
- `correlation`: qualifying exploratory numeric analysis tool call
- `missingness`: data-quality EDA over collected data
- `outliers`: distribution and outlier EDA over collected data

Minimum Step 2 compliant EDA flow:

1. run `aggregate`
2. run `segment` or `correlation`
3. inspect the outputs
4. then reason about the findings

This structure makes the EDA phase explicit and tool-driven rather than a direct answer from the model.

### Framework, Tool Calling, and Multi-Agent Design

- `Agent framework`: the app now uses `Agno` agents for EDA planning and specialist orchestration, with LiteLLM-backed models underneath.
- `Real tool calling`: the chat router in `scripts/app.py` uses LiteLLM `tools` plus `tool_choice="auto"` so the model can invoke `run_runtime_query` and `run_analysis_pipeline` as actual function calls.
- `Distinct specialist agents`: the warehouse EDA path creates role-specific agents for trend, comparison, correlation, quality, and distribution analysis. Each specialist has separate instructions, calls its specialist tool, and runs concurrently.
- `Grounded hypothesis objects`: the final hypothesis now carries machine-readable `evidence_objects` tied back to profile, trend, correlation, and quality outputs in addition to human-readable evidence bullets.

### Betting Room Add-On

The repo also includes a completely separate betting page at `/betting-room-page`, built from the public `luisgomezordoniez/poisson-football` project and adapted into the current FastAPI app as an additive feature.

What was ported from that repo:

- `Poisson-family match models`: [scripts/betting_room_service.py](scripts/betting_room_service.py) ports the original repo logic for `estimateParams`, `predictMatch`, `simulateLeague`, `computeTable`, `poissonGoodnessOfFitTest`, `independenceTest`, and `dispersionTest` into Python equivalents such as `estimate_params`, `predict_match_tool`, `simulate_league_tool`, `compute_table`, `poisson_goodness_of_fit_test`, `independence_test`, and `dispersion_test`.
- `Runtime collection from football-data.co.uk`: [scripts/betting_room_service.py](scripts/betting_room_service.py) implements `collect_match_data_tool`, `fetch_external_season_matches`, and `fetch_runtime_csv`, which fetch season CSVs at runtime and cache artifacts under `artifacts/betting_room/`.
- `Standalone frontend`: [betting_room.html](betting_room.html), [ui/betting_room.js](ui/betting_room.js), and [ui/betting_room.css](ui/betting_room.css) render a separate betting-room experience with probability bars, exact-score matrix, EDA tests, league-table comparison, and backend tool trace.
- `Standalone API`: [scripts/app.py](scripts/app.py) adds `/betting-room-page`, `/betting/options`, and `/betting/analyze` as separate additive routes so the existing chat and standings flow stays intact.

How the betting room maps to the homework steps:

- `Step 1: Collect`: `collect_match_data_tool` retrieves real league-season data from `football-data.co.uk` at runtime and stores persistent JSON artifacts in `artifacts/betting_room/data/`.
- `Step 2: Explore and Analyze`: `run_betting_analysis` fans out specialist tools in parallel for probability estimation, assumption testing, and league simulation before aggregating the outputs.
- `Step 3: Hypothesize`: `build_hypothesis_tool` converts model probabilities, statistical tests, and bookmaker edge comparison into a concrete betting thesis with evidence and caveats.

Specific concepts implemented in the betting room:

- `Frontend`: `betting_room.html` + `ui/betting_room.js`
- `Agent framework`: the main app framework remains `Agno`; the betting room is an additive analytics surface inside the same deployed app
- `Tool calling`: `run_betting_analysis` executes real backend tools including `collect_match_data_tool`, `predict_match_tool`, `run_assumption_tests_tool`, `simulate_league_tool`, `evaluate_value_bet_tool`, and `build_hypothesis_tool`
- `Non-trivial dataset`: runtime football-data CSV collection over multiple seasons from `football-data.co.uk`
- `Multi-agent pattern`: fan-out specialist pattern inside `run_betting_analysis` using parallel probability, assumption-test, and league-simulation specialists
- `Artifacts`: betting-room cache files and markdown reports are written under `artifacts/betting_room/`
- `Structured output`: the betting-room API returns structured JSON payloads for the page, including tables, tool traces, score matrices, and hypothesis blocks
- `Second retrieval method`: the betting room uses runtime web CSV retrieval first and DuckDB as a backup retrieval path
- `Data visualization`: probability bars, score matrix, and predicted-vs-actual table comparison
- `Parallel execution`: specialist tools run concurrently inside `run_betting_analysis`

## Setup

Preferred local workflow with `uv`:

```bash
uv sync
```

Alternative pip workflow:

```bash
python3 -m pip install -r requirements.txt
```

Authenticate to Google Cloud with one of these options:

Option A: Application Default Credentials

```bash
gcloud auth application-default login
```

Option B: service account JSON key

Use the `--credentials-file` flag when you run the script.

## App Runtime Configuration

Create a local `.env` file if you want model/runtime configuration without exporting env vars manually.

Core app env vars:

- `MODEL`
- `LITELLM_API_BASE`
- `LITELLM_API_KEY`
- `DUCKDB_PATH`
- `DUCKDB_GCS_URI`
- `SYNC_DUCKDB_FROM_GCS`
- `MAX_MESSAGE_CHARS`
- `MODEL_TIMEOUT_SECONDS`

Recommended Vertex AI config for local and Cloud Run:

```env
MODEL=vertex_ai/gemini-2.5-flash-lite
DUCKDB_PATH=football_data.duckdb
```

Recommended Ollama config for local use:

```env
MODEL=ollama/llama3.1
LITELLM_API_BASE=http://localhost:11434
DUCKDB_PATH=football_data.duckdb
```

If you deploy without bundling a local DuckDB file, set:

```env
DUCKDB_GCS_URI=gs://your-bucket/path/to/football_data.duckdb
DUCKDB_PATH=/tmp/football_data.duckdb
SYNC_DUCKDB_FROM_GCS=true
```

## Usage

### 1. Full Historical Backfill

Upload every discovered country, league, and season into a bucket:

```bash
python3 historical_football_data_to_gcs.py --bucket your-gcs-bucket
```

Upload only selected countries and seasons:

```bash
python3 historical_football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --countries England,Spain,USA \
  --seasons 2025-2026,2024-2025
```

Write into a prefix inside the bucket:

```bash
python3 historical_football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --bucket-prefix raw/football-data
```

Dry run without downloading or uploading files:

```bash
python3 historical_football_data_to_gcs.py \
  --dry-run \
  --countries England \
  --seasons 2025-2026
```

Use a service account key directly:

```bash
python3 historical_football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --credentials-file /path/to/service-account.json \
  --project-id your-gcp-project
```

Build a runtime DuckDB database during the backfill:

```bash
python3 historical_football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --project-id your-gcp-project \
  --duckdb-path football_data.duckdb
```

### 2. Incremental Recent Refresh

Refresh only partitions with match rows from the last 2 days:

```bash
python3 football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --project-id your-gcp-project \
  --duckdb-path football_data.duckdb
```

Refresh only the last day:

```bash
python3 football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --project-id your-gcp-project \
  --duckdb-path football_data.duckdb \
  --lookback-days 1
```

Increase parallelism for faster country discovery and refresh:

```bash
python3 football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --project-id your-gcp-project \
  --duckdb-path football_data.duckdb \
  --workers 8
```

Run the incremental refresh for selected countries:

```bash
python3 football_data_to_gcs.py \
  --bucket your-gcs-bucket \
  --project-id your-gcp-project \
  --countries England,Spain \
  --duckdb-path football_data.duckdb
```

### 3. Python EDA Tools

The `football_eda.py` script is organized as assignment-style EDA tool calls over collected DuckDB data. It focuses on the England four-tier dataset (`E0`-`E3`) because that is the cleanest comparable slice for long-range analysis.

Tool-call mapping:

- `overview`: dataset profile and coverage snapshot
- `aggregate`: statistical aggregation over league/season groups
- `segment`: filtering and grouping analysis for league-level comparisons and result composition
- `correlation`: correlation analysis across match metrics
- `missingness`: data-quality completeness analysis
- `outliers`: distribution and outlier analysis

Run the full EDA bundle:

```bash
python3 football_eda.py all \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda
```

Run only the dataset overview tool:

```bash
python3 football_eda.py overview \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda
```

Run the statistical aggregation tool:

```bash
python3 football_eda.py aggregate \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda
```

Run the filtering and grouping tool:

```bash
python3 football_eda.py segment \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda
```

Run the correlation tool:

```bash
python3 football_eda.py correlation \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda
```

Run the missing-values tool:

```bash
python3 football_eda.py missingness \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda
```

Run only the outlier tool and distribution checks:

```bash
python3 football_eda.py outliers \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda \
  --distribution-columns fthg,hr,ar
```

Limit the EDA to selected seasons:

```bash
python3 football_eda.py all \
  --duckdb-path football_data.duckdb \
  --output-dir artifacts/eda_recent \
  --seasons 2023-2024,2024-2025,2025-2026
```

### 4. Football Analyst UI

The web app is chat-first. The landing page keeps the assistant in the center and uses compact side panels for dataset coverage, league snapshots, active EDA modules, and analyst workflow context.

Run the UI locally:

```bash
uv run uvicorn scripts.app:app --reload
```

Then open:

```text
http://127.0.0.1:8000
```

Implemented UI endpoints:

- `GET /`: serves the landing page
- `GET /health`: simple health check
- `GET /stats`: returns dashboard cards, prompt chips, spotlight stats, runtime metadata, and tool metadata
- `GET /standings`: returns a computed latest-season standings table for the selected country and league
- `POST /refresh`: enqueues a background refresh job and returns `202 Accepted` with a job id
- `GET /refresh/{job_id}`: returns refresh job status, timestamps, and recent log tail
- `POST /chat`: runs the full analyst pipeline for every question:
  1. domain validation
  2. warehouse retrieval from DuckDB if coverage exists
  3. web search + crawling + ranked snippet retrieval if warehouse coverage is missing
  4. EDA over the retrieved evidence with charts
  5. a final evidence-backed hypothesis and answer
- `GET /api/health`, `GET /api/dashboard`, `GET /api/standings`, `POST /api/refresh`, `GET /api/refresh/{job_id}`, and `POST /api/chat`: backward-compatible aliases

Current chat analysis modes:

- home advantage trend analysis
- league comparison analysis
- scoring trend analysis
- correlation analysis
- data-quality analysis
- general dataset overview

The chat app is no longer limited to the England four-tier slice. Questions like `Analyze La Liga`, `Compare Spain leagues`, or `How has home advantage changed in Serie A?` are resolved against the corresponding country and league in DuckDB. The standalone `football_eda.py` script still focuses on England `E0-E3`.

Current question flow:

1. `Domain Validation`: reject non-football questions through explicit out-of-context handling.
2. `Warehouse Retrieval`: resolve the country/league/season from the question and fetch the matching DuckDB slice.
3. `Fallback Retrieval`: if the warehouse does not have the requested football topic, switch to web search and crawling, then rank the crawled text snippets with a lightweight RAG-style retriever.
4. `EDA`: run parallel specialist sub-agents over the retrieved evidence. For warehouse-backed questions, the app launches aggregate, segment, correlation, and quality specialists in parallel, then combines their outputs into charts, tables, and supporting evidence.
5. `Hypothesis`: produce a final analytical claim with explicit supporting evidence bullets instead of only a conversational answer.

The frontend is intentionally lightweight:

- main focus is the chatbot
- supporting panels surface useful analyst context
- chat responses display tool calls, highlight metrics, EDA charts with captions, compact result tables, source cards, and the final hypothesis block

### 5. Deploy to Google Cloud Run

Build and deploy with Cloud Build:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable cloudbuild.googleapis.com run.googleapis.com
gcloud builds submit .
```

The included `cloudbuild.yaml`:

- builds `gcr.io/$PROJECT_ID/footy-agent`
- pushes the image
- deploys Cloud Run service `footy-agent` in `us-central1`
- sets `MODEL=vertex_ai/gemini-2.5-flash-lite`

For Vertex AI on Cloud Run, make sure the service account has `Vertex AI User` (`roles/aiplatform.user`).

## Environment Variables

The ingestion scripts are container-friendly and can be configured through environment variables instead of CLI flags. CLI flags override environment variables when both are set.

- `FOOTBALL_DATA_BUCKET`
- `FOOTBALL_DATA_BUCKET_PREFIX`
- `FOOTBALL_DATA_COUNTRIES`
- `FOOTBALL_DATA_SEASONS`
- `FOOTBALL_DATA_PROJECT_ID`
- `FOOTBALL_DATA_CREDENTIALS_FILE`
- `FOOTBALL_DATA_OBJECT_NAME`
- `FOOTBALL_DATA_DUCKDB_PATH`
- `FOOTBALL_DATA_DUCKDB_TABLE`
- `FOOTBALL_DATA_LOOKBACK_DAYS`
- `FOOTBALL_DATA_WORKERS`
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
export FOOTBALL_DATA_DUCKDB_PATH=football_data.duckdb
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
  -e FOOTBALL_DATA_DUCKDB_PATH=/data/football_data.duckdb \
  -v "$(pwd)/data:/data" \
  football-data-gcs
```

The default container command runs the incremental refresh script. To run the full historical backfill instead:

```bash
docker run --rm \
  -e FOOTBALL_DATA_BUCKET=your-gcs-bucket \
  -e FOOTBALL_DATA_DUCKDB_PATH=/data/football_data.duckdb \
  -v "$(pwd)/data:/data" \
  football-data-gcs \
  historical_football_data_to_gcs.py
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

This image is compatible with Google Cloud batch-style runtimes. `Cloud Run Jobs` is the natural fit because both scripts run to completion and exit.

For Google Cloud deployment:

- Push the built image to Artifact Registry or another container registry Google Cloud can read.
- Attach a Google Cloud service account to the runtime instead of baking credentials into the image.
- Grant that service account permission to write to the target bucket, typically Storage object creation or admin permissions depending on your workflow.
- Set the `FOOTBALL_DATA_*` environment variables on the job.
- For UI-triggered refreshes, call `football_data_to_gcs.py` from the backend when the user clicks refresh.
- Do not rely on local key files in production unless you have a specific reason. Application Default Credentials from the attached service account are cleaner and safer.

## Useful Flags

- `--countries`: Comma-separated country filter.
- `--seasons`: Comma-separated season filter in `YYYY-YYYY` format.
- `--bucket-prefix`: Optional prefix inside the bucket.
- `--duckdb-path`: Optional local DuckDB database file for runtime usage.
- `--duckdb-table`: Target DuckDB table name. Defaults to `matches`.
- `--lookback-days`: Incremental refresh window used by `football_data_to_gcs.py`.
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
- Both scripts can also maintain a local DuckDB database, which is practical here because the full dataset is small enough to use directly during runtime.
- The incremental script checks only the latest season for each league by default, plus the combined extra-league feeds, then refreshes partitions that contain match rows in the last `N` days so GCS and DuckDB stay consistent.
- The incremental script can run country discovery and dataset refresh in parallel with `--workers`. DuckDB writes are coordinated so the local database stays consistent while network fetches and GCS uploads run concurrently.
- The Python EDA tool is designed around explicit exploratory tool calls on collected data: statistical aggregation, filtering/grouping, correlation analysis, plus supplemental data-quality checks such as missingness and outliers.
- Season folder values are normalized from `YYYY/YYYY` on the site to `YYYY-YYYY` in GCS because `/` is a path separator.
- Country and league partition values are normalized for storage paths by converting them to lowercase, replacing spaces with underscores, and cleaning unsafe path characters.
- The GCS layout uses Hive-style partition folders: `country=<country>/league=<league>/season=<season>/...`.
- Some Football-Data country pages, especially in the extra leagues section, expose a single combined CSV instead of season-specific league links. The script now partitions those files by the source `League` and `Season` columns so they still upload into the same partitioned layout.
- The same script works in local Python, Docker, and Google Cloud because it uses environment variables plus standard Google Application Default Credentials.
