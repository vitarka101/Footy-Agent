from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from google.cloud import storage
from pydantic import BaseModel

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional during install/bootstrap
    load_dotenv = None

try:
    from litellm import completion
except ImportError:  # pragma: no cover - app can still serve deterministic answers
    completion = None

from football_ui_service import (
    DEFAULT_DUCKDB_PATH,
    chat_response,
    dashboard_payload,
    ensure_duckdb_file,
    parse_gcs_uri,
    standings_payload,
)

if load_dotenv is not None:
    load_dotenv(override=True)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "ui"
INDEX_HTML_PATH = BASE_DIR / "index.html"
STANDINGS_HTML_PATH = BASE_DIR / "standings.html"

LOGGER = logging.getLogger("footy_agent")

MODEL = os.getenv("MODEL", "vertex_ai/gemini-2.5-flash-lite")
LITELLM_API_BASE = os.getenv("LITELLM_API_BASE", "").strip()
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "").strip()
DUCKDB_PATH = os.getenv("DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
DUCKDB_GCS_URI = os.getenv("DUCKDB_GCS_URI", "").strip()
MAX_MESSAGE_CHARS = int(os.getenv("MAX_MESSAGE_CHARS", "4000"))
MODEL_TIMEOUT_SECONDS = float(os.getenv("MODEL_TIMEOUT_SECONDS", "20"))
REFRESH_BUCKET = os.getenv("FOOTBALL_DATA_BUCKET", "footy-agent").strip()
REFRESH_PROJECT_ID = (
    os.getenv("FOOTBALL_DATA_PROJECT_ID")
    or os.getenv("GOOGLE_CLOUD_PROJECT")
    or os.getenv("GCP_PROJECT")
    or "agentic-ai-ak5486"
).strip()
REFRESH_BUCKET_PREFIX = os.getenv("FOOTBALL_DATA_BUCKET_PREFIX", "").strip()
REFRESH_WORKERS = int(os.getenv("FOOTBALL_DATA_WORKERS", "8"))
REFRESH_LOOKBACK_DAYS = int(os.getenv("FOOTBALL_DATA_LOOKBACK_DAYS", "2"))
REFRESH_TIMEOUT_SECONDS = int(os.getenv("REFRESH_TIMEOUT_SECONDS", "1200"))
REFRESH_LOCK = threading.Lock()

SYSTEM_PROMPT = """\
You are Footy Agent, a football analytics assistant.

Rules:
- Use only the analytical context and tool outputs provided to you.
- Mention the EDA steps that were run before the conclusion.
- Be concise, clear, and evidence-led.
- Do not invent rows, seasons, teams, or statistics that are not in the tool output.
- If the data slice is limited, say so directly.
- Keep the final answer under 1800 characters.
"""

print(f"[STARTUP] MODEL={MODEL}")
print(f"[STARTUP] DUCKDB_PATH={DUCKDB_PATH}")
print(f"[STARTUP] DUCKDB_GCS_URI={'SET' if DUCKDB_GCS_URI else 'NOT SET'}")

app = FastAPI(title="Footy Agent", version="2.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class ChatRequest(BaseModel):
    message: str
    duckdb_path: str = DUCKDB_PATH


class ChatResponse(BaseModel):
    answer: str
    tool_calls: list[dict]
    highlights: list[dict]
    table: dict | None = None
    charts: list[dict] = []
    hypothesis: dict | None = None
    sources: list[dict] = []
    suggested_prompts: list[str]
    model: str
    provider: str
    fallback_used: bool
    data_mode: str | None = None
    out_of_context: bool = False


class RefreshRequest(BaseModel):
    lookback_days: int | None = None


class RefreshResponse(BaseModel):
    status: str
    detail: str
    output_tail: list[str]


def build_refresh_command(lookback_days: int | None) -> list[str]:
    command = [
        sys.executable,
        "football_data_to_gcs.py",
        "--duckdb-path",
        DUCKDB_PATH,
        "--workers",
        str(REFRESH_WORKERS),
        "--lookback-days",
        str(lookback_days or REFRESH_LOOKBACK_DAYS),
    ]
    if REFRESH_BUCKET:
        command.extend(["--bucket", REFRESH_BUCKET])
    if REFRESH_PROJECT_ID:
        command.extend(["--project-id", REFRESH_PROJECT_ID])
    if REFRESH_BUCKET_PREFIX:
        command.extend(["--bucket-prefix", REFRESH_BUCKET_PREFIX])
    return command


def upload_duckdb_snapshot() -> None:
    if not DUCKDB_GCS_URI:
        return
    database_path = Path(DUCKDB_PATH)
    if not database_path.exists():
        return

    bucket_name, object_name = parse_gcs_uri(DUCKDB_GCS_URI)
    client = storage.Client(project=REFRESH_PROJECT_ID or None)
    blob = client.bucket(bucket_name).blob(object_name)
    blob.upload_from_filename(str(database_path))


def provider_label(model_name: str) -> str:
    normalized = model_name.casefold()
    if normalized.startswith("vertex_ai/"):
        return "Vertex AI"
    if normalized.startswith("ollama/"):
        return "Ollama"
    if normalized.startswith("gpt-") or normalized.startswith("openai/"):
        return "OpenAI"
    return "LiteLLM"


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def compact_table_context(table: dict | None, max_rows: int = 6) -> str:
    if not table or not table.get("columns") or not table.get("rows"):
        return "No result table returned."

    columns = table["columns"]
    rows = table["rows"][:max_rows]
    lines = [", ".join(str(column) for column in columns)]
    for row in rows:
        lines.append(", ".join(str(cell) for cell in row))
    if len(table["rows"]) > max_rows:
        lines.append(f"... {len(table['rows']) - max_rows} more rows omitted")
    return "\n".join(lines)


def render_llm_context(message: str, analysis_payload: dict) -> str:
    tool_lines = "\n".join(
        f"- {tool['label']}: {tool['summary']}" for tool in analysis_payload.get("tool_calls", [])
    )
    highlight_lines = "\n".join(
        f"- {item['label']}: {item['value']} ({item['caption']})"
        for item in analysis_payload.get("highlights", [])
    )
    deterministic_answer = analysis_payload.get("answer", "")
    table_context = compact_table_context(analysis_payload.get("table"))
    hypothesis = analysis_payload.get("hypothesis") or {}
    hypothesis_text = ""
    if hypothesis:
        evidence_lines = "\n".join(f"- {item}" for item in hypothesis.get("evidence", []))
        hypothesis_text = (
            f"\nHypothesis:\n{hypothesis.get('title', '')}\n"
            f"{hypothesis.get('statement', '')}\n"
            f"{evidence_lines or '- No evidence bullets returned.'}\n"
        )
    source_lines = "\n".join(
        f"- {source['title']}: {source.get('snippet', '')}" for source in analysis_payload.get("sources", [])[:4]
    )

    return f"""\
User question:
{message}

EDA steps already executed:
{tool_lines or "- No explicit tool calls were returned."}

Deterministic analysis summary:
{deterministic_answer or "No deterministic summary available."}

Key highlights:
{highlight_lines or "- No highlights were returned."}

Result table:
{table_context}

Sources:
{source_lines or "- No source list returned."}
{hypothesis_text}
"""


def generate_model_answer(message: str, analysis_payload: dict) -> tuple[str, bool]:
    if completion is None:
        return analysis_payload["answer"], True

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": render_llm_context(message, analysis_payload)},
    ]
    completion_kwargs = {
        "model": MODEL,
        "messages": messages,
        "temperature": 0.2,
        "timeout": MODEL_TIMEOUT_SECONDS,
    }
    if LITELLM_API_BASE:
        completion_kwargs["api_base"] = LITELLM_API_BASE
    if LITELLM_API_KEY:
        completion_kwargs["api_key"] = LITELLM_API_KEY

    try:
        response = completion(**completion_kwargs)
        content = (response.choices[0].message.content or "").strip()
        if content:
            return content, False
    except Exception as exc:  # pragma: no cover - model/provider runtime failure
        LOGGER.warning("Model generation failed, using deterministic fallback: %s", exc)

    return analysis_payload["answer"], True


def enriched_dashboard_payload(duckdb_path: str) -> dict:
    payload = dashboard_payload(duckdb_path)
    payload["runtime"] = {
        "model": MODEL,
        "provider": provider_label(MODEL),
        "duckdb_path": duckdb_path,
        "duckdb_gcs_backed": bool(DUCKDB_GCS_URI),
    }
    return payload


def build_chat_payload(message: str, duckdb_path: str) -> dict:
    analysis_payload = chat_response(message, duckdb_path)
    answer, fallback_used = generate_model_answer(message, analysis_payload)
    analysis_payload["answer"] = answer
    analysis_payload["model"] = MODEL
    analysis_payload["provider"] = provider_label(MODEL)
    analysis_payload["fallback_used"] = fallback_used
    return analysis_payload


@app.on_event("startup")
def preload_duckdb() -> None:
    should_sync = env_flag("SYNC_DUCKDB_FROM_GCS", default=False)
    if not Path(DUCKDB_PATH).exists() and not DUCKDB_GCS_URI:
        return

    try:
        ensure_duckdb_file(DUCKDB_PATH, duckdb_gcs_uri=DUCKDB_GCS_URI, force_download=should_sync)
    except Exception as exc:  # pragma: no cover - startup diagnostics only
        LOGGER.warning("DuckDB preload skipped: %s", exc)


@app.get("/")
def read_index() -> FileResponse:
    return FileResponse(INDEX_HTML_PATH)


@app.get("/standings-page")
def read_standings_page() -> FileResponse:
    return FileResponse(STANDINGS_HTML_PATH)


@app.get("/health")
@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
@app.get("/api/dashboard")
def get_dashboard(duckdb_path: str = DUCKDB_PATH) -> dict:
    try:
        return enriched_dashboard_payload(duckdb_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime API protection
        raise HTTPException(status_code=500, detail=f"Failed to build dashboard: {exc}") from exc


@app.get("/standings")
@app.get("/api/standings")
def get_standings(
    country: str | None = None,
    league: str | None = None,
    duckdb_path: str = DUCKDB_PATH,
) -> dict:
    try:
        return standings_payload(duckdb_path, country=country, league=league)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime API protection
        raise HTTPException(status_code=500, detail=f"Failed to build standings: {exc}") from exc


@app.post("/chat", response_model=ChatResponse)
@app.post("/api/chat", response_model=ChatResponse)
def post_chat(request: ChatRequest) -> dict:
    message = request.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")
    if len(message) > MAX_MESSAGE_CHARS:
        raise HTTPException(
            status_code=400,
            detail=f"Message exceeds {MAX_MESSAGE_CHARS} characters.",
        )

    try:
        return build_chat_payload(message, request.duckdb_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime API protection
        raise HTTPException(status_code=500, detail=f"Failed to answer chat request: {exc}") from exc


@app.post("/refresh", response_model=RefreshResponse)
@app.post("/api/refresh", response_model=RefreshResponse)
def refresh_data(request: RefreshRequest) -> dict:
    if not REFRESH_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A refresh job is already running.")

    try:
        command = build_refresh_command(request.lookback_days)
        completed = subprocess.run(
            command,
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=REFRESH_TIMEOUT_SECONDS,
            env=os.environ.copy(),
        )
        combined_output = "\n".join(
            part.strip() for part in (completed.stdout, completed.stderr) if part.strip()
        )
        output_tail = [line for line in combined_output.splitlines() if line.strip()][-12:]

        if completed.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Refresh job failed.",
                    "output_tail": output_tail,
                },
            )

        upload_duckdb_snapshot()
        return {
            "status": "ok",
            "detail": "Recent football data refreshed in GCS and DuckDB.",
            "output_tail": output_tail,
        }
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(
            status_code=504,
            detail=f"Refresh job exceeded {REFRESH_TIMEOUT_SECONDS} seconds.",
        ) from exc
    finally:
        REFRESH_LOCK.release()


if __name__ == "__main__":  # pragma: no cover - local entrypoint
    uvicorn.run(app, host="127.0.0.1", port=8000)
