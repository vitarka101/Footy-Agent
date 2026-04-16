from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional during install/bootstrap
    load_dotenv = None

try:
    from litellm import completion
except ImportError:  # pragma: no cover - app can still serve deterministic answers
    completion = None

from scripts.football_ui_service import (
    DEFAULT_DUCKDB_PATH,
    chat_response,
    dashboard_payload,
    open_connection,
    resolve_message_with_recent_context,
    standings_payload,
    table_payload,
)
from scripts.betting_room_service import MODEL_NAMES as BETTING_MODELS, options_payload as betting_options_payload, run_betting_analysis

if load_dotenv is not None:
    load_dotenv(override=True)

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
STATIC_DIR = FRONTEND_DIR / "static"
TEMPLATES_DIR = FRONTEND_DIR / "templates"
INDEX_HTML_PATH = TEMPLATES_DIR / "index.html"
STANDINGS_HTML_PATH = TEMPLATES_DIR / "standings.html"
BETTING_ROOM_HTML_PATH = TEMPLATES_DIR / "betting_room.html"

LOGGER = logging.getLogger("footy_agent")

MODEL = os.getenv("MODEL", "vertex_ai/gemini-2.5-flash-lite")
LITELLM_API_BASE = os.getenv("LITELLM_API_BASE", "").strip()
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "").strip()
DUCKDB_PATH = os.getenv("DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
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
REFRESH_JOBS_LOCK = threading.Lock()
REFRESH_JOBS: dict[str, dict] = {}
ACTIVE_REFRESH_JOB_ID: str | None = None

SYSTEM_PROMPT = """\
You are Footy Agent, a football analytics assistant.

Rules:
# - Use only the analytical context and tool outputs provided to you.
- Mention the EDA steps that were run before the conclusion.
- Be concise, clear, and evidence-led.
- Do not invent rows, seasons, teams, or statistics that are not in the tool output.
- If the data slice is limited, say so directly.
- Keep the final answer under 1800 characters.
"""

QUERY_PLANNER_SYSTEM_PROMPT = """\
You are a DuckDB query planner for a football dataset.

You must answer with strict JSON only. No markdown.

Available table:
- matches

Important columns:
- country, league, season
- date, time
- hometeam, awayteam
- fthg, ftag, ftr
- hthg, htag, htr
- hs, "as", hst, ast
- hc, ac, hy, ay, hr, ar
- referee

Rules:
- Only produce read-only SQL using SELECT or WITH.
- Only query the matches table.
- Use try_strptime(date, '%d/%m/%Y') when ordering by match date.
- Quote the away shots column exactly as "as" if used.
- If the question cannot be answered from the matches table alone, set applicable to false.
- Prefer concise aggregate queries.
- If returning detailed rows, limit the result to at most 20 rows inside the SQL.

Return JSON with this exact shape:
{
  "applicable": true,
  "sql": "SELECT ...",
  "title": "short result title",
  "reason": "one sentence"
}
"""

QUERY_SUMMARY_SYSTEM_PROMPT = """\
You are Footy Agent summarizing a DuckDB query result.

Rules:
- Answer the user's question directly from the query result.
- Be concise and factual.
- Do not mention missing tools or lack of context if the table answers the question.
- If the result does not settle the question, say what is missing.
- Keep the answer under 120 words.
"""

TOOL_ROUTER_SYSTEM_PROMPT = """\
You are Footy Agent's tool-using router.

You must choose tools before answering whenever useful.

Available tools:
- run_runtime_query: use for direct fact lookups or SQL-answerable questions against the matches table
- run_analysis_pipeline: use for broader football analysis, EDA, standings, trends, team performance, or external football fallback

Rules:
- Prefer run_runtime_query for narrow lookup-style questions.
- Prefer run_analysis_pipeline for anything exploratory, comparative, team-history based, or if external football info may be needed.
- After receiving tool output, answer briefly and faithfully from the tool result.
"""

print(f"[STARTUP] MODEL={MODEL}")
print(f"[STARTUP] DUCKDB_PATH={DUCKDB_PATH}")

app = FastAPI(title="Footy Agent", version="2.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class ChatRequest(BaseModel):
    message: str
    duckdb_path: str = DUCKDB_PATH
    history: list[dict] = []


class ChatResponse(BaseModel):
    answer: str
    executive_summary: list[str] = []
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
    is_conversational: bool = False
    is_simple_response: bool = False


class BettingRoomRequest(BaseModel):
    league_id: str
    season: str
    home_team: str
    away_team: str
    model: str = "Maher"
    train_pct: float = 0.7
    xi: float = 0.005
    force_refresh: bool = False
    duckdb_path: str = DUCKDB_PATH


def extract_json_object(value: str) -> dict | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None


def validate_runtime_sql(sql: str) -> str | None:
    normalized = (sql or "").strip()
    if not normalized:
        return None
    if ";" in normalized:
        return None
    lowered = normalized.casefold()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        return None
    banned = (
        "insert ",
        "update ",
        "delete ",
        "drop ",
        "alter ",
        "create ",
        "attach ",
        "copy ",
        "export ",
        "install ",
        "load ",
        "call ",
        "pragma ",
        "vacuum ",
    )
    if any(token in lowered for token in banned):
        return None
    if " matches" not in f" {lowered} ":
        return None
    return normalized


def summarize_query_table(frame) -> str:
    if frame.empty:
        return "No rows returned."
    sample = frame.head(12)
    return compact_table_context(table_payload(sample), max_rows=12)


def try_runtime_query_payload(message: str, duckdb_path: str, fallback_payload: dict) -> tuple[dict | None, bool]:
    if completion is None:
        return None, True

    skip_modes = {"knowledge", "lookup", "conversation", "direct", "none", "external_fact"}
    if fallback_payload.get("out_of_context") or fallback_payload.get("is_conversational") or fallback_payload.get("is_simple_response"):
        return None, True
    if fallback_payload.get("data_mode") in skip_modes:
        return None, True

    planner_messages = [
        {"role": "system", "content": QUERY_PLANNER_SYSTEM_PROMPT},
        {"role": "user", "content": f"User question: {message}"},
    ]
    planner_kwargs = {
        "model": MODEL,
        "messages": planner_messages,
        "temperature": 0,
        "timeout": MODEL_TIMEOUT_SECONDS,
    }
    if LITELLM_API_BASE:
        planner_kwargs["api_base"] = LITELLM_API_BASE
    if LITELLM_API_KEY:
        planner_kwargs["api_key"] = LITELLM_API_KEY

    try:
        planner_response = completion(**planner_kwargs)
        planner_content = (planner_response.choices[0].message.content or "").strip()
        planner_payload = extract_json_object(planner_content) or {}
    except Exception as exc:  # pragma: no cover - planner runtime failure
        LOGGER.warning("Runtime query planner failed: %s", exc)
        return None, True

    if not planner_payload.get("applicable"):
        return None, True

    sql = validate_runtime_sql(str(planner_payload.get("sql", "")))
    if not sql:
        return None, True

    try:
        connection = open_connection(duckdb_path)
        try:
            frame = connection.execute(f"SELECT * FROM ({sql}) AS runtime_query LIMIT 50").df()
        finally:
            connection.close()
    except Exception as exc:  # pragma: no cover - query execution failure
        LOGGER.warning("Runtime query execution failed: %s", exc)
        return None, True

    query_table = table_payload(frame) if not frame.empty else None
    summary_messages = [
        {"role": "system", "content": QUERY_SUMMARY_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"User question:\n{message}\n\n"
                f"SQL used:\n{sql}\n\n"
                f"Query result:\n{summarize_query_table(frame)}"
            ),
        },
    ]
    summary_kwargs = {
        "model": MODEL,
        "messages": summary_messages,
        "temperature": 0.1,
        "timeout": MODEL_TIMEOUT_SECONDS,
    }
    if LITELLM_API_BASE:
        summary_kwargs["api_base"] = LITELLM_API_BASE
    if LITELLM_API_KEY:
        summary_kwargs["api_key"] = LITELLM_API_KEY

    used_fallback = False
    answer = ""
    try:
        summary_response = completion(**summary_kwargs)
        answer = (summary_response.choices[0].message.content or "").strip()
    except Exception as exc:  # pragma: no cover - summary runtime failure
        LOGGER.warning("Runtime query summarizer failed: %s", exc)
        used_fallback = True

    if not answer:
        used_fallback = True
        if frame.empty:
            answer = "I ran a runtime DuckDB query, but it returned no rows for that question."
        elif len(frame) == 1 and len(frame.columns) == 1:
            answer = f"The query result is {frame.iloc[0, 0]}."
        else:
            answer = "I ran a runtime DuckDB query and returned the matching result."

    payload = {
        "answer": answer,
        "executive_summary": [],
        "tool_calls": [],
        "highlights": [],
        "table": query_table,
        "charts": [],
        "hypothesis": None,
        "sources": [],
        "suggested_prompts": [
            "Show the SQL result in more detail.",
            "Ask a follow-up about the same team or league.",
            "Compare this with another league or team.",
        ],
        "data_mode": "runtime_query",
        "out_of_context": False,
        "is_conversational": False,
        "is_simple_response": True,
        "intent": "runtime_query",
        "scope": planner_payload.get("title") or "runtime query",
    }
    return payload, used_fallback


class RefreshRequest(BaseModel):
    lookback_days: int | None = None


class RefreshResponse(BaseModel):
    status: str
    detail: str
    job_id: str
    status_url: str
    output_tail: list[str]


class RefreshStatusResponse(BaseModel):
    job_id: str
    status: str
    detail: str
    lookback_days: int
    output_tail: list[str]
    started_at: str | None = None
    finished_at: str | None = None


def build_refresh_command(lookback_days: int | None, duckdb_path: str) -> list[str]:
    command = [
        sys.executable,
        str(BASE_DIR / "scripts" / "football_data_to_gcs.py"),
        "--duckdb-path",
        duckdb_path,
        "--workers",
        str(REFRESH_WORKERS),
        "--lookback-days",
        str(lookback_days or REFRESH_LOOKBACK_DAYS),
    ]
    return command


def refresh_staging_duckdb_path(job_id: str) -> Path:
    target = Path(DUCKDB_PATH)
    suffix = target.suffix or ".duckdb"
    return target.with_name(f"{target.stem}.refresh-{job_id}{suffix}")


def prepare_refresh_staging_file(staged_path: Path) -> None:
    live_path = Path(DUCKDB_PATH)
    staged_path.parent.mkdir(parents=True, exist_ok=True)
    staged_path.unlink(missing_ok=True)
    if live_path.exists():
        shutil.copy2(live_path, staged_path)
        return
    live_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.connect(str(staged_path)).close()


def validate_refreshed_duckdb(staged_path: Path) -> None:
    connection = duckdb.connect(str(staged_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE lower(table_name) = 'matches'
            """
        ).fetchone()
        if not row or int(row[0]) < 1:
            raise ValueError("Refreshed DuckDB file does not contain the matches table.")
    finally:
        connection.close()


def promote_refreshed_duckdb(staged_path: Path) -> None:
    target = Path(DUCKDB_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    staged_path.replace(target)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def refresh_status_url(job_id: str) -> str:
    return f"/refresh/{job_id}"


def get_active_refresh_job() -> dict | None:
    with REFRESH_JOBS_LOCK:
        if ACTIVE_REFRESH_JOB_ID is None:
            return None
        job = REFRESH_JOBS.get(ACTIVE_REFRESH_JOB_ID)
        return dict(job) if job else None


def get_refresh_job(job_id: str) -> dict:
    with REFRESH_JOBS_LOCK:
        job = REFRESH_JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Refresh job not found.")
        return dict(job)


def update_refresh_job(job_id: str, **fields) -> None:
    with REFRESH_JOBS_LOCK:
        job = REFRESH_JOBS.get(job_id)
        if job is None:
            return
        job.update(fields)


def clear_active_refresh_job(job_id: str) -> None:
    global ACTIVE_REFRESH_JOB_ID

    with REFRESH_JOBS_LOCK:
        if ACTIVE_REFRESH_JOB_ID == job_id:
            ACTIVE_REFRESH_JOB_ID = None


def run_refresh_job(job_id: str, lookback_days: int) -> None:
    """Refresh the warehouse in the background using a staged DuckDB copy.

    The live DuckDB file keeps serving reads while the refresh subprocess updates
    a temporary copy. Only a validated staged file is promoted over the live
    file, which keeps the homepage-triggered refresh non-blocking for the rest
    of the product surfaces.
    """
    update_refresh_job(job_id, status="running", detail="Refresh job is running.", started_at=utc_now_iso())

    staged_path = refresh_staging_duckdb_path(job_id)
    try:
        prepare_refresh_staging_file(staged_path)
        command = build_refresh_command(lookback_days, str(staged_path))
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
            update_refresh_job(
                job_id,
                status="failed",
                detail="Refresh job failed.",
                output_tail=output_tail,
                finished_at=utc_now_iso(),
            )
            return

        validate_refreshed_duckdb(staged_path)
        promote_refreshed_duckdb(staged_path)
        update_refresh_job(
            job_id,
            status="succeeded",
            detail="Refresh done.",
            output_tail=output_tail,
            finished_at=utc_now_iso(),
        )
    except subprocess.TimeoutExpired:
        update_refresh_job(
            job_id,
            status="timed_out",
            detail=f"Refresh job exceeded {REFRESH_TIMEOUT_SECONDS} seconds.",
            finished_at=utc_now_iso(),
        )
    except Exception as exc:  # pragma: no cover - runtime protection
        LOGGER.exception("Refresh job %s crashed", job_id)
        update_refresh_job(
            job_id,
            status="failed",
            detail=f"Refresh job crashed: {exc}",
            finished_at=utc_now_iso(),
        )
    finally:
        staged_path.unlink(missing_ok=True)
        REFRESH_LOCK.release()
        clear_active_refresh_job(job_id)


def provider_label(model_name: str) -> str:
    normalized = model_name.casefold()
    if normalized.startswith("vertex_ai/"):
        return "Vertex AI"
    if normalized.startswith("ollama/"):
        return "Ollama"
    if normalized.startswith("gpt-") or normalized.startswith("openai/"):
        return "OpenAI"
    return "LiteLLM"


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
    if analysis_payload.get("is_simple_response") or analysis_payload.get("is_conversational") or analysis_payload.get("out_of_context"):
        return analysis_payload["answer"], True
    if analysis_payload.get("data_mode") in {"knowledge", "lookup", "conversation", "direct", "none", "external_fact"}:
        return analysis_payload["answer"], True

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


def llm_tool_definitions() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": "run_runtime_query",
                "description": "Run a read-only DuckDB lookup for direct factual questions answerable from the matches table.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "The user question to answer with a direct runtime DuckDB query.",
                        }
                    },
                    "required": ["question"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_analysis_pipeline",
                "description": "Run the full football analysis pipeline, including warehouse EDA or external football fallback.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "The user question to answer through the analysis pipeline.",
                        }
                    },
                    "required": ["question"],
                    "additionalProperties": False,
                },
            },
        },
    ]


def tool_message_to_dict(message) -> dict:
    if isinstance(message, dict):
        return message
    payload = {
        "role": getattr(message, "role", "assistant"),
        "content": getattr(message, "content", None),
    }
    tool_calls = getattr(message, "tool_calls", None)
    if tool_calls:
        normalized_calls = []
        for call in tool_calls:
            normalized_calls.append(
                {
                    "id": getattr(call, "id", None),
                    "type": getattr(call, "type", "function"),
                    "function": {
                        "name": getattr(getattr(call, "function", None), "name", None),
                        "arguments": getattr(getattr(call, "function", None), "arguments", "{}"),
                    },
                }
            )
        payload["tool_calls"] = normalized_calls
    return payload


def execute_llm_tool_call(tool_name: str, arguments: dict, duckdb_path: str) -> tuple[dict, bool]:
    question = str(arguments.get("question", "")).strip()
    if tool_name == "run_runtime_query":
        runtime_payload, used_fallback = try_runtime_query_payload(question, duckdb_path, {})
        if runtime_payload is None:
            analysis_payload = chat_response(question, duckdb_path)
            return analysis_payload, True
        runtime_payload["tool_calls"] = [
            {
                "name": "llm_tool_runtime_query",
                "label": "LLM Tool Call: Runtime Query",
                "summary": "The model invoked the runtime DuckDB query tool.",
            },
            *(runtime_payload.get("tool_calls") or []),
        ]
        return runtime_payload, used_fallback
    if tool_name == "run_analysis_pipeline":
        analysis_payload = chat_response(question, duckdb_path)
        analysis_payload["tool_calls"] = [
            {
                "name": "llm_tool_analysis_pipeline",
                "label": "LLM Tool Call: Analysis Pipeline",
                "summary": "The model invoked the full football analysis pipeline tool.",
            },
            *(analysis_payload.get("tool_calls") or []),
        ]
        return analysis_payload, False
    raise ValueError(f"Unsupported tool: {tool_name}")


def try_tool_calling_chat_payload(message: str, duckdb_path: str) -> tuple[dict | None, bool]:
    if completion is None:
        return None, True

    messages: list[dict] = [
        {"role": "system", "content": TOOL_ROUTER_SYSTEM_PROMPT},
        {"role": "user", "content": message},
    ]
    completion_kwargs = {
        "model": MODEL,
        "messages": messages,
        "tools": llm_tool_definitions(),
        "tool_choice": "auto",
        "temperature": 0,
        "timeout": MODEL_TIMEOUT_SECONDS,
    }
    if LITELLM_API_BASE:
        completion_kwargs["api_base"] = LITELLM_API_BASE
    if LITELLM_API_KEY:
        completion_kwargs["api_key"] = LITELLM_API_KEY

    try:
        first_response = completion(**completion_kwargs)
    except Exception as exc:
        LOGGER.warning("Tool-calling router failed, using fallback path: %s", exc)
        return None, True

    first_message = tool_message_to_dict(first_response.choices[0].message)
    tool_calls = first_message.get("tool_calls") or []
    if not tool_calls:
        return None, True

    messages.append(first_message)
    selected_payload: dict | None = None
    used_fallback = False

    for tool_call in tool_calls[:2]:
        function_payload = tool_call.get("function", {})
        tool_name = function_payload.get("name", "")
        try:
            arguments = json.loads(function_payload.get("arguments") or "{}")
        except json.JSONDecodeError:
            arguments = {}
        try:
            tool_payload, tool_used_fallback = execute_llm_tool_call(tool_name, arguments, duckdb_path)
        except Exception as exc:
            LOGGER.warning("Tool execution failed for %s: %s", tool_name, exc)
            return None, True
        selected_payload = tool_payload
        used_fallback = used_fallback or tool_used_fallback
        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call.get("id"),
                "name": tool_name,
                "content": json.dumps(
                    {
                        "answer": tool_payload.get("answer"),
                        "data_mode": tool_payload.get("data_mode"),
                        "scope": tool_payload.get("scope"),
                        "executive_summary": tool_payload.get("executive_summary", []),
                        "hypothesis": tool_payload.get("hypothesis"),
                        "table_preview": compact_table_context(tool_payload.get("table")),
                    }
                ),
            }
        )

    if selected_payload is None:
        return None, True

    final_kwargs = {
        "model": MODEL,
        "messages": messages,
        "temperature": 0.1,
        "timeout": MODEL_TIMEOUT_SECONDS,
    }
    if LITELLM_API_BASE:
        final_kwargs["api_base"] = LITELLM_API_BASE
    if LITELLM_API_KEY:
        final_kwargs["api_key"] = LITELLM_API_KEY

    try:
        final_response = completion(**final_kwargs)
        final_answer = (final_response.choices[0].message.content or "").strip()
        if final_answer:
            selected_payload["answer"] = final_answer
            return selected_payload, used_fallback
    except Exception as exc:
        LOGGER.warning("Tool-calling final answer failed, using tool payload answer: %s", exc)

    return selected_payload, True


def enriched_dashboard_payload(duckdb_path: str) -> dict:
    payload = dashboard_payload(duckdb_path)
    payload["runtime"] = {
        "model": MODEL,
        "provider": provider_label(MODEL),
        "duckdb_path": duckdb_path,
    }
    return payload


def build_chat_payload(message: str, duckdb_path: str, history: list[dict] | None = None) -> dict:
    effective_message = message
    try:
        connection = open_connection(duckdb_path)
        try:
            effective_message, _ = resolve_message_with_recent_context(connection, message, history or [])
        finally:
            connection.close()
    except Exception as exc:
        LOGGER.warning("Recent-context resolution failed, using raw message: %s", exc)

    tool_payload, tool_fallback_used = try_tool_calling_chat_payload(effective_message, duckdb_path)
    if tool_payload is not None:
        tool_payload["model"] = MODEL
        tool_payload["provider"] = provider_label(MODEL)
        tool_payload["fallback_used"] = tool_fallback_used
        return tool_payload

    analysis_payload = chat_response(effective_message, duckdb_path)
    runtime_payload, runtime_fallback_used = try_runtime_query_payload(effective_message, duckdb_path, analysis_payload)
    if runtime_payload is not None:
        runtime_payload["model"] = MODEL
        runtime_payload["provider"] = provider_label(MODEL)
        runtime_payload["fallback_used"] = runtime_fallback_used
        return runtime_payload

    answer, fallback_used = generate_model_answer(effective_message, analysis_payload)
    analysis_payload["answer"] = answer
    analysis_payload["model"] = MODEL
    analysis_payload["provider"] = provider_label(MODEL)
    analysis_payload["fallback_used"] = fallback_used
    return analysis_payload


@app.get("/")
def read_index() -> FileResponse:
    return FileResponse(INDEX_HTML_PATH)


@app.get("/standings-page")
def read_standings_page() -> FileResponse:
    return FileResponse(STANDINGS_HTML_PATH)


@app.get("/betting-room-page")
def read_betting_room_page() -> FileResponse:
    return FileResponse(BETTING_ROOM_HTML_PATH)


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


@app.get("/betting/options")
@app.get("/api/betting/options")
def get_betting_options(
    league_id: str = "E0",
    season: str | None = None,
    duckdb_path: str = DUCKDB_PATH,
) -> dict:
    try:
        return betting_options_payload(league_id=league_id, season_name=season, duckdb_path=duckdb_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime API protection
        raise HTTPException(status_code=500, detail=f"Failed to load betting room options: {exc}") from exc


@app.post("/betting/analyze")
@app.post("/api/betting/analyze")
def post_betting_analysis(request: BettingRoomRequest) -> dict:
    if request.model not in BETTING_MODELS:
        raise HTTPException(status_code=400, detail=f"Unsupported model: {request.model}")
    if not 0.3 <= request.train_pct <= 1.0:
        raise HTTPException(status_code=400, detail="train_pct must be between 0.3 and 1.0.")
    if request.home_team.strip() == request.away_team.strip():
        raise HTTPException(status_code=400, detail="Home and away teams must be different.")
    try:
        return run_betting_analysis(
            request.league_id,
            request.season,
            request.home_team.strip(),
            request.away_team.strip(),
            request.model,
            train_pct=request.train_pct,
            xi=request.xi,
            force_refresh=request.force_refresh,
            duckdb_path=request.duckdb_path,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime API protection
        raise HTTPException(status_code=500, detail=f"Failed to run betting analysis: {exc}") from exc


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
        return build_chat_payload(message, request.duckdb_path, request.history)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime API protection
        raise HTTPException(status_code=500, detail=f"Failed to answer chat request: {exc}") from exc


@app.post("/refresh", response_model=RefreshResponse, status_code=202)
@app.post("/api/refresh", response_model=RefreshResponse, status_code=202)
def refresh_data(request: RefreshRequest) -> dict:
    active_job = get_active_refresh_job()
    if active_job is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "A refresh job is already running.",
                "job_id": active_job["job_id"],
                "status_url": refresh_status_url(active_job["job_id"]),
            },
        )
    if not REFRESH_LOCK.acquire(blocking=False):
        active_job = get_active_refresh_job()
        detail: dict | str = "A refresh job is already running."
        if active_job is not None:
            detail = {
                "message": "A refresh job is already running.",
                "job_id": active_job["job_id"],
                "status_url": refresh_status_url(active_job["job_id"]),
            }
        raise HTTPException(status_code=409, detail=detail)

    lookback_days = request.lookback_days or REFRESH_LOOKBACK_DAYS
    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "status": "queued",
        "detail": "Refresh job queued.",
        "lookback_days": lookback_days,
        "output_tail": [],
        "started_at": None,
        "finished_at": None,
    }

    global ACTIVE_REFRESH_JOB_ID
    with REFRESH_JOBS_LOCK:
        REFRESH_JOBS[job_id] = job
        ACTIVE_REFRESH_JOB_ID = job_id

    worker = threading.Thread(target=run_refresh_job, args=(job_id, lookback_days), daemon=True)
    worker.start()
    return {
        "status": "accepted",
        "detail": "Refresh job accepted.",
        "job_id": job_id,
        "status_url": refresh_status_url(job_id),
        "output_tail": [],
    }


@app.get("/refresh/{job_id}", response_model=RefreshStatusResponse)
@app.get("/api/refresh/{job_id}", response_model=RefreshStatusResponse)
def get_refresh_status(job_id: str) -> dict:
    return get_refresh_job(job_id)


if __name__ == "__main__":  # pragma: no cover - local entrypoint
    uvicorn.run(app, host="127.0.0.1", port=8000)
