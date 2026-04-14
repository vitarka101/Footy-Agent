import duckdb

from football_ui_service import detect_intent, parse_gcs_uri, resolve_scope
from app import compact_table_context, provider_label


def test_detect_intent_home_advantage() -> None:
    assert detect_intent("How has home advantage changed over time?") == "home_advantage"


def test_detect_intent_data_quality() -> None:
    assert detect_intent("Which columns have the most missing data?") == "data_quality"


def test_parse_gcs_uri() -> None:
    assert parse_gcs_uri("gs://footy-agent/runtime/football_data.duckdb") == (
        "footy-agent",
        "runtime/football_data.duckdb",
    )


def test_resolve_scope_la_liga() -> None:
    con = duckdb.connect("football_data.duckdb", read_only=True)
    try:
        scope = resolve_scope(con, "Analyze La Liga")
    finally:
        con.close()
    assert scope.country == "Spain"
    assert scope.league == "La Liga Primera Division"


def test_provider_label() -> None:
    assert provider_label("vertex_ai/gemini-2.5-flash-lite") == "Vertex AI"
    assert provider_label("ollama/llama3.1") == "Ollama"


def test_compact_table_context_truncates() -> None:
    context = compact_table_context(
        {
            "columns": ["season", "avg_goals"],
            "rows": [
                ["2021-2022", "2.64"],
                ["2022-2023", "2.71"],
                ["2023-2024", "2.77"],
                ["2024-2025", "2.74"],
                ["2025-2026", "2.69"],
                ["2026-2027", "2.68"],
                ["2027-2028", "2.70"],
            ],
        },
        max_rows=3,
    )
    assert "season, avg_goals" in context
    assert "... 4 more rows omitted" in context
