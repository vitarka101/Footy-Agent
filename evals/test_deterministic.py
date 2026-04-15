import duckdb

from football_ui_service import chat_response, detect_intent, parse_gcs_uri, resolve_scope
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


def test_world_cup_titles_query_uses_external_fact_mode() -> None:
    payload = chat_response("can you tell me which country won the fifa most")
    assert payload["data_mode"] == "external_fact"
    assert payload["scope"] == "FIFA World Cup"
    assert "Brazil has won the FIFA World Cup the most" in payload["answer"]


def test_analytics_queries_default_to_schema_driven_eda() -> None:
    payload = chat_response("Analyze La Liga")
    tool_names = [tool["name"] for tool in payload["tool_calls"]]
    assert payload["data_mode"] == "warehouse"
    assert "agent_framework" in tool_names
    assert "schema_profile" in tool_names
    assert "iterative_eda" in tool_names
    assert "parallel_specialists" in tool_names
    assert payload["hypothesis"] is not None
    assert payload["hypothesis"].get("evidence_objects")
    assert len(payload["hypothesis"].get("candidates", [])) >= 1
    assert len(payload.get("charts", [])) >= 5
    assert len(payload.get("charts", [])) <= 6


def test_team_performance_queries_show_full_eda_pack() -> None:
    payload = chat_response("how has arsenal performed in the last 3 seasons")
    assert payload["intent"] == "team_performance"
    assert payload["data_mode"] == "warehouse"
    assert not payload["is_simple_response"]
    assert payload["hypothesis"] is not None
    assert payload["hypothesis"].get("evidence_objects")
    assert 5 <= len(payload.get("charts", [])) <= 6


def test_external_football_business_questions_use_web_fallback() -> None:
    payload = chat_response("What league generates most revenue ?")
    assert payload["data_mode"] in {"web_fallback", "web_fallback_failed"}


def test_profitable_league_question_uses_web_fallback() -> None:
    payload = chat_response("what is the most profitable league")
    assert payload["data_mode"] in {"web_fallback", "web_fallback_failed"}


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
