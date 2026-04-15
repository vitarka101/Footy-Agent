from pathlib import Path

from scripts.betting_room_service import MODEL_NAMES, options_payload, run_betting_analysis


def test_betting_room_options_load_team_choices() -> None:
    payload = options_payload("E0", "2024/2025")
    assert payload["selected_league"] == "E0"
    assert payload["team_options"]
    assert any(item["value"] == "Arsenal" for item in payload["team_options"])


def test_betting_room_analysis_returns_tool_trace_and_artifact() -> None:
    payload = run_betting_analysis("E0", "2024/2025", "Arsenal", "Aston Villa", "Maher")
    tool_names = [tool["name"] for tool in payload["tool_calls"]]
    assert payload["selected_model"] in MODEL_NAMES
    assert payload["hypothesis"] is not None
    assert "collect_match_data" in tool_names
    assert "probability_model" in tool_names
    assert "assumption_tests" in tool_names
    assert "market_edge" in tool_names
    assert "betting_hypothesis" in tool_names
    assert len(payload["predicted_table"]) > 0
    assert len(payload["actual_table"]) > 0
    assert Path(payload["artifact_path"]).suffix == ".md"
