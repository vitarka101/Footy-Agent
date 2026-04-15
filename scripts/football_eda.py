#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

LOGGER = logging.getLogger("football-eda")
DEFAULT_DUCKDB_PATH = "football_data.duckdb"
DEFAULT_OUTPUT_DIR = "artifacts/eda"
LEAGUE_ORDER = ["Premier League", "Championship", "League One", "League Two"]
LEAGUE_NAME_BY_DIV = {
    "E0": "Premier League",
    "E1": "Championship",
    "E2": "League One",
    "E3": "League Two",
}
TEXT_COLUMNS = [
    "div",
    "season",
    "league",
    "date",
    "time",
    "hometeam",
    "awayteam",
    "ftr",
    "htr",
    "referee",
]
NUMERIC_COLUMNS = [
    "fthg",
    "ftag",
    "hthg",
    "htag",
    "hs",
    "as",
    "hst",
    "ast",
    "hf",
    "af",
    "hc",
    "ac",
    "hy",
    "ay",
    "hr",
    "ar",
]
ANALYSIS_COLUMNS = TEXT_COLUMNS + NUMERIC_COLUMNS
DEFAULT_DISTRIBUTION_COLUMNS = ["fthg", "hr", "ar"]
COLOR_SCALE = [(0.0, "#ffffff"), (1.0, "#191970")]
RESULT_ORDER = ["H", "D", "A"]
RESULT_LABELS = {"H": "Home Win", "D": "Draw", "A": "Away Win"}
RESULT_COLORS = {"H": "#0f766e", "D": "#64748b", "A": "#b91c1c"}
CORRELATION_COLUMNS = [
    "fthg",
    "ftag",
    "total_goals",
    "goal_diff",
    "hs",
    "as",
    "total_shots",
    "hst",
    "ast",
    "total_shots_on_target",
    "hf",
    "af",
    "total_fouls",
    "hc",
    "ac",
    "total_corners",
    "hy",
    "ay",
    "hr",
    "ar",
    "total_cards",
]


def parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def configure_plotly_theme() -> None:
    pio.templates["footy_agent"] = go.layout.Template(
        layout=go.Layout(
            font=dict(family="Arial, sans-serif", size=12, color="#1f2937"),
            paper_bgcolor="white",
            plot_bgcolor="white",
            title=dict(font=dict(size=18, color="#0f172a")),
            legend=dict(borderwidth=0),
            margin=dict(l=60, r=40, t=80, b=60),
        )
    )
    pio.templates.default = "plotly_white+footy_agent"


def connect_duckdb(path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path, read_only=True)


def load_england_four_tiers(
    connection: duckdb.DuckDBPyConnection,
    seasons: list[str] | None = None,
) -> pd.DataFrame:
    filters = ["lower(country) = 'england'", "div IN ('E0', 'E1', 'E2', 'E3')"]
    if seasons:
        season_list = ", ".join(quote_sql_string(value) for value in seasons)
        filters.append(f"season IN ({season_list})")

    query = f"""
        SELECT
            div,
            CASE div
                WHEN 'E0' THEN 'Premier League'
                WHEN 'E1' THEN 'Championship'
                WHEN 'E2' THEN 'League One'
                WHEN 'E3' THEN 'League Two'
            END AS league,
            season,
            TRY_CAST(substr(season, 1, 4) AS INTEGER) AS season_start_year,
            TRY_CAST(substr(season, 6, 4) AS INTEGER) AS season_end_year,
            date,
            time,
            hometeam,
            awayteam,
            fthg,
            ftag,
            ftr,
            hthg,
            htag,
            htr,
            referee,
            hs,
            as,
            hst,
            ast,
            hf,
            af,
            hc,
            ac,
            hy,
            ay,
            hr,
            ar
        FROM matches
        WHERE {" AND ".join(filters)}
        ORDER BY season_start_year, div, date, hometeam, awayteam
    """
    dataframe = connection.execute(query).df()
    if dataframe.empty:
        raise ValueError("No England E0-E3 rows matched the selected filters.")

    dataframe["league"] = pd.Categorical(dataframe["league"], categories=LEAGUE_ORDER, ordered=True)
    return dataframe


def prepare_analysis_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    prepared["total_goals"] = prepared["fthg"].fillna(0) + prepared["ftag"].fillna(0)
    prepared["goal_diff"] = prepared["fthg"].fillna(0) - prepared["ftag"].fillna(0)
    prepared["total_shots"] = prepared["hs"].fillna(0) + prepared["as"].fillna(0)
    prepared["total_shots_on_target"] = prepared["hst"].fillna(0) + prepared["ast"].fillna(0)
    prepared["total_fouls"] = prepared["hf"].fillna(0) + prepared["af"].fillna(0)
    prepared["total_corners"] = prepared["hc"].fillna(0) + prepared["ac"].fillna(0)
    prepared["total_cards"] = (
        prepared["hy"].fillna(0)
        + prepared["ay"].fillna(0)
        + prepared["hr"].fillna(0)
        + prepared["ar"].fillna(0)
    )
    prepared["is_home_win"] = (prepared["ftr"] == "H").astype(int)
    prepared["is_draw"] = (prepared["ftr"] == "D").astype(int)
    prepared["is_away_win"] = (prepared["ftr"] == "A").astype(int)
    return prepared


def ensure_output_dir(path: str) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_dataframe(dataframe: pd.DataFrame, path: Path) -> None:
    dataframe.to_csv(path, index=False)
    LOGGER.info("WROTE %s", path)


def write_json(payload: dict, path: Path) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOGGER.info("WROTE %s", path)


def write_html_figure(figure: go.Figure, path: Path) -> None:
    figure.write_html(str(path), include_plotlyjs="cdn")
    LOGGER.info("WROTE %s", path)


def dataset_overview_tool(
    dataframe: pd.DataFrame,
    output_dir: Path,
) -> dict:
    league_summary = (
        dataframe.groupby("league", observed=True)
        .agg(
            matches=("league", "size"),
            seasons=("season", "nunique"),
            first_season=("season", "min"),
            last_season=("season", "max"),
        )
        .reset_index()
    )

    season_summary = (
        dataframe.groupby(["league", "season", "season_start_year", "season_end_year"], observed=True)
        .size()
        .reset_index(name="matches")
        .sort_values(["season_start_year", "league"])
    )

    missing_overall = (
        dataframe[ANALYSIS_COLUMNS]
        .isna()
        .mean()
        .mul(100)
        .reset_index()
        .rename(columns={"index": "column", 0: "missing_pct"})
        .sort_values(["missing_pct", "column"], ascending=[False, True])
    )

    overview = {
        "rows": int(len(dataframe)),
        "seasons": int(dataframe["season"].nunique()),
        "leagues": int(dataframe["league"].nunique()),
        "season_min": str(dataframe["season"].min()),
        "season_max": str(dataframe["season"].max()),
        "columns_used": ANALYSIS_COLUMNS,
    }

    write_json(overview, output_dir / "overview.json")
    write_dataframe(league_summary, output_dir / "overview_rows_by_league.csv")
    write_dataframe(season_summary, output_dir / "overview_rows_by_league_season.csv")
    write_dataframe(missing_overall, output_dir / "overview_missing_overall.csv")

    return overview


def statistical_aggregation_tool(
    dataframe: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    summary = (
        dataframe.groupby(["league", "season", "season_start_year"], observed=True)
        .agg(
            matches=("league", "size"),
            avg_total_goals=("total_goals", "mean"),
            median_total_goals=("total_goals", "median"),
            avg_home_goals=("fthg", "mean"),
            avg_away_goals=("ftag", "mean"),
            avg_total_shots=("total_shots", "mean"),
            avg_total_cards=("total_cards", "mean"),
            home_win_rate=("is_home_win", "mean"),
            draw_rate=("is_draw", "mean"),
            away_win_rate=("is_away_win", "mean"),
        )
        .reset_index()
        .sort_values(["season_start_year", "league"])
    )

    rate_columns = ["home_win_rate", "draw_rate", "away_win_rate"]
    summary[rate_columns] = summary[rate_columns] * 100
    write_dataframe(summary, output_dir / "statistical_aggregation_summary.csv")

    figure = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=["Average Goals per Match by Season", "Home Win Rate by Season"],
        vertical_spacing=0.12,
    )
    for league_name in LEAGUE_ORDER:
        league_data = summary[summary["league"] == league_name]
        figure.add_trace(
            go.Scatter(
                x=league_data["season_start_year"],
                y=league_data["avg_total_goals"],
                mode="lines+markers",
                name=league_name,
                legendgroup=league_name,
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=league_data["season_start_year"],
                y=league_data["home_win_rate"],
                mode="lines+markers",
                name=league_name,
                legendgroup=league_name,
                showlegend=False,
            ),
            row=2,
            col=1,
        )

    figure.update_yaxes(title_text="Goals", row=1, col=1)
    figure.update_yaxes(title_text="Home Win %", row=2, col=1)
    figure.update_xaxes(title_text="Season Start Year", row=2, col=1)
    figure.update_layout(
        title="Statistical Aggregation Tool: Season-Level Match Trends",
        height=900,
        width=1200,
    )
    write_html_figure(figure, output_dir / "statistical_aggregation_trends.html")

    return summary


def filtering_grouping_tool(
    dataframe: pd.DataFrame,
    output_dir: Path,
) -> dict[str, pd.DataFrame]:
    league_summary = (
        dataframe.groupby("league", observed=True)
        .agg(
            matches=("league", "size"),
            seasons=("season", "nunique"),
            avg_total_goals=("total_goals", "mean"),
            avg_total_shots=("total_shots", "mean"),
            avg_total_cards=("total_cards", "mean"),
            home_win_rate=("is_home_win", "mean"),
            draw_rate=("is_draw", "mean"),
            away_win_rate=("is_away_win", "mean"),
        )
        .reset_index()
    )
    league_summary[["home_win_rate", "draw_rate", "away_win_rate"]] = (
        league_summary[["home_win_rate", "draw_rate", "away_win_rate"]] * 100
    )
    write_dataframe(league_summary, output_dir / "filtering_grouping_league_summary.csv")

    result_share = (
        dataframe.groupby(["league", "ftr"], observed=True)
        .size()
        .reset_index(name="matches")
    )
    totals = result_share.groupby("league", observed=True)["matches"].transform("sum")
    result_share["share_pct"] = result_share["matches"] / totals * 100
    result_share["result_label"] = result_share["ftr"].map(RESULT_LABELS)
    write_dataframe(result_share, output_dir / "filtering_grouping_result_share.csv")

    goals_bar = league_summary.sort_values("avg_total_goals", ascending=False)
    figure_bar = go.Figure(
        data=[
            go.Bar(
                x=goals_bar["league"],
                y=goals_bar["avg_total_goals"],
                marker_color="#191970",
                hovertemplate="League=%{x}<br>Avg Goals=%{y:.2f}<extra></extra>",
            )
        ]
    )
    figure_bar.update_layout(
        title="Filtering and Grouping Tool: Average Goals by League",
        xaxis_title="League",
        yaxis_title="Average Goals per Match",
        width=1000,
        height=520,
    )
    write_html_figure(figure_bar, output_dir / "filtering_grouping_avg_goals_by_league.html")

    figure_stack = go.Figure()
    for result_code in RESULT_ORDER:
        plot_data = result_share[result_share["ftr"] == result_code]
        figure_stack.add_trace(
            go.Bar(
                x=plot_data["league"],
                y=plot_data["share_pct"],
                name=RESULT_LABELS[result_code],
                marker_color=RESULT_COLORS[result_code],
                hovertemplate="League=%{x}<br>Result=" + RESULT_LABELS[result_code] + "<br>Share=%{y:.2f}%<extra></extra>",
            )
        )
    figure_stack.update_layout(
        title="Filtering and Grouping Tool: Result Composition by League",
        barmode="stack",
        xaxis_title="League",
        yaxis_title="Match Share (%)",
        width=1000,
        height=560,
    )
    write_html_figure(figure_stack, output_dir / "filtering_grouping_result_share_by_league.html")

    return {"league_summary": league_summary, "result_share": result_share}


def correlation_analysis_tool(
    dataframe: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    numeric_frame = dataframe[CORRELATION_COLUMNS].copy()
    valid_columns = [
        column_name
        for column_name in CORRELATION_COLUMNS
        if column_name in numeric_frame.columns and numeric_frame[column_name].notna().mean() >= 0.35
    ]
    filtered = numeric_frame[valid_columns]
    correlation = filtered.corr(numeric_only=True)
    correlation_long = (
        correlation.reset_index()
        .melt(id_vars="index", var_name="column_b", value_name="correlation")
        .rename(columns={"index": "column_a"})
    )
    write_dataframe(correlation_long, output_dir / "correlation_matrix.csv")

    heatmap = go.Figure(
        data=[
            go.Heatmap(
                z=correlation.to_numpy(),
                x=list(correlation.columns),
                y=list(correlation.index),
                colorscale="RdBu",
                zmin=-1,
                zmax=1,
                hovertemplate="A=%{y}<br>B=%{x}<br>Correlation=%{z:.2f}<extra></extra>",
            )
        ]
    )
    heatmap.update_layout(
        title="Correlation Analysis Tool: Match Metric Correlation Matrix",
        width=1200,
        height=980,
    )
    write_html_figure(heatmap, output_dir / "correlation_matrix.html")

    pairwise = correlation_long[correlation_long["column_a"] < correlation_long["column_b"]].copy()
    pairwise["abs_correlation"] = pairwise["correlation"].abs()
    strongest = pairwise.sort_values("abs_correlation", ascending=False).head(20)
    write_dataframe(strongest, output_dir / "correlation_top_pairs.csv")

    return strongest


def missing_values_tool(
    dataframe: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    summary = (
        dataframe.groupby(["league", "season", "season_end_year"], observed=True)[ANALYSIS_COLUMNS]
        .agg(lambda column: column.isna().mean() * 100)
        .reset_index()
    )

    missing_long = summary.melt(
        id_vars=["league", "season", "season_end_year"],
        value_vars=ANALYSIS_COLUMNS,
        var_name="column",
        value_name="missing_pct",
    )
    missing_long["season_label"] = missing_long["season_end_year"].astype("Int64").astype(str)
    missing_long = missing_long.sort_values(["league", "season_end_year", "column"])

    write_dataframe(missing_long, output_dir / "missing_values_summary.csv")

    figure = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=LEAGUE_ORDER,
        vertical_spacing=0.10,
        horizontal_spacing=0.06,
    )

    for index, league_name in enumerate(LEAGUE_ORDER):
        league_data = missing_long[missing_long["league"] == league_name]
        pivot = (
            league_data.pivot(index="season_label", columns="column", values="missing_pct")
            .sort_index()
        )
        row = index // 2 + 1
        col = index % 2 + 1
        figure.add_trace(
            go.Heatmap(
                z=pivot.to_numpy(),
                x=list(pivot.columns),
                y=list(pivot.index),
                colorscale=COLOR_SCALE,
                zmin=0,
                zmax=100,
                colorbar=dict(title="Missing %") if index == 0 else None,
                showscale=index == 0,
                hovertemplate="League=%{meta}<br>Season End=%{y}<br>Column=%{x}<br>Missing=%{z:.1f}%<extra></extra>",
                meta=league_name,
            ),
            row=row,
            col=col,
        )

    figure.update_layout(
        title="Missing Values by League, Season, and Column",
        height=980,
        width=1400,
    )
    figure.update_xaxes(tickangle=90)
    write_html_figure(figure, output_dir / "missing_values_heatmap.html")

    return missing_long


def zscore_outlier_pct(series: pd.Series) -> float:
    if len(series) < 2:
        return 0.0
    std = series.std(ddof=0)
    if std == 0 or pd.isna(std):
        return 0.0
    zscores = (series - series.mean()) / std
    return float((zscores.abs() > 3).mean() * 100)


def iqr_outlier_pct(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return float(((series < lower) | (series > upper)).mean() * 100)


def percentile_outlier_pct(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    p1 = series.quantile(0.01)
    p99 = series.quantile(0.99)
    return float(((series < p1) | (series > p99)).mean() * 100)


def outlier_analysis_tool(
    dataframe: pd.DataFrame,
    output_dir: Path,
    distribution_columns: list[str],
) -> pd.DataFrame:
    records: list[dict] = []

    for column_name in NUMERIC_COLUMNS:
        series = dataframe[column_name].dropna().astype(float)
        if series.empty:
            continue

        records.append(
            {
                "column": column_name,
                "count": int(series.shape[0]),
                "mean": float(series.mean()),
                "median": float(series.median()),
                "min": float(series.min()),
                "max": float(series.max()),
                "zscore_pct": zscore_outlier_pct(series),
                "iqr_pct": iqr_outlier_pct(series),
                "percentile_pct": percentile_outlier_pct(series),
            }
        )

    outlier_summary = pd.DataFrame.from_records(records).sort_values("column")
    write_dataframe(outlier_summary, output_dir / "outlier_summary.csv")

    heatmap_frame = outlier_summary.melt(
        id_vars=["column"],
        value_vars=["zscore_pct", "iqr_pct", "percentile_pct"],
        var_name="method",
        value_name="outlier_pct",
    )
    write_dataframe(heatmap_frame, output_dir / "outlier_heatmap_summary.csv")

    heatmap = (
        heatmap_frame.pivot(index="column", columns="method", values="outlier_pct")
        .loc[:, ["zscore_pct", "iqr_pct", "percentile_pct"]]
    )

    heatmap_figure = go.Figure(
        data=[
            go.Heatmap(
                z=heatmap.to_numpy(),
                x=["z-score", "IQR", "Percentile 1/99"],
                y=list(heatmap.index),
                colorscale=COLOR_SCALE,
                hovertemplate="Variable=%{y}<br>Method=%{x}<br>Outliers=%{z:.2f}%<extra></extra>",
            )
        ]
    )
    heatmap_figure.update_layout(
        title="Outlier Percentage by Variable and Detection Method",
        height=820,
        width=980,
    )
    write_html_figure(heatmap_figure, output_dir / "outlier_heatmap.html")

    valid_distribution_columns = [column for column in distribution_columns if column in NUMERIC_COLUMNS]
    distribution_figure = make_subplots(
        rows=len(valid_distribution_columns),
        cols=1,
        subplot_titles=[column.upper() for column in valid_distribution_columns],
        vertical_spacing=0.10,
    )

    for index, column_name in enumerate(valid_distribution_columns, start=1):
        series = dataframe[column_name].dropna().astype(float)
        lower = float(series.quantile(0.05))
        upper = float(series.quantile(0.95))
        distribution_figure.add_trace(
            go.Histogram(
                x=series,
                marker=dict(color="#191970"),
                opacity=0.82,
                nbinsx=min(25, max(10, int(series.nunique()))),
                name=column_name.upper(),
                showlegend=False,
                hovertemplate=f"{column_name.upper()}=%{{x}}<br>Matches=%{{y}}<extra></extra>",
            ),
            row=index,
            col=1,
        )
        distribution_figure.add_vline(
            x=lower,
            line_dash="dash",
            line_color="#b91c1c",
            row=index,
            col=1,
        )
        distribution_figure.add_vline(
            x=upper,
            line_dash="dash",
            line_color="#b91c1c",
            row=index,
            col=1,
        )

    distribution_figure.update_layout(
        title="Distribution Checks with 5th and 95th Percentile Reference Lines",
        height=max(420, 320 * max(1, len(valid_distribution_columns))),
        width=1100,
        bargap=0.05,
    )
    distribution_figure.update_xaxes(title_text="Value")
    distribution_figure.update_yaxes(title_text="Matches")
    write_html_figure(distribution_figure, output_dir / "outlier_distributions.html")

    return outlier_summary


def run_all_tools(
    dataframe: pd.DataFrame,
    output_dir: Path,
    distribution_columns: list[str],
) -> dict:
    overview = dataset_overview_tool(dataframe, output_dir)
    aggregates = statistical_aggregation_tool(dataframe, output_dir)
    grouped = filtering_grouping_tool(dataframe, output_dir)
    correlations = correlation_analysis_tool(dataframe, output_dir)
    missing_values = missing_values_tool(dataframe, output_dir)
    outliers = outlier_analysis_tool(dataframe, output_dir, distribution_columns)
    manifest = {
        "overview_rows": overview["rows"],
        "aggregate_rows": int(len(aggregates)),
        "grouped_leagues": int(len(grouped["league_summary"])),
        "top_correlations": int(len(correlations)),
        "missing_value_cells": int(len(missing_values)),
        "outlier_variables": int(len(outliers)),
        "output_dir": str(output_dir),
        "artifacts": sorted(path.name for path in output_dir.iterdir()),
    }
    write_json(manifest, output_dir / "manifest.json")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Python EDA tools for the England E0-E3 football dataset stored in DuckDB. "
            "The commands are designed as assignment-style analytical tool calls over "
            "collected data: statistical aggregation, filtering/grouping, correlation, "
            "and supplemental data-quality checks."
        )
    )
    parser.add_argument(
        "command",
        choices=("overview", "aggregate", "segment", "correlation", "missingness", "outliers", "all"),
        help="Which EDA tool to run.",
    )
    parser.add_argument(
        "--duckdb-path",
        default=DEFAULT_DUCKDB_PATH,
        help=f"DuckDB database path. Defaults to {DEFAULT_DUCKDB_PATH}.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for generated tables and plots. Defaults to {DEFAULT_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--seasons",
        help="Optional comma-separated season filter, for example 2024-2025,2025-2026.",
    )
    parser.add_argument(
        "--distribution-columns",
        default=",".join(DEFAULT_DISTRIBUTION_COLUMNS),
        help="Numeric columns for histogram-based outlier distribution plots.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity. Defaults to INFO.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")
    configure_plotly_theme()

    seasons = parse_csv_list(args.seasons)
    distribution_columns = parse_csv_list(args.distribution_columns)
    output_dir = ensure_output_dir(args.output_dir)

    connection = connect_duckdb(args.duckdb_path)
    try:
        dataframe = prepare_analysis_frame(load_england_four_tiers(connection, seasons=seasons))
    finally:
        connection.close()

    LOGGER.info(
        "Loaded %s England E0-E3 matches across %s seasons into the EDA workspace.",
        len(dataframe),
        dataframe["season"].nunique(),
    )

    if args.command == "overview":
        dataset_overview_tool(dataframe, output_dir)
    elif args.command == "aggregate":
        statistical_aggregation_tool(dataframe, output_dir)
    elif args.command == "segment":
        filtering_grouping_tool(dataframe, output_dir)
    elif args.command == "correlation":
        correlation_analysis_tool(dataframe, output_dir)
    elif args.command == "missingness":
        missing_values_tool(dataframe, output_dir)
    elif args.command == "outliers":
        outlier_analysis_tool(dataframe, output_dir, distribution_columns)
    else:
        run_all_tools(dataframe, output_dir, distribution_columns)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
