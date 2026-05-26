"""Tests for the MCP server tool definitions and season resolution logic."""
import asyncio
from datetime import date
from unittest.mock import patch

import pytest

from football_analytics.mcp.server import list_tools
from football_analytics.agent.tool_handlers import ToolHandlers
from football_analytics.agent.football_agent import call_tool


# ---------------------------------------------------------------------------
# Tool definition tests
# ---------------------------------------------------------------------------

def get_tools():
    return asyncio.run(list_tools())


def test_all_tools_defined():
    tools = get_tools()
    tool_names = [t.name for t in tools]
    expected = [s["name"] for s in ToolHandlers.SCHEMAS]
    for name in expected:
        assert name in tool_names, f"Tool '{name}' missing from MCP server"


def test_tool_schemas_have_descriptions():
    tools = get_tools()
    for tool in tools:
        assert tool.description
        assert len(tool.description) > 20


def test_data_tools_have_season_start_year_param():
    tools = get_tools()
    tool_map = {t.name: t for t in tools}
    data_tools = [
        "get_league_table",
        "get_bottler_index",
        "get_team_form",
        "get_head_to_head",
        "get_high_scoring_matches",
        "get_season_summary",
    ]
    for name in data_tools:
        props = tool_map[name].inputSchema["properties"]
        assert "season_start_year" in props, f"'{name}' is missing season_start_year param"


# ---------------------------------------------------------------------------
# Season resolution unit tests (no DB required)
# ---------------------------------------------------------------------------

class TestCurrentSeasonYear:
    def test_month_before_july_returns_previous_year(self):
        with patch("football_analytics.agent.tool_handlers.date") as mock_date:
            mock_date.today.return_value = date(2026, 5, 24)
            assert ToolHandlers.current_season_year() == 2025

    def test_july_returns_current_year(self):
        with patch("football_analytics.agent.tool_handlers.date") as mock_date:
            mock_date.today.return_value = date(2026, 7, 1)
            assert ToolHandlers.current_season_year() == 2026

    def test_august_returns_current_year(self):
        with patch("football_analytics.agent.tool_handlers.date") as mock_date:
            mock_date.today.return_value = date(2026, 8, 15)
            assert ToolHandlers.current_season_year() == 2026

    def test_december_returns_current_year(self):
        with patch("football_analytics.agent.tool_handlers.date") as mock_date:
            mock_date.today.return_value = date(2025, 12, 1)
            assert ToolHandlers.current_season_year() == 2025

    def test_january_returns_previous_year(self):
        with patch("football_analytics.agent.tool_handlers.date") as mock_date:
            mock_date.today.return_value = date(2025, 1, 15)
            assert ToolHandlers.current_season_year() == 2024


class TestParseSeasonReference:
    """Uses a fixed "current" season of 2025 (simulating May 2026)."""

    @pytest.fixture(autouse=True)
    def fix_current_season(self):
        with patch("football_analytics.agent.tool_handlers.date") as mock_date:
            mock_date.today.return_value = date(2026, 5, 24)
            yield

    def test_this_season(self):
        assert ToolHandlers.parse_season_reference("this season") == 2025

    def test_current_season(self):
        assert ToolHandlers.parse_season_reference("current season") == 2025

    def test_last_season(self):
        assert ToolHandlers.parse_season_reference("last season") == 2024

    def test_previous_season(self):
        assert ToolHandlers.parse_season_reference("previous season") == 2024

    def test_two_seasons_ago(self):
        assert ToolHandlers.parse_season_reference("2 seasons ago") == 2023

    def test_four_seasons_ago(self):
        assert ToolHandlers.parse_season_reference("4 seasons ago") == 2021

    def test_explicit_yyyy_dash_yyyy(self):
        assert ToolHandlers.parse_season_reference("2023-2024") == 2023

    def test_explicit_yyyy_slash_yy(self):
        assert ToolHandlers.parse_season_reference("2023/24") == 2023

    def test_bare_year(self):
        assert ToolHandlers.parse_season_reference("2022") == 2022

    def test_case_insensitive(self):
        assert ToolHandlers.parse_season_reference("THIS SEASON") == 2025
        assert ToolHandlers.parse_season_reference("Last Season") == 2024


class TestSeasonDateFilter:
    def test_none_returns_empty(self):
        clause, params = ToolHandlers._season_date_filter(None)
        assert clause == ""
        assert params == []

    def test_2024_season_spans_two_years(self):
        clause, params = ToolHandlers._season_date_filter(2024)
        assert "MAKE_DATE(%s, 7, 1)" in clause
        assert params == [2024, 2025]

    def test_2025_season(self):
        _, params = ToolHandlers._season_date_filter(2025)
        assert params == [2025, 2026]


class TestSeasonIdFilter:
    def test_returns_subquery_with_correct_params(self):
        clause, params = ToolHandlers._season_id_filter("PL", 2024)
        assert "mart_match_results" in clause
        assert "MAKE_DATE" in clause
        assert params == ["PL", 2024, 2025]

    def test_upper_bound_is_next_year(self):
        _, params = ToolHandlers._season_id_filter("BL1", 2023)
        assert params[2] == 2024


# ---------------------------------------------------------------------------
# Integration tests (require live Supabase connection)
# ---------------------------------------------------------------------------

class TestResolveSeasonTool:
    def test_returns_season_start_year_and_label(self):
        result = call_tool("resolve_season", {"reference": "last season"})
        assert "season_start_year" in result
        assert "season_label" in result
        assert isinstance(result["season_start_year"], int)
        assert "/" in result["season_label"]

    def test_last_season_label_format(self):
        result = call_tool("resolve_season", {"reference": "last season"})
        year = result["season_start_year"]
        expected_label = f"{year}/{str(year + 1)[-2:]}"
        assert result["season_label"] == expected_label


class TestLeagueTableWithSeason:
    def test_2024_season_returns_20_pl_teams(self):
        rows = call_tool("get_league_table", {"competition_code": "PL", "season_start_year": 2024})
        assert len(rows) == 20

    def test_rows_have_no_duplicates(self):
        rows = call_tool("get_league_table", {"competition_code": "PL", "season_start_year": 2024})
        team_names = [r["team_name"] for r in rows]
        assert len(team_names) == len(set(team_names)), "Duplicate teams returned"

    def test_positions_are_sequential(self):
        rows = call_tool("get_league_table", {"competition_code": "PL", "season_start_year": 2024})
        positions = [r["position"] for r in rows]
        assert positions == list(range(1, 21))

    def test_no_season_defaults_to_latest(self):
        rows = call_tool("get_league_table", {"competition_code": "PL"})
        assert len(rows) > 0
        team_names = [r["team_name"] for r in rows]
        assert len(team_names) == len(set(team_names)), "Duplicate teams in default season"


class TestBottlerIndexWithSeason:
    def test_2024_season_returns_results(self):
        rows = call_tool("get_bottler_index", {"competition_code": "BL1", "season_start_year": 2024})
        assert len(rows) > 0

    def test_drop_rates_are_valid_percentages(self):
        rows = call_tool("get_bottler_index", {"competition_code": "PL", "season_start_year": 2024})
        for row in rows:
            assert 0 <= row["drop_rate_pct"] <= 100

    def test_no_season_returns_no_duplicates(self):
        rows = call_tool("get_bottler_index", {"competition_code": "PL"})
        team_names = [r["team_name"] for r in rows]
        assert len(team_names) == len(set(team_names)), "Duplicate teams in bottler index"


class TestMatchResultsSeasonFilter:
    def test_2024_season_match_dates_in_range(self):
        rows = call_tool("get_high_scoring_matches", {"season_start_year": 2024, "min_goals": 4, "limit": 50})
        for row in rows:
            d = str(row["match_date"])
            assert d >= "2024-07-01", f"Match {d} predates 2024/25 season"
            assert d < "2025-07-01", f"Match {d} postdates 2024/25 season"

    def test_season_summary_returns_one_row_per_competition(self):
        result = call_tool("get_season_summary", {"competition_code": "PL", "season_start_year": 2024})
        assert len(result) == 1
        assert result[0]["competition_code"] == "PL"


class TestDbIntegrity:
    def test_match_results_goals_non_negative(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM marts.mart_match_results
                WHERE home_goals < 0 OR away_goals < 0
            """)
            assert cur.fetchone()[0] == 0

    def test_bottler_drop_rates_valid(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM marts.mart_bottler_index
                WHERE drop_rate_pct < 0 OR drop_rate_pct > 100
            """)
            assert cur.fetchone()[0] == 0
