import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "mcp"))

from football_mcp_server import list_tools


def get_tools():
    return asyncio.run(list_tools())


def test_all_six_tools_defined():
    tools = get_tools()
    tool_names = [t.name for t in tools]
    expected = [
        "get_league_table",
        "get_bottler_index",
        "get_team_form",
        "get_head_to_head",
        "get_high_scoring_matches",
        "get_season_summary",
    ]
    for name in expected:
        assert name in tool_names


def test_tool_schemas_have_descriptions():
    tools = get_tools()
    for tool in tools:
        assert tool.description
        assert len(tool.description) > 20


def test_db_query_returns_pl_standings(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM marts.mart_league_table
            WHERE competition_code = 'PL'
        """)
        count = cur.fetchone()[0]
        assert count == 20


def test_bottler_index_drop_rates_are_valid(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM marts.mart_bottler_index
            WHERE drop_rate_pct < 0 OR drop_rate_pct > 100
        """)
        invalid_count = cur.fetchone()[0]
        assert invalid_count == 0


def test_match_results_goals_are_non_negative(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM marts.mart_match_results
            WHERE home_goals < 0 OR away_goals < 0
        """)
        invalid_count = cur.fetchone()[0]
        assert invalid_count == 0
