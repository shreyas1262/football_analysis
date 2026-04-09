import asyncio
import json
import os
from decimal import Decimal

import psycopg2
import psycopg2.extras
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "football_db"),
    "user": os.getenv("DB_USER", "football"),
    "password": os.getenv("DB_PASSWORD", "football"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def query_to_json(sql: str, params: tuple) -> str:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    # Convert Decimal → float so json.dumps doesn't choke
    clean = [
        {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]
    return json.dumps(clean, default=str, indent=2)


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

server = Server("football-analytics")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_league_table",
            description=(
                "Returns current league standings with points, wins, goals and "
                "performance metrics. Use for questions about league position, "
                "title races, who is top, who is bottom, or points gaps."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "competition_code": {
                        "type": "string",
                        "description": "League code: PL, PD, BL1, or SA",
                        "enum": ["PL", "PD", "BL1", "SA"],
                    },
                    "season": {
                        "type": "integer",
                        "description": "Season start year (default 2024)",
                        "default": 2024,
                    },
                },
                "required": ["competition_code"],
            },
        ),
        Tool(
            name="get_bottler_index",
            description=(
                "Returns teams ranked by how often they drop points from winning "
                "half-time positions. Use for questions about collapses, "
                "second-half records, throwing away leads, or teams that cannot hold on."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "competition_code": {
                        "type": "string",
                        "description": "League code: PL, PD, BL1, or SA",
                        "enum": ["PL", "PD", "BL1", "SA"],
                    },
                    "min_matches_leading": {
                        "type": "integer",
                        "description": "Minimum half-time leads to include (default 3)",
                        "default": 3,
                    },
                },
                "required": ["competition_code"],
            },
        ),
        Tool(
            name="get_team_form",
            description=(
                "Returns a team's last N matches with results, goals and rolling "
                "form points. Use for questions about recent form, winning streaks, "
                "losing runs, or current momentum."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "team_name": {
                        "type": "string",
                        "description": "Team name or partial name (case-insensitive)",
                    },
                    "last_n_games": {
                        "type": "integer",
                        "description": "Number of recent games to return (default 5)",
                        "default": 5,
                    },
                },
                "required": ["team_name"],
            },
        ),
        Tool(
            name="get_head_to_head",
            description=(
                "Returns historical results between two specific teams. Use for "
                "questions about head to head records, rivalry history, or how "
                "two teams have faced each other."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "team_a": {
                        "type": "string",
                        "description": "First team name or partial name",
                    },
                    "team_b": {
                        "type": "string",
                        "description": "Second team name or partial name",
                    },
                },
                "required": ["team_a", "team_b"],
            },
        ),
        Tool(
            name="get_high_scoring_matches",
            description=(
                "Returns matches with the most total goals. Use for questions "
                "about exciting matches, goal fests, biggest wins, or dramatic scorelines."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "competition_code": {
                        "type": "string",
                        "description": "Optional league filter: PL, PD, BL1, or SA",
                    },
                    "min_goals": {
                        "type": "integer",
                        "description": "Minimum total goals in match (default 5)",
                        "default": 5,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of matches to return (default 10)",
                        "default": 10,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="get_season_summary",
            description=(
                "Returns high-level statistics for a competition this season — "
                "total goals, average goals per game, most common result, biggest win. "
                "Use for overview questions about a league."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "competition_code": {
                        "type": "string",
                        "description": "League code: PL, PD, BL1, or SA",
                        "enum": ["PL", "PD", "BL1", "SA"],
                    },
                },
                "required": ["competition_code"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "get_league_table":
        competition_code = arguments["competition_code"]
        sql = """
            SELECT position, team_name, played_games, won, draw, lost,
                   goals_for, goals_against, goal_difference, points,
                   points_per_game, win_percentage, goals_per_game,
                   conceded_per_game
            FROM marts.mart_league_table
            WHERE competition_code = %s
              AND season_id IN (
                  SELECT DISTINCT season_id
                  FROM marts.mart_league_table
                  WHERE competition_code = %s
              )
            ORDER BY position
        """
        result = query_to_json(sql, (competition_code, competition_code))

    elif name == "get_bottler_index":
        competition_code = arguments["competition_code"]
        min_matches = arguments.get("min_matches_leading", 3)
        sql = """
            SELECT team_name, competition_name, matches_leading_ht,
                   leads_dropped, drop_rate_pct
            FROM marts.mart_bottler_index
            WHERE competition_code = %s
              AND matches_leading_ht >= %s
            ORDER BY drop_rate_pct DESC
        """
        result = query_to_json(sql, (competition_code, min_matches))

    elif name == "get_team_form":
        team_name = "%" + arguments["team_name"] + "%"
        last_n = arguments.get("last_n_games", 5)
        sql = """
            SELECT m.match_date, m.competition_code,
                   CASE WHEN m.home_team_name ILIKE %s
                        THEN m.away_team_name
                        ELSE m.home_team_name END AS opponent,
                   CASE WHEN m.home_team_name ILIKE %s
                        THEN 'home' ELSE 'away' END AS venue,
                   m.home_goals, m.away_goals,
                   CASE WHEN m.home_team_name ILIKE %s
                        THEN m.home_goals ELSE m.away_goals END AS goals_scored,
                   CASE WHEN m.home_team_name ILIKE %s
                        THEN m.away_goals ELSE m.home_goals END AS goals_conceded,
                   m.result, f.form_points_last5
            FROM marts.mart_match_results m
            JOIN intermediate.int_team_form f
              ON m.match_id = f.match_id
             AND f.team_name ILIKE %s
            ORDER BY m.match_date DESC
            LIMIT %s
        """
        result = query_to_json(sql, (team_name, team_name, team_name, team_name, team_name, last_n))

    elif name == "get_head_to_head":
        team_a = "%" + arguments["team_a"] + "%"
        team_b = "%" + arguments["team_b"] + "%"
        sql = """
            SELECT match_date, competition_code, matchday,
                   home_team_name, away_team_name,
                   home_goals, away_goals, result, total_goals
            FROM marts.mart_match_results
            WHERE (home_team_name ILIKE %s AND away_team_name ILIKE %s)
               OR (home_team_name ILIKE %s AND away_team_name ILIKE %s)
            ORDER BY match_date DESC
        """
        result = query_to_json(sql, (team_a, team_b, team_b, team_a))

    elif name == "get_high_scoring_matches":
        min_goals = arguments.get("min_goals", 5)
        limit = arguments.get("limit", 10)
        competition_code = arguments.get("competition_code") or None
        sql = """
            SELECT match_date, competition_code, home_team_name,
                   away_team_name, home_goals, away_goals, total_goals, result
            FROM marts.mart_match_results
            WHERE total_goals >= %s
              AND (%s IS NULL OR competition_code = %s)
            ORDER BY total_goals DESC, match_date DESC
            LIMIT %s
        """
        result = query_to_json(sql, (min_goals, competition_code, competition_code, limit))

    elif name == "get_season_summary":
        competition_code = arguments["competition_code"]
        sql = """
            SELECT
                competition_code,
                COUNT(*)                                                  AS total_matches,
                SUM(total_goals)                                          AS total_goals,
                ROUND(AVG(total_goals), 2)                                AS avg_goals_per_game,
                SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END)         AS home_wins,
                SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END)         AS draws,
                SUM(CASE WHEN result = 'away' THEN 1 ELSE 0 END)         AS away_wins,
                MAX(total_goals)                                          AS highest_scoring_match,
                SUM(CASE WHEN is_high_scoring THEN 1 ELSE 0 END)         AS high_scoring_matches
            FROM marts.mart_match_results
            WHERE competition_code = %s
            GROUP BY competition_code
        """
        result = query_to_json(sql, (competition_code,))

    else:
        result = json.dumps({"error": f"Unknown tool: {name}"})

    return [TextContent(type="text", text=result)]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
