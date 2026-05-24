"""
Shared tool logic used by both the Anthropic agent (football_agent.py) and
the MCP server (mcp/football_mcp_server.py).

ToolHandlers.SCHEMAS  — tool definitions in Anthropic API format (input_schema).
                        The MCP server converts these to mcp.types.Tool objects.
ToolHandlers.<name>() — each handler takes plain Python args and returns
                        list[dict] (or dict for resolve_season). Callers
                        serialise the result as their transport requires.
"""
import re
from datetime import date
from decimal import Decimal

import psycopg2
import psycopg2.extras

from config.db import get_conn


_SEASON_PARAM = {
    "season_start_year": {
        "type": "integer",
        "description": "Season start year (e.g. 2024 for 2024/25). Get from resolve_season.",
    }
}

_SEASON_NOTE = (
    "Accepts an optional season_start_year (e.g. 2024 for 2024/25). "
    "Call resolve_season first to convert a natural-language season reference "
    "into the correct season_start_year."
)


class ToolHandlers:

    # -----------------------------------------------------------------------
    # Shared tool schemas (Anthropic API format; MCP server converts as needed)
    # -----------------------------------------------------------------------

    SCHEMAS: list[dict] = [
        {
            "name": "resolve_season",
            "description": (
                "Converts a natural-language season reference into a season_start_year integer. "
                "Always call this first when the user mentions a season. "
                "Examples: 'this season' → current season, 'last season' → one year back, "
                "'2 seasons ago' → two years back, '2023-2024' → 2023."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "reference": {
                        "type": "string",
                        "description": (
                            "Season reference string. Examples: 'this season', "
                            "'last season', '2 seasons ago', '2023-2024', '2024/25', '2024'"
                        ),
                    },
                },
                "required": ["reference"],
            },
        },
        {
            "name": "get_league_table",
            "description": (
                "Returns league standings with points, wins, goals and "
                "performance metrics. Use for questions about league position, "
                "title races, who is top, who is bottom, or points gaps. " + _SEASON_NOTE
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "competition_code": {
                        "type": "string",
                        "description": "League code: PL, PD, BL1, or SA",
                        "enum": ["PL", "PD", "BL1", "SA"],
                    },
                    **_SEASON_PARAM,
                },
                "required": ["competition_code"],
            },
        },
        {
            "name": "get_bottler_index",
            "description": (
                "Returns teams ranked by how often they drop points from winning "
                "half-time positions. Use for questions about collapses, "
                "second-half records, throwing away leads, or teams that cannot hold on. "
                + _SEASON_NOTE
            ),
            "input_schema": {
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
                    **_SEASON_PARAM,
                },
                "required": ["competition_code"],
            },
        },
        {
            "name": "get_team_form",
            "description": (
                "Returns a team's last N matches with results, goals and rolling "
                "form points. Use for questions about recent form, winning streaks, "
                "losing runs, or current momentum. " + _SEASON_NOTE
            ),
            "input_schema": {
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
                    **_SEASON_PARAM,
                },
                "required": ["team_name"],
            },
        },
        {
            "name": "get_head_to_head",
            "description": (
                "Returns historical results between two specific teams. Use for "
                "questions about head to head records, rivalry history, or how "
                "two teams have faced each other. " + _SEASON_NOTE
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "team_a": {"type": "string", "description": "First team name or partial name"},
                    "team_b": {"type": "string", "description": "Second team name or partial name"},
                    "season_start_year": {
                        "type": "integer",
                        "description": "Season start year (e.g. 2024 for 2024/25). Get from resolve_season. Omit to search all seasons.",
                    },
                },
                "required": ["team_a", "team_b"],
            },
        },
        {
            "name": "get_high_scoring_matches",
            "description": (
                "Returns matches with the most total goals. Use for questions "
                "about exciting matches, goal fests, biggest wins, or dramatic scorelines. "
                + _SEASON_NOTE
            ),
            "input_schema": {
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
                    **_SEASON_PARAM,
                },
                "required": [],
            },
        },
        {
            "name": "get_season_summary",
            "description": (
                "Returns high-level statistics for a competition — "
                "total goals, average goals per game, most common result, biggest win. "
                "Use for overview questions about a league. " + _SEASON_NOTE
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "competition_code": {
                        "type": "string",
                        "description": "League code: PL, PD, BL1, or SA",
                        "enum": ["PL", "PD", "BL1", "SA"],
                    },
                    **_SEASON_PARAM,
                },
                "required": ["competition_code"],
            },
        },
        {
            "name": "search_match_reports",
            "description": (
                "Searches match narratives and reports for contextual information "
                "about specific matches, team performances, or match atmospheres. "
                "Use when the question asks about how a match was played, team "
                "momentum, or narrative context beyond raw stats."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query"},
                    "limit": {"type": "integer", "description": "Number of results to return (default 5)", "default": 5},
                },
                "required": ["query"],
            },
        },
    ]

    # -----------------------------------------------------------------------
    # Season helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def current_season_year() -> int:
        today = date.today()
        return today.year if today.month >= 7 else today.year - 1

    @staticmethod
    def parse_season_reference(reference: str) -> int:
        current = ToolHandlers.current_season_year()
        ref = reference.strip().lower()

        if ref in ("this season", "current season", "current", "this year"):
            return current
        if ref in ("last season", "previous season"):
            return current - 1
        if ref in ("next season",):
            return current + 1

        m = re.match(r"(\d+)\s+seasons?\s+ago", ref)
        if m:
            return current - int(m.group(1))

        # "2023-2024" or "2023/24"
        m = re.match(r"(\d{4})[/-]\d{2,4}", ref)
        if m:
            return int(m.group(1))

        # bare year "2024"
        m = re.match(r"(\d{4})", ref)
        if m:
            return int(m.group(1))

        return current

    @staticmethod
    def _season_id_filter(competition_code: str, season_start_year: int) -> tuple[str, list]:
        """WHERE fragment for tables with opaque season_id values.

        mart_league_table and mart_bottler_index store an internal season_id
        (not a year), so we resolve it via match dates in mart_match_results.
        Season YYYY/YY+1 runs July 1 YYYY – June 30 YY+1.
        """
        clause = """AND season_id IN (
            SELECT DISTINCT season_id FROM marts.mart_match_results
            WHERE competition_code = %s
              AND match_date >= MAKE_DATE(%s, 7, 1)
              AND match_date < MAKE_DATE(%s, 7, 1)
        )"""
        return clause, [competition_code, season_start_year, season_start_year + 1]

    @staticmethod
    def _season_date_filter(season_start_year: int | None) -> tuple[str, list]:
        """WHERE fragment for tables filtered directly by match_date."""
        if season_start_year is None:
            return "", []
        clause = "AND match_date >= MAKE_DATE(%s, 7, 1) AND match_date < MAKE_DATE(%s, 7, 1)"
        return clause, [season_start_year, season_start_year + 1]

    # -----------------------------------------------------------------------
    # DB helper
    # -----------------------------------------------------------------------

    @staticmethod
    def _query_db(sql: str, params: tuple) -> list[dict]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [
            {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
            for row in rows
        ]

    # -----------------------------------------------------------------------
    # Tool handlers
    # -----------------------------------------------------------------------

    @staticmethod
    def resolve_season(reference: str) -> dict:
        year = ToolHandlers.parse_season_reference(reference)
        label = f"{year}/{str(year + 1)[-2:]}"
        return {"season_start_year": year, "season_label": label}

    @staticmethod
    def get_league_table(
        competition_code: str,
        season_start_year: int | None = None,
    ) -> list[dict]:
        if season_start_year is not None:
            sid_clause, sid_params = ToolHandlers._season_id_filter(competition_code, season_start_year)
            return ToolHandlers._query_db(f"""
                SELECT position, team_name, played_games, won, draw, lost,
                       goals_for, goals_against, goal_difference, points,
                       points_per_game, win_percentage, goals_per_game,
                       conceded_per_game
                FROM marts.mart_league_table
                WHERE competition_code = %s
                {sid_clause}
                ORDER BY position
            """, (competition_code, *sid_params))
        return ToolHandlers._query_db("""
            SELECT position, team_name, played_games, won, draw, lost,
                   goals_for, goals_against, goal_difference, points,
                   points_per_game, win_percentage, goals_per_game,
                   conceded_per_game
            FROM marts.mart_league_table
            WHERE competition_code = %s
              AND season_id = (SELECT MAX(season_id) FROM marts.mart_league_table WHERE competition_code = %s)
            ORDER BY position
        """, (competition_code, competition_code))

    @staticmethod
    def get_bottler_index(
        competition_code: str,
        min_matches_leading: int = 3,
        season_start_year: int | None = None,
    ) -> list[dict]:
        if season_start_year is not None:
            sid_clause, sid_params = ToolHandlers._season_id_filter(competition_code, season_start_year)
            return ToolHandlers._query_db(f"""
                SELECT team_name, competition_name, matches_leading_ht,
                       leads_dropped, drop_rate_pct
                FROM marts.mart_bottler_index
                WHERE competition_code = %s
                  AND matches_leading_ht >= %s
                {sid_clause}
                ORDER BY drop_rate_pct DESC
            """, (competition_code, min_matches_leading, *sid_params))
        return ToolHandlers._query_db("""
            SELECT team_name, competition_name, matches_leading_ht,
                   leads_dropped, drop_rate_pct
            FROM marts.mart_bottler_index
            WHERE competition_code = %s
              AND matches_leading_ht >= %s
              AND season_id = (SELECT MAX(season_id) FROM marts.mart_bottler_index WHERE competition_code = %s)
            ORDER BY drop_rate_pct DESC
        """, (competition_code, min_matches_leading, competition_code))

    @staticmethod
    def get_team_form(
        team_name: str,
        last_n_games: int = 5,
        season_start_year: int | None = None,
    ) -> list[dict]:
        team = f"%{team_name}%"
        season_clause, season_params = ToolHandlers._season_date_filter(season_start_year)
        # form_points_last5 is computed inline via window function so we don't
        # depend on the intermediate.int_team_form dbt model being materialised.
        return ToolHandlers._query_db(f"""
            WITH team_matches AS (
                SELECT
                    m.match_date, m.competition_code,
                    CASE WHEN m.home_team_name ILIKE %s THEN m.away_team_name ELSE m.home_team_name END AS opponent,
                    CASE WHEN m.home_team_name ILIKE %s THEN 'home' ELSE 'away' END AS venue,
                    m.home_goals, m.away_goals,
                    CASE WHEN m.home_team_name ILIKE %s THEN m.home_goals ELSE m.away_goals END AS goals_scored,
                    CASE WHEN m.home_team_name ILIKE %s THEN m.away_goals ELSE m.home_goals END AS goals_conceded,
                    m.result,
                    CASE
                        WHEN m.home_team_name ILIKE %s AND m.result = 'home' THEN 3
                        WHEN m.away_team_name ILIKE %s AND m.result = 'away' THEN 3
                        WHEN m.result = 'draw' THEN 1
                        ELSE 0
                    END AS match_pts
                FROM marts.mart_match_results m
                WHERE (m.home_team_name ILIKE %s OR m.away_team_name ILIKE %s)
                {season_clause}
            ),
            with_form AS (
                SELECT match_date, competition_code, opponent, venue,
                       home_goals, away_goals, goals_scored, goals_conceded, result,
                       SUM(match_pts) OVER (ORDER BY match_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS form_points_last5
                FROM team_matches
            )
            SELECT * FROM with_form
            ORDER BY match_date DESC
            LIMIT %s
        """, (team, team, team, team, team, team, team, team, *season_params, last_n_games))

    @staticmethod
    def get_head_to_head(
        team_a: str,
        team_b: str,
        season_start_year: int | None = None,
    ) -> list[dict]:
        a, b = f"%{team_a}%", f"%{team_b}%"
        season_clause, season_params = ToolHandlers._season_date_filter(season_start_year)
        return ToolHandlers._query_db(f"""
            SELECT match_date, competition_code, matchday,
                   home_team_name, away_team_name,
                   home_goals, away_goals, result, total_goals
            FROM marts.mart_match_results
            WHERE (
                (home_team_name ILIKE %s AND away_team_name ILIKE %s)
                OR (home_team_name ILIKE %s AND away_team_name ILIKE %s)
            )
            {season_clause}
            ORDER BY match_date DESC
        """, (a, b, b, a, *season_params))

    @staticmethod
    def get_high_scoring_matches(
        competition_code: str | None = None,
        min_goals: int = 5,
        limit: int = 10,
        season_start_year: int | None = None,
    ) -> list[dict]:
        season_clause, season_params = ToolHandlers._season_date_filter(season_start_year)
        return ToolHandlers._query_db(f"""
            SELECT match_date, competition_code, home_team_name,
                   away_team_name, home_goals, away_goals, total_goals, result
            FROM marts.mart_match_results
            WHERE total_goals >= %s
              AND (%s IS NULL OR competition_code = %s)
            {season_clause}
            ORDER BY total_goals DESC, match_date DESC
            LIMIT %s
        """, (min_goals, competition_code, competition_code, *season_params, limit))

    @staticmethod
    def search_match_reports(query: str, limit: int = 5) -> list[dict]:
        from agent.rag_retrieval import retrieve_relevant_chunks
        return retrieve_relevant_chunks(query, limit=limit)

    @staticmethod
    def get_season_summary(
        competition_code: str,
        season_start_year: int | None = None,
    ) -> list[dict]:
        season_clause, season_params = ToolHandlers._season_date_filter(season_start_year)
        return ToolHandlers._query_db(f"""
            SELECT
                competition_code,
                COUNT(*)                                             AS total_matches,
                SUM(total_goals)                                     AS total_goals,
                ROUND(AVG(total_goals), 2)                           AS avg_goals_per_game,
                SUM(CASE WHEN result = 'home' THEN 1 ELSE 0 END)    AS home_wins,
                SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END)    AS draws,
                SUM(CASE WHEN result = 'away' THEN 1 ELSE 0 END)    AS away_wins,
                MAX(total_goals)                                     AS highest_scoring_match,
                SUM(CASE WHEN is_high_scoring THEN 1 ELSE 0 END)    AS high_scoring_matches
            FROM marts.mart_match_results
            WHERE competition_code = %s
            {season_clause}
            GROUP BY competition_code
        """, (competition_code, *season_params))
