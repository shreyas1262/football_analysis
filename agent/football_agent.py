import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import anthropic
from dotenv import load_dotenv

from nl_to_sql import nl_to_sql_pipeline
from rag_retrieval import build_rag_context, retrieve_relevant_chunks

load_dotenv()

# ---------------------------------------------------------------------------
# Anthropic client
# ---------------------------------------------------------------------------

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "get_league_table",
        "description": (
            "Returns current league standings with points, wins, goals and "
            "performance metrics. Use for questions about league position, "
            "title races, who is top, who is bottom, or points gaps."
        ),
        "input_schema": {
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
                },
            },
            "required": ["competition_code"],
        },
    },
    {
        "name": "get_bottler_index",
        "description": (
            "Returns teams ranked by how often they drop points from winning "
            "half-time positions. Use for questions about collapses, "
            "second-half records, throwing away leads, or teams that cannot hold on."
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
                },
            },
            "required": ["competition_code"],
        },
    },
    {
        "name": "get_team_form",
        "description": (
            "Returns a team's last N matches with results, goals and rolling "
            "form points. Use for questions about recent form, winning streaks, "
            "losing runs, or current momentum."
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
                },
            },
            "required": ["team_name"],
        },
    },
    {
        "name": "get_head_to_head",
        "description": (
            "Returns historical results between two specific teams. Use for "
            "questions about head to head records, rivalry history, or how "
            "two teams have faced each other."
        ),
        "input_schema": {
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
    },
    {
        "name": "get_high_scoring_matches",
        "description": (
            "Returns matches with the most total goals. Use for questions "
            "about exciting matches, goal fests, biggest wins, or dramatic scorelines."
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
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of matches to return (default 10)",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_season_summary",
        "description": (
            "Returns high-level statistics for a competition this season — "
            "total goals, average goals per game, most common result, biggest win. "
            "Use for overview questions about a league."
        ),
        "input_schema": {
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
                "query": {
                    "type": "string",
                    "description": "The search query",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of results to return (default 5)",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "nl_to_sql",
        "description": (
            "Generates and executes a custom SQL query for any football question "
            "not covered by the other tools. Use this as a fallback when the specific "
            "predefined tools cannot answer the question. Examples: cross-table analysis, "
            "custom aggregations, questions about specific dates or matchdays, "
            "anything requiring a JOIN between tables."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The natural language question to convert to SQL",
                },
            },
            "required": ["question"],
        },
    },
]

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a football analytics assistant with access to a database covering the Premier League, La Liga, Serie A, and Bundesliga for the 2024/25 season.

## What you must always do
- Use the provided tools to retrieve data before answering any statistical question
- Cite which tool result your answer is based on
- If a question needs both statistics and match context, use both a data tool AND search_match_reports
- Keep answers concise — 3-5 sentences unless asked for more detail

## What you must never do
- Answer statistical questions from training memory
- Invent match results, scores, or statistics
- Make claims you cannot attribute to a tool result"""

# ---------------------------------------------------------------------------
# Tool routing
# ---------------------------------------------------------------------------

def query_db(sql: str, params: tuple) -> list[dict]:
    from decimal import Decimal
    import psycopg2, psycopg2.extras
    from config import DB_CONFIG
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [
        {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]


def call_tool(name: str, inputs: dict) -> list[dict] | str:
    if name == "get_league_table":
        code = inputs["competition_code"]
        return query_db("""
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
        """, (code, code))

    elif name == "get_bottler_index":
        code = inputs["competition_code"]
        min_matches = inputs.get("min_matches_leading", 3)
        return query_db("""
            SELECT team_name, competition_name, matches_leading_ht,
                   leads_dropped, drop_rate_pct
            FROM marts.mart_bottler_index
            WHERE competition_code = %s
              AND matches_leading_ht >= %s
            ORDER BY drop_rate_pct DESC
        """, (code, min_matches))

    elif name == "get_team_form":
        team = "%" + inputs["team_name"] + "%"
        last_n = inputs.get("last_n_games", 5)
        return query_db("""
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
        """, (team, team, team, team, team, last_n))

    elif name == "get_head_to_head":
        a = "%" + inputs["team_a"] + "%"
        b = "%" + inputs["team_b"] + "%"
        return query_db("""
            SELECT match_date, competition_code, matchday,
                   home_team_name, away_team_name,
                   home_goals, away_goals, result, total_goals
            FROM marts.mart_match_results
            WHERE (home_team_name ILIKE %s AND away_team_name ILIKE %s)
               OR (home_team_name ILIKE %s AND away_team_name ILIKE %s)
            ORDER BY match_date DESC
        """, (a, b, b, a))

    elif name == "get_high_scoring_matches":
        min_goals = inputs.get("min_goals", 5)
        limit = inputs.get("limit", 10)
        code = inputs.get("competition_code") or None
        return query_db("""
            SELECT match_date, competition_code, home_team_name,
                   away_team_name, home_goals, away_goals, total_goals, result
            FROM marts.mart_match_results
            WHERE total_goals >= %s
              AND (%s IS NULL OR competition_code = %s)
            ORDER BY total_goals DESC, match_date DESC
            LIMIT %s
        """, (min_goals, code, code, limit))

    elif name == "get_season_summary":
        code = inputs["competition_code"]
        return query_db("""
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
            GROUP BY competition_code
        """, (code,))

    elif name == "search_match_reports":
        chunks = retrieve_relevant_chunks(
            inputs["query"],
            limit=inputs.get("limit", 5),
        )
        return chunks

    elif name == "nl_to_sql":
        result = nl_to_sql_pipeline(inputs["question"])
        return result

    return []


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

def run_agent(question: str, verbose: bool = True) -> str:
    import json

    messages = [{"role": "user", "content": question}]

    for iteration in range(10):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            return next(
                b.text for b in response.content if hasattr(b, "text")
            )

        if response.stop_reason == "tool_use":
            # Append assistant turn (contains tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                if verbose:
                    print(f"  [tool] {block.name}({json.dumps(block.input)})")

                result = call_tool(block.name, block.input)

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })

            messages.append({"role": "user", "content": tool_results})

    return "Agent reached iteration limit without a final answer."


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

DEMO_QUESTIONS = [
    "Who are the top 3 biggest bottlers in the Bundesliga this season?",
    "How did Liverpool perform against the top 6 this season? Were there any surprise results?",
    "Which league had the most goals per game on average this season?",
    "Tell me about the most dramatic matches in the Premier League — any games with big comebacks or late drama?",
    "Compare Arsenal and Liverpool — who had the better season based on the data?",
]


def run_demo() -> None:
    for i, question in enumerate(DEMO_QUESTIONS, 1):
        print(f"\n{'='*70}")
        print(f"  Q{i}: {question}")
        print(f"{'='*70}")
        answer = run_agent(question, verbose=True)
        print(f"\n{answer}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_demo()
