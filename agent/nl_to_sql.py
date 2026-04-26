import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import anthropic
import psycopg2
import psycopg2.extras
import sqlparse
import re
from dotenv import load_dotenv
from decimal import Decimal

from config import DB_CONFIG

load_dotenv()

# ---------------------------------------------------------------------------
# Anthropic client
# ---------------------------------------------------------------------------

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ---------------------------------------------------------------------------
# Schema context
# ---------------------------------------------------------------------------

SCHEMA_CONTEXT = """
Available tables and columns:

marts.mart_league_table
  - competition_code TEXT        -- league identifier: PL, PD, BL1, SA
  - season_id        INTEGER
  - competition_name TEXT
  - position         INTEGER
  - team_name        TEXT
  - played_games     INTEGER
  - won              INTEGER
  - draw             INTEGER
  - lost             INTEGER
  - goals_for        INTEGER
  - goals_against    INTEGER
  - goal_difference  INTEGER
  - points           INTEGER
  - points_per_game  NUMERIC
  - win_percentage   NUMERIC
  - goals_per_game   NUMERIC
  - conceded_per_game NUMERIC

marts.mart_match_results
  - match_id         INTEGER
  - match_date       DATE
  - matchday         INTEGER
  - season_id        INTEGER
  - competition_code TEXT        -- PL, PD, BL1, SA
  - competition_name TEXT
  - home_team_name   TEXT
  - away_team_name   TEXT
  - home_goals       INTEGER
  - away_goals       INTEGER
  - home_goals_ht    INTEGER
  - away_goals_ht    INTEGER
  - total_goals      INTEGER
  - goal_diff        INTEGER
  - result           TEXT        -- 'home', 'away', or 'draw'
  - ht_leader        TEXT        -- 'home', 'away', or 'draw'
  - is_high_scoring  BOOLEAN
  - is_ht_lead_dropped BOOLEAN

marts.mart_bottler_index
  - competition_code   TEXT
  - competition_name   TEXT
  - season_id          INTEGER
  - team_id            INTEGER
  - team_name          TEXT
  - matches_leading_ht INTEGER
  - leads_dropped      INTEGER
  - drop_rate_pct      NUMERIC
""".strip()

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

NL_TO_SQL_SYSTEM = """You are a PostgreSQL expert. Translate natural language questions into SQL queries against a football analytics database.

Rules:
- Only use tables and columns defined in the schema
- Always prefix table names with the marts. schema
- Always include LIMIT 50 unless the question asks for all results or a specific number
- Use lowercase SQL keywords
- Return ONLY the raw SQL query
- No markdown, no code fences, no explanation
- No semicolon at the end
- For queries comparing home vs away performance, always verify: home wins come from rows where result = 'home' AND the team is home_team_name. Away wins come from rows where result = 'away' AND the team is away_team_name.
- For any aggregation query, add a comment in the SQL explaining what each COUNT or SUM is measuring"""

# ---------------------------------------------------------------------------
# SQL validation
# ---------------------------------------------------------------------------

DANGEROUS_PATTERNS = [
    r"\bdrop\b",
    r"\bdelete\b",
    r"\binsert\b",
    r"\bupdate\b",
    r"\btruncate\b",
    r"\balter\b",
    r"\bcreate\b",
    r"\bpg_",
]


def validate_sql(sql: str) -> tuple[bool, str]:
    import re

    if not sql or not sql.strip():
        return False, "Empty query"

    statements = sqlparse.parse(sql.strip())
    if not statements:
        return False, "Could not parse SQL"

    statement = statements[0]
    if statement.get_type() != "SELECT":
        return False, f"Only SELECT statements are allowed (got {statement.get_type()})"

    sql_lower = sql.lower()

    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, sql_lower):
            return False, f"Forbidden keyword detected: {pattern}"

    if "marts." not in sql_lower:
        return False, "Query must reference at least one marts. table"

    return True, ""

# ---------------------------------------------------------------------------
# Few-shot examples
# ---------------------------------------------------------------------------

FEW_SHOT_EXAMPLES = """Example 1:
Question: which team scored the most goals this season in the Premier League?
SQL: select team_name, goals_for from marts.mart_league_table where competition_code = 'PL' order by goals_for desc limit 1

Example 2:
Question: how many matches ended as draws in the Bundesliga?
SQL: select count(*) as total_draws from marts.mart_match_results where competition_code = 'BL1' and result = 'draw'

Example 3:
Question: which teams have a drop rate above 50 percent?
SQL: select team_name, competition_code, drop_rate_pct from marts.mart_bottler_index where drop_rate_pct > 50 order by drop_rate_pct desc limit 50

Example 4:
Question: which teams won more away games than home games?
SQL: select team_name,
  sum(case when result = 'home' then 1 else 0 end) as home_wins, -- wins when this team is the home side
  sum(case when result = 'away' then 1 else 0 end) as away_wins  -- wins when this team is the away side
from (
  select home_team_name as team_name, result
  from marts.mart_match_results
  union all
  select away_team_name as team_name, result
  from marts.mart_match_results
) all_matches
group by team_name
having sum(case when result = 'away' then 1 else 0 end) >
       sum(case when result = 'home' then 1 else 0 end)
order by away_wins desc
limit 50"""

# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

def generate_sql(question: str, error_context: str | None = None) -> str:
    user_message = (
        f"{FEW_SHOT_EXAMPLES}\n\n"
        f"Schema:\n{SCHEMA_CONTEXT}\n\n"
        f"Question: {question}\n"
        f"SQL:"
    )

    if error_context:
        user_message += (
            f"\n\nPrevious attempt failed: {error_context}\n"
            f"Please fix the SQL and try again."
        )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        temperature=0,
        system=NL_TO_SQL_SYSTEM,
        messages=[{"role": "user", "content": user_message}],
    )

    sql = response.content[0].text.strip()

    # Strip accidental markdown code fences
    if sql.startswith("```"):
        sql = sql.split("\n", 1)[-1]
    if sql.endswith("```"):
        sql = sql.rsplit("```", 1)[0]

    return sql.strip()


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

def execute_sql(sql: str) -> tuple[bool, list, str]:
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return (
            True,
            [{k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()} for row in rows],
            "",
        )
    except Exception as e:
        return False, [], str(e)


# ---------------------------------------------------------------------------
# Result interpretation
# ---------------------------------------------------------------------------

def interpret_results(question: str, sql: str, results: list) -> str:
    import json

    if not results:
        return "No data found for that question."

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=300,
        temperature=0,
        system=(
            "You are a football analyst. Answer the user's question using only "
            "the provided data. Be concise — 2-4 sentences. Always cite specific "
            "numbers from the data."
        ),
        messages=[{
            "role": "user",
            "content": (
                f"Question: {question}\n"
                f"SQL used: {sql}\n"
                f"Results: {json.dumps(results[:20], default=str)}\n"
                f"Answer:"
            ),
        }],
    )
    return response.content[0].text.strip()


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def nl_to_sql_pipeline(question: str, max_retries: int = 2) -> dict:
    sql = generate_sql(question)
    error_context = None

    for attempt in range(max_retries + 1):
        if attempt > 0:
            sql = generate_sql(question, error_context)

        # Validate
        valid, validation_error = validate_sql(sql)
        if not valid:
            error_context = f"Validation failed: {validation_error}. Generated SQL was: {sql}"
            continue

        # Execute
        success, rows, exec_error = execute_sql(sql)
        if not success:
            error_context = f"Execution failed: {exec_error}. Generated SQL was: {sql}"
            continue

        # Both passed
        answer = interpret_results(question, sql, rows)
        return {
            "question": question,
            "sql": sql,
            "success": True,
            "row_count": len(rows),
            "answer": answer,
            "attempts": attempt + 1,
        }

    return {
        "question": question,
        "sql": sql,
        "success": False,
        "row_count": 0,
        "answer": f"Failed after {max_retries + 1} attempt(s): {error_context}",
        "attempts": max_retries + 1,
    }


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

DEMO_QUESTIONS = [
    "Which team had the best away record in the Premier League this season?",
    "How many total goals were scored across all leagues in our database?",
    "Which Bundesliga team conceded the fewest goals?",
    "What percentage of Premier League matches ended as home wins?",
    "Which team had the biggest positive goal difference in La Liga?",
    "How many matches had a half-time lead that was eventually dropped across all competitions?",
    "Which teams won more away games than home games this season?",
    "What was the average number of goals per game in matches where the half-time leader dropped their lead?",
]


def run_demo() -> None:
    for i, question in enumerate(DEMO_QUESTIONS, 1):
        print(f"\n{'='*70}")
        print(f"Q{i}: {question}")
        print(f"{'='*70}")

        result = nl_to_sql_pipeline(question)

        print(f"SQL      : {result['sql']}")
        print(f"Rows     : {result['row_count']}")
        print(f"Attempts : {result['attempts']}")
        print(f"\nAnswer   : {result['answer']}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_demo()
