import os
import time
from decimal import Decimal

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "football_db"),
    "user": os.getenv("DB_USER", "football"),
    "password": os.getenv("DB_PASSWORD", "football"),
}

# ---------------------------------------------------------------------------
# Anthropic client
# ---------------------------------------------------------------------------

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a football match reporter writing concise match reports for a "
    "football analytics platform. Write in an engaging but factual style. "
    "Keep reports to 3-4 sentences. Focus on the key moments and what the "
    "result means."
)

FEW_SHOT_EXAMPLES = """\
Here are two example match reports to guide your style:

Example 1:
Match: Arsenal 2-0 Chelsea, HT: 1-0, Matchday 15, PL
Report: Arsenal secured a comfortable north London derby victory with a composed \
performance at the Emirates. Leading through a first-half goal, they doubled their \
advantage after the break to extend their unbeaten run. Chelsea struggled to create \
meaningful chances and remain five points adrift of the top four.

Example 2:
Match: Brentford 3-3 Fulham, HT: 1-2, Matchday 8, PL
Report: A pulsating west London derby ended all square in a match that had everything. \
Fulham led at the break but Brentford mounted a remarkable comeback, only for Fulham \
to snatch a late equaliser. Both managers will have mixed feelings — a point each but \
two dropped in different ways.

Now write a report for this match:
"""


def build_user_message(match: dict) -> str:
    ht_leader = match["ht_leader"] or "none"
    return (
        f"{FEW_SHOT_EXAMPLES}"
        f"Match: {match['home_team_name']} {match['home_goals']}-{match['away_goals']} "
        f"{match['away_team_name']}, "
        f"HT: {match['home_goals_ht']}-{match['away_goals_ht']}, "
        f"Matchday {match['matchday']}, {match['competition_code']}\n"
        f"Half-time leader: {ht_leader}\n"
        f"HT lead dropped: {match['is_ht_lead_dropped']}\n"
        f"High scoring: {match['is_high_scoring']}\n"
        f"Write the match report:"
    )


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_matches(competition_code: str, limit: int = 5) -> list[dict]:
    sql = """
        SELECT match_id, match_date, matchday, competition_code,
               competition_name, home_team_name, away_team_name,
               home_goals, away_goals, home_goals_ht, away_goals_ht,
               total_goals, result, ht_leader, is_ht_lead_dropped,
               is_high_scoring
        FROM marts.mart_match_results
        WHERE competition_code = %s
        ORDER BY match_date DESC
        LIMIT %s
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (competition_code, limit))
            rows = cur.fetchall()
    # Convert any Decimal values to float
    return [
        {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Narrative generation
# ---------------------------------------------------------------------------

def generate_narrative(match: dict, temperature: float = 0.8) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=400,
        temperature=temperature,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_user_message(match)}],
    )
    return response.content[0].text.strip()


# ---------------------------------------------------------------------------
# Run narratives
# ---------------------------------------------------------------------------

def run_narratives(
    competition_code: str = "PL",
    limit: int = 5,
    temperature: float = 0.8,
) -> list[tuple[dict, str]]:
    matches = fetch_matches(competition_code, limit)
    results = []

    print(f"\n{'='*70}")
    print(f"  MATCH NARRATIVES — {competition_code} (temperature={temperature})")
    print(f"{'='*70}\n")

    for i, match in enumerate(matches, 1):
        header = (
            f"{match['home_team_name']} {match['home_goals']}-{match['away_goals']} "
            f"{match['away_team_name']}"
        )
        print(f"[{i}/{len(matches)}] {match['match_date']} | MD{match['matchday']} | {header}")

        narrative = generate_narrative(match, temperature=temperature)
        print(f"{narrative}\n")

        results.append((match, narrative))

        if i < len(matches):
            time.sleep(1)

    return results


# ---------------------------------------------------------------------------
# Temperature comparison
# ---------------------------------------------------------------------------

def compare_temperatures(match: dict) -> None:
    temperatures = [0.3, 0.7, 1.0]
    header = (
        f"{match['home_team_name']} {match['home_goals']}-{match['away_goals']} "
        f"{match['away_team_name']}"
    )

    print(f"\n{'='*70}")
    print(f"  TEMPERATURE COMPARISON")
    print(f"  {match['match_date']} | MD{match['matchday']} | {header}")
    print(f"{'='*70}\n")

    for temp in temperatures:
        print(f"--- temperature={temp} ---")
        narrative = generate_narrative(match, temperature=temp)
        print(f"{narrative}\n")
        time.sleep(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results = run_narratives(competition_code="PL", limit=5, temperature=0.8)

    if results:
        first_match = results[0][0]
        compare_temperatures(first_match)
