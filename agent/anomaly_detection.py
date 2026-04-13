import json
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
# Prompts and tool schema
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a football data analyst identifying statistically unusual team "
    "performances. Be precise and data-driven. Only flag genuine anomalies — "
    "not just good or bad performances, but ones that deviate significantly "
    "from what the data would predict. Always cite specific numbers."
)

ANOMALY_TOOL = {
    "name": "flag_anomaly",
    "description": "Report whether a team's performance is statistically anomalous relative to their league.",
    "input_schema": {
        "type": "object",
        "properties": {
            "is_anomalous": {
                "type": "boolean",
                "description": "Whether the team's performance is genuinely anomalous",
            },
            "anomaly_type": {
                "type": "string",
                "enum": [
                    "overperforming",
                    "underperforming",
                    "defensive_outlier",
                    "attacking_outlier",
                    "none",
                ],
                "description": "The category of anomaly, or 'none' if not anomalous",
            },
            "severity": {
                "type": "string",
                "enum": ["low", "medium", "high", "none"],
                "description": "How severe the anomaly is relative to the league distribution",
            },
            "key_metric": {
                "type": "string",
                "description": "The single most important metric driving the anomaly flag",
            },
            "explanation": {
                "type": "string",
                "description": "2-3 sentences explaining the anomaly with specific numbers cited",
            },
        },
        "required": [
            "is_anomalous",
            "anomaly_type",
            "severity",
            "key_metric",
            "explanation",
        ],
    },
}


def build_user_message(team_stats: dict, league_stats: dict) -> str:
    return (
        "Analyse this team's statistics against their league averages. "
        "Think step by step:\n"
        "1. How does their points per game compare to the league average and standard deviation?\n"
        "2. Is their goals scored or conceded rate unusual relative to their points total?\n"
        "3. Is there a meaningful gap between their performance metrics that suggests "
        "over or underperformance?\n\n"
        "Then give your verdict: is this team anomalous, and if so why?\n\n"
        f"Team: {team_stats['team_name']}\n"
        f"Competition: {team_stats['competition_name']}\n"
        f"Games played: {team_stats['played_games']}\n\n"
        "Team stats:\n"
        f"- Points per game: {team_stats['points_per_game']:.2f}\n"
        f"- Win percentage: {team_stats['win_percentage']:.1f}%\n"
        f"- Goals per game: {team_stats['goals_per_game']:.2f}\n"
        f"- Conceded per game: {team_stats['conceded_per_game']:.2f}\n"
        f"- Goal difference: {team_stats['goal_difference']}\n\n"
        "League averages:\n"
        f"- Avg points per game: {league_stats['league_avg_ppg']:.2f}\n"
        f"- Avg goals per game: {league_stats['league_avg_goals']:.2f}\n"
        f"- Avg conceded per game: {league_stats['league_avg_conceded']:.2f}\n"
        f"- Std deviation PPG: {league_stats['stddev_ppg']:.2f}\n\n"
        "Analysis:"
    )


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_team_stats(competition_code: str) -> list[dict]:
    sql = """
        SELECT
            t.team_name,
            t.competition_code,
            t.competition_name,
            t.played_games,
            t.points,
            t.points_per_game,
            t.win_percentage,
            t.goals_per_game,
            t.conceded_per_game,
            t.goal_difference,
            AVG(t.points_per_game) OVER (
                PARTITION BY t.competition_code
            ) AS league_avg_ppg,
            AVG(t.goals_per_game) OVER (
                PARTITION BY t.competition_code
            ) AS league_avg_goals,
            AVG(t.conceded_per_game) OVER (
                PARTITION BY t.competition_code
            ) AS league_avg_conceded,
            STDDEV(t.points_per_game) OVER (
                PARTITION BY t.competition_code
            ) AS stddev_ppg
        FROM marts.mart_league_table t
        WHERE competition_code = %s
        ORDER BY points DESC
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (competition_code,))
            rows = cur.fetchall()
    return [
        {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

def detect_anomaly(team_stats: dict, league_stats: dict) -> dict:
    """Call Claude with tool_choice to force structured JSON output."""
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=300,
        temperature=0,
        system=SYSTEM_PROMPT,
        tools=[ANOMALY_TOOL],
        tool_choice={"type": "tool", "name": "flag_anomaly"},
        messages=[
            {"role": "user", "content": build_user_message(team_stats, league_stats)}
        ],
    )
    tool_use = next(b for b in response.content if b.type == "tool_use")
    return tool_use.input


# ---------------------------------------------------------------------------
# Run detection
# ---------------------------------------------------------------------------

def run_anomaly_detection(competition_code: str = "PL") -> list[dict]:
    rows = fetch_team_stats(competition_code)
    if not rows:
        print(f"No data found for {competition_code}")
        return []

    # League stats are identical across all rows (window functions) — take from first
    league_stats = {
        "league_avg_ppg": rows[0]["league_avg_ppg"],
        "league_avg_goals": rows[0]["league_avg_goals"],
        "league_avg_conceded": rows[0]["league_avg_conceded"],
        "stddev_ppg": rows[0]["stddev_ppg"],
    }

    competition_name = rows[0]["competition_name"]

    print(f"\n{'='*70}")
    print(f"  ANOMALY DETECTION — {competition_name} ({competition_code})")
    print(f"  {len(rows)} teams analysed | League avg PPG: {league_stats['league_avg_ppg']:.2f} "
          f"± {league_stats['stddev_ppg']:.2f}")
    print(f"{'='*70}\n")

    anomalous = []

    for i, row in enumerate(rows, 1):
        team_stats = {k: row[k] for k in (
            "team_name", "competition_name", "played_games", "points",
            "points_per_game", "win_percentage", "goals_per_game",
            "conceded_per_game", "goal_difference",
        )}

        result = detect_anomaly(team_stats, league_stats)

        if result["is_anomalous"]:
            severity_label = result.get("severity", "unknown").upper()
            print(
                f"[ANOMALY — {severity_label}] {row['team_name']} "
                f"({result.get('anomaly_type', 'unknown').replace('_', ' ')})"
            )
            print(f"  Key metric : {result.get('key_metric', 'N/A')}")
            print(f"  {result.get('explanation', 'No explanation provided.')}")
            print()

            anomalous.append({
                "team_name": row["team_name"],
                "competition_code": competition_code,
                "points": row["points"],
                "points_per_game": row["points_per_game"],
                **result,
            })

        if i < len(rows):
            time.sleep(0.5)

    if not anomalous:
        print("No anomalies detected — all teams performing within expected ranges.\n")

    print(f"Summary: {len(anomalous)} anomalous team(s) out of {len(rows)}\n")
    return anomalous


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pl_anomalies = run_anomaly_detection("PL")
    bl1_anomalies = run_anomaly_detection("BL1")

    total = len(pl_anomalies) + len(bl1_anomalies)
    print(f"\n{'='*70}")
    print(f"  COMBINED: {total} anomalous team(s) flagged across PL and BL1")
    print(f"{'='*70}")
