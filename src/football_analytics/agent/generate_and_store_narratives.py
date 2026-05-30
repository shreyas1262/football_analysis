import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from decimal import Decimal

import anthropic
import psycopg2
import psycopg2.extras
import voyageai
from dotenv import load_dotenv

from football_analytics.config import DB_CONFIG

load_dotenv()

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
voyage_client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"])

# ---------------------------------------------------------------------------
# Prompts (same structure as match_narratives.py)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are a football match reporter writing detailed match reports for a football analytics platform. "
    "Write in an engaging, analytical style. Reports should be 7-9 sentences covering: "
    "(1) the overall result and its significance, "
    "(2) first-half performance and who dominated, "
    "(3) second-half narrative and any turning points, "
    "(4) tactical or statistical observations, "
    "(5) what the result means for both teams in the context of the season. "
    "Use explicit language about dominance, collapses, comebacks, and high-scoring when applicable. "
    "Always mention both team names multiple times so the report is rich with entity context."
)

FEW_SHOT_EXAMPLES = """\
Here are two example match reports to guide your style:

Example 1:
Match: Arsenal 2-0 Chelsea, HT: 1-0, Matchday 15, PL
Report: Arsenal produced a dominant north London derby display to dismantle Chelsea 2-0 at \
the Emirates, extending their unbeaten run with a commanding and tactically disciplined \
performance. The Gunners controlled the first half from the outset, pressing high and \
suffocating Chelsea's build-up play before breaking the deadlock through a well-worked \
team move. Arsenal's defensive organisation was exceptional throughout, limiting Chelsea \
to almost no clear-cut chances as the visitors struggled to impose themselves in midfield. \
A second goal after the break sealed the win and underlined Arsenal's superiority on the \
day. For Chelsea, it was a chastening afternoon that exposed their attacking fragility and \
left them five points adrift of the top four. Arsenal meanwhile sent a statement to their \
title rivals — this is a team capable of grinding out wins without conceding.

Example 2:
Match: Brentford 3-3 Fulham, HT: 1-2, Matchday 8, PL
Report: A breathless west London derby ended in a dramatic 3-3 draw as Brentford \
spectacularly squandered a half-time lead, only for Fulham to then collapse and allow \
the hosts to level in stoppage time. Fulham had controlled large spells of the first \
half and deservedly led by two goals at the break, looking commanding and well-organised \
in possession. Brentford's second-half comeback was remarkable — they overturned the \
deficit with two quick goals to lead for the first time — before Fulham snatched a \
late equaliser in a frantic finale. Both sides will rue their inability to hold leads, \
a recurring theme in a high-scoring encounter that could easily have ended 5-4. \
The draw typifies a derby where neither team could maintain their dominance for a \
full ninety minutes. Brentford will feel the momentum was theirs, while Fulham's \
failure to see out the win represents two dropped points from a winning position.

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
# Ensure unique constraint exists
# ---------------------------------------------------------------------------

def ensure_unique_constraint(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'match_reports_match_id_unique'
        """)
        if cur.fetchone() is None:
            cur.execute("""
                ALTER TABLE marts.match_reports
                ADD CONSTRAINT match_reports_match_id_unique
                UNIQUE (match_id)
            """)
    conn.commit()


# ---------------------------------------------------------------------------
# Narrative generation and storage
# ---------------------------------------------------------------------------

def generate_narratives_for_all(matches: list[dict]) -> list[int]:
    upsert_sql = """
        INSERT INTO marts.match_reports
            (match_id, competition_code, home_team, away_team,
             match_date, matchday, narrative)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO UPDATE SET
            narrative    = EXCLUDED.narrative,
            generated_at = NOW()
        RETURNING id
    """

    report_ids = []
    total = len(matches)

    with psycopg2.connect(**DB_CONFIG) as conn:
        ensure_unique_constraint(conn)

        for count, match in enumerate(matches, 1):
            header = (
                f"{match['home_team_name']} {match['home_goals']}-"
                f"{match['away_goals']} {match['away_team_name']}"
            )
            print(
                f"  [{count:04d}/{total}] {match['competition_code']} "
                f"MD{match['matchday']} | {header}",
                end=" ... ", flush=True,
            )

            for attempt in range(5):
                try:
                    response = client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=700,
                        temperature=0.8,
                        system=SYSTEM_PROMPT,
                        messages=[{"role": "user", "content": build_user_message(match)}],
                    )
                    break
                except anthropic.APIStatusError as exc:
                    if exc.status_code == 529 and attempt < 4:
                        wait = 30 * (2 ** attempt)
                        print(f"\n  [overloaded] waiting {wait}s before retry {attempt + 1}/4...", flush=True)
                        time.sleep(wait)
                    else:
                        raise
            narrative = response.content[0].text.strip()

            with conn.cursor() as cur:
                cur.execute(upsert_sql, (
                    match["match_id"],
                    match["competition_code"],
                    match["home_team_name"],
                    match["away_team_name"],
                    match["match_date"],
                    match["matchday"],
                    narrative,
                ))
                report_id = cur.fetchone()[0]
            conn.commit()

            report_ids.append(report_id)
            print(f"stored (id={report_id})")

            if count % 50 == 0:
                print(f"\n  Progress: {count}/{total} narratives generated\n")

            if count < total:
                time.sleep(0.5)

    return report_ids


# ---------------------------------------------------------------------------
# Text chunking
# ---------------------------------------------------------------------------

def chunk_text(text: str, chunk_size: int = 300) -> list[str]:
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    chunks = []
    current = ""

    for sentence in sentences:
        if len(current) + len(sentence) + 1 <= chunk_size:
            current = f"{current} {sentence}".strip() if current else sentence
        else:
            if current:
                chunks.append(current)
            current = sentence

    if current:
        chunks.append(current)

    return chunks


# ---------------------------------------------------------------------------
# Thematic tag generation
# ---------------------------------------------------------------------------

def generate_tags(row: dict) -> str:
    """Ask Claude to generate thematic search phrases for a match, used only for embedding."""
    prompt = (
        f"Generate 4-5 short thematic phrases (3-6 words each) that describe this football match. "
        f"Focus on themes someone would search for — "
        f"e.g. 'dominant away win', 'last-minute collapse', 'high-scoring thriller', "
        f"'team collapsed after leading'.\n\n"
        f"Match: {row['home_team']} {row['home_goals']}-{row['away_goals']} {row['away_team']}\n"
        f"Competition: {row['competition_code']} Matchday {row['matchday']}\n"
        f"Half-time lead dropped: {row['is_ht_lead_dropped']}\n"
        f"High scoring (4+ goals): {row['is_high_scoring']}\n"
        f"Report: {row['narrative']}\n\n"
        f"Return only the phrases as a comma-separated list, nothing else."
    )
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


# ---------------------------------------------------------------------------
# Embedding and storage
# ---------------------------------------------------------------------------

def embed_and_store_reports(report_ids: list[int]) -> int:
    fetch_sql = "SELECT id, narrative FROM marts.match_reports WHERE id = %s"

    upsert_sql = """
        INSERT INTO marts.report_embeddings
            (report_id, chunk_index, chunk_text, embedding)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (report_id, chunk_index) DO UPDATE SET
            chunk_text = EXCLUDED.chunk_text,
            embedding  = EXCLUDED.embedding,
            created_at = NOW()
    """

    print(f"\n{'='*70}")
    print(f"  EMBEDDING REPORTS ({len(report_ids)} total)")
    print(f"{'='*70}")

    total_chunks = 0

    with psycopg2.connect(**DB_CONFIG) as conn:
        for i, report_id in enumerate(report_ids, 1):
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(fetch_sql, (report_id,))
                row = cur.fetchone()

            if not row:
                print(f"  [{i:03d}/{len(report_ids)}] report_id={report_id} not found, skipping")
                continue

            chunks = chunk_text(row["narrative"])
            print(
                f"  [{i:03d}/{len(report_ids)}] report_id={report_id} "
                f"— {len(chunks)} chunk(s)", end=" ... ", flush=True
            )

            for chunk_index, chunk in enumerate(chunks):
                for attempt in range(5):
                    try:
                        response = voyage_client.embed(
                            [chunk],
                            model="voyage-3",
                        )
                        break
                    except Exception as exc:
                        if attempt < 4:
                            wait = 15 * (2 ** attempt)
                            print(f"\n  [voyage error] {exc} — waiting {wait}s before retry {attempt + 1}/4...", flush=True)
                            time.sleep(wait)
                        else:
                            raise
                embedding = response.embeddings[0]

                with conn.cursor() as cur:
                    cur.execute(upsert_sql, (
                        report_id,
                        chunk_index,
                        chunk,
                        embedding,
                    ))
                conn.commit()

                total_chunks += 1

                if chunk_index < len(chunks) - 1:
                    time.sleep(0.2)

            print("embedded")

            if i < len(report_ids):
                time.sleep(0.2)

    return total_chunks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

COMPETITIONS = ["PL", "PD", "BL1", "SA", "FL1", "CL"]

FETCH_SQL = """
    SELECT m.match_id, m.match_date, m.matchday, m.competition_code,
           m.competition_name, m.home_team_name, m.away_team_name,
           m.home_goals, m.away_goals, m.home_goals_ht, m.away_goals_ht,
           m.total_goals, m.result, m.ht_leader, m.is_ht_lead_dropped,
           m.is_high_scoring
    FROM marts.mart_match_results m
    WHERE m.competition_code = ANY(%s)
      AND m.match_id NOT IN (
          SELECT match_id FROM marts.match_reports
          WHERE match_id IS NOT NULL
      )
    ORDER BY m.match_date ASC
"""

UNEMBEDDED_SQL = """
    SELECT mr.id
    FROM marts.match_reports mr
    WHERE NOT EXISTS (
        SELECT 1 FROM marts.report_embeddings re
        WHERE re.report_id = mr.id
    )
    ORDER BY mr.id ASC
"""


def main() -> None:
    auto_confirm = "--yes" in sys.argv
    with psycopg2.connect(**DB_CONFIG) as conn:
        ensure_unique_constraint(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(FETCH_SQL, (COMPETITIONS,))
            rows = cur.fetchall()

        with conn.cursor() as cur:
            cur.execute(UNEMBEDDED_SQL)
            unembedded_ids = [row[0] for row in cur.fetchall()]

    matches = [
        {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]

    total_matches = len(matches)
    estimated_cost = total_matches * 500 * (0.80 + 4.00) / 1_000_000

    print(f"\n{'='*70}")
    print(f"  Matches without narratives : {total_matches}")
    print(f"  Reports without embeddings : {len(unembedded_ids)}")
    print(f"  Estimated cost             : ${estimated_cost:.2f} (Haiku, ~500 tokens/narrative)")
    print(f"{'='*70}")
    if not auto_confirm:
        print("Continue? (y/n) ", end="", flush=True)
        if input().strip().lower() != "y":
            print("Aborted.")
            return

    if total_matches > 0:
        print(f"\n{'='*70}")
        print(f"  GENERATING NARRATIVES — ALL COMPETITIONS")
        print(f"{'='*70}")
        new_report_ids = generate_narratives_for_all(matches)
        unembedded_ids = list(dict.fromkeys(unembedded_ids + new_report_ids))

    total_chunks = embed_and_store_reports(unembedded_ids)

    print(f"\n{'='*70}")
    print(f"  COMPLETE")
    print(f"  {len(unembedded_ids)} reports embedded")
    print(f"  {total_chunks} chunks embedded")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
