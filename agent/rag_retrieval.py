import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import psycopg2.extras
import voyageai
from dotenv import load_dotenv

from config import DB_CONFIG

load_dotenv()

# ---------------------------------------------------------------------------
# Voyage client
# ---------------------------------------------------------------------------

voyage_client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"])

# ---------------------------------------------------------------------------
# Query embedding
# ---------------------------------------------------------------------------

def embed_query(query_text: str) -> list[float]:
    """Embed a query string using the voyage-3 model and return the vector."""
    response = voyage_client.embed(
        [query_text],
        model="voyage-3",
    )
    return response.embeddings[0]


# ---------------------------------------------------------------------------
# Semantic search
# ---------------------------------------------------------------------------

def retrieve_relevant_chunks(
    query_text: str,
    limit: int = 5,
    min_similarity: float = 0.4,
) -> list[dict]:
    """Search report_embeddings for chunks semantically similar to query_text.

    Returns up to `limit` chunks whose cosine similarity meets `min_similarity`,
    each with chunk_text, match metadata, and the similarity score.
    """
    embedding = embed_query(query_text)

    sql = """
        SELECT
            e.chunk_text,
            r.home_team,
            r.away_team,
            r.match_date,
            r.competition_code,
            r.matchday,
            1 - (e.embedding <=> %s::vector) AS similarity
        FROM marts.report_embeddings e
        JOIN marts.match_reports r ON e.report_id = r.id
        WHERE 1 - (e.embedding <=> %s::vector) >= %s
        ORDER BY similarity DESC
        LIMIT %s
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (embedding, embedding, min_similarity, limit))
            rows = cur.fetchall()

    return [
        {
            "chunk_text": row["chunk_text"],
            "home_team": row["home_team"],
            "away_team": row["away_team"],
            "match_date": str(row["match_date"]),
            "competition_code": row["competition_code"],
            "matchday": row["matchday"],
            "similarity": float(row["similarity"]),
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Context formatting
# ---------------------------------------------------------------------------

def build_rag_context(chunks: list[dict]) -> str:
    """Format retrieved chunks into a context block ready to inject into a Claude prompt."""
    entries = "".join(
        f"[{c['home_team']} vs {c['away_team']}, {c['competition_code']}, "
        f"{c['match_date']}, similarity: {c['similarity']:.2f}]\n"
        f"{c['chunk_text']}\n\n"
        for c in chunks
    )
    return "## Relevant match reports\n\n" + entries


# ---------------------------------------------------------------------------
# Retrieval test
# ---------------------------------------------------------------------------

def test_retrieval() -> bool:
    """Run three semantic search queries and print results. Returns True if all return at least 1 chunk."""
    queries = [
        "teams that collapsed after leading",
        "high scoring Bundesliga matches",
        "Liverpool dominance",
    ]

    all_passed = True

    for query in queries:
        print(f"\n{'='*70}")
        print(f'  QUERY: "{query}"')
        print(f"{'='*70}")

        chunks = retrieve_relevant_chunks(query, limit=5, min_similarity=0.4)

        if not chunks:
            print("  No results above similarity threshold.\n")
            all_passed = False
            continue

        for i, chunk in enumerate(chunks, 1):
            print(
                f"\n  [{i}] {chunk['home_team']} vs {chunk['away_team']} "
                f"| {chunk['competition_code']} MD{chunk['matchday']} "
                f"| {chunk['match_date']} | similarity: {chunk['similarity']:.2f}"
            )
            print(f"  {chunk['chunk_text']}")

    return all_passed


if __name__ == "__main__":
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM marts.report_embeddings")
            chunk_count = cur.fetchone()[0]

    print(f"\nDatabase contains {chunk_count} embedded chunk(s) in marts.report_embeddings\n")

    passed = test_retrieval()

    print(f"\n{'='*70}")
    print(f"  All queries returned results: {passed}")
    print(f"{'='*70}\n")
