from dotenv import load_dotenv

load_dotenv()


def ask_question(question: str, verbose: bool = False) -> str:
    """Ask a football question using the AI agent."""
    from agent.football_agent import run_agent
    return run_agent(question, verbose=verbose)


def get_stats():
    """Print database statistics."""
    import click
    import psycopg2.extras

    from config.db import get_conn

    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                competition_code,
                COUNT(*) as matches,
                COUNT(DISTINCT season_id) as seasons
            FROM marts.mart_match_results
            GROUP BY competition_code
            ORDER BY matches DESC
        """)
        rows = cur.fetchall()
    conn.close()

    click.echo("\n  Football Analytics Database\n")
    click.echo(f"  {'Competition':<12} {'Matches':<10} {'Seasons'}")
    click.echo(f"  {'-'*35}")
    for row in rows:
        click.echo(
            f"  {row['competition_code']:<12} "
            f"{row['matches']:<10} "
            f"{row['seasons']}"
        )
