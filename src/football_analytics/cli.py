import os
from decimal import Decimal

import click
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


@click.group()
@click.version_option()
def main():
    """Football Analytics — ask questions about football data."""
    pass


@main.command()
@click.argument("question")
@click.option("--verbose", "-v", is_flag=True, help="Show which tools were called")
def ask(question, verbose):
    """Ask a football question in plain English.

    Example: football-analytics ask "who are the biggest bottlers?"
    """
    from football_analytics.agent.football_agent import run_agent
    answer = run_agent(question, verbose=verbose)
    click.echo(answer)


@main.command()
def stats():
    """Show database statistics — matches, leagues, seasons."""
    from football_analytics.config import get_conn

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


@main.command()
@click.option("--seasons", default=None, help="Comma-separated seasons to ingest, e.g. 2023,2022")
@click.option("--full", "full_ingest", is_flag=True, help="Ingest current and previous season")
@click.option("--skip-dbt", is_flag=True, help="Skip dbt run and test")
@click.option("--skip-narratives", is_flag=True, help="Skip narrative and embedding generation")
@click.option("--yes", "-y", "auto_confirm", is_flag=True, help="Skip cost confirmation prompt")
def sync(seasons, full_ingest, skip_dbt, skip_narratives, auto_confirm):
    """Run the full data pipeline: ingest → dbt → narratives.

    Examples:
      football-analytics sync
      football-analytics sync --full
      football-analytics sync --seasons 2023,2022
    """
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).parent.parent.parent.parent  # repo root

    # --- ingest ---
    ingest_args = [sys.executable, str(root / "airflow/dags/football_ingestion.py")]
    if seasons:
        ingest_args += ["--seasons", seasons]
    elif full_ingest:
        ingest_args.append("--full")
    click.echo("Running ingestion...")
    result = subprocess.run(ingest_args)
    if result.returncode != 0:
        raise SystemExit(result.returncode)

    # --- dbt ---
    if not skip_dbt:
        dbt_dir = root / "dbt"
        click.echo("Running dbt run...")
        r = subprocess.run(["dbt", "run", "--profiles-dir", "./", "--profile", "ci"], cwd=dbt_dir)
        if r.returncode != 0:
            raise SystemExit(r.returncode)
        click.echo("Running dbt test...")
        r = subprocess.run(["dbt", "test", "--profiles-dir", "./", "--profile", "ci"], cwd=dbt_dir)
        if r.returncode != 0:
            raise SystemExit(r.returncode)

    # --- narratives ---
    if not skip_narratives:
        click.echo("Generating narratives and embeddings...")
        narrative_script = root / "src/football_analytics/agent/generate_and_store_narratives.py"
        narrative_args = [sys.executable, str(narrative_script)]
        if auto_confirm:
            narrative_args.append("--yes")
        r = subprocess.run(narrative_args)
        if r.returncode != 0:
            raise SystemExit(r.returncode)

    click.echo("Sync complete.")


@main.command()
def health():
    """Check database connection and API key status."""
    checks = {
        "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
        "VOYAGE_API_KEY": os.getenv("VOYAGE_API_KEY"),
        "SUPABASE_HOST / DB_HOST": os.getenv("SUPABASE_HOST") or os.getenv("DB_HOST"),
    }

    all_good = True
    for key, value in checks.items():
        if value:
            click.echo(f"  ✓ {key}")
        else:
            click.echo(f"  ✗ {key} — not set")
            all_good = False

    if all_good:
        try:
            from football_analytics.config import get_conn
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM marts.mart_match_results")
                count = cur.fetchone()[0]
            conn.close()
            click.echo(f"  ✓ Database — {count} matches accessible")
        except Exception as e:
            click.echo(f"  ✗ Database connection failed: {e}")
            all_good = False

    if all_good:
        click.echo("\n  All checks passed — ready to use!")
    else:
        click.echo("\n  Some checks failed — see above.")
