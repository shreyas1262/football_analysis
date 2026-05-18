import os

from dotenv import load_dotenv

load_dotenv()


def check_health():
    """Verify all required environment variables and connections."""
    import click

    checks = {
        "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
        "VOYAGE_API_KEY": os.getenv("VOYAGE_API_KEY"),
        "SUPABASE_HOST": os.getenv("SUPABASE_HOST"),
        "SUPABASE_PASSWORD": os.getenv("SUPABASE_PASSWORD"),
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
            from config.db import get_conn
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
