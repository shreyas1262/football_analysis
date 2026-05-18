import click
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
    from football_analytics.agent import ask_question
    answer = ask_question(question, verbose=verbose)
    click.echo(answer)


@main.command()
def stats():
    """Show database statistics — matches, leagues, seasons."""
    from football_analytics.agent import get_stats
    get_stats()


@main.command()
def health():
    """Check database connection and API key status."""
    from football_analytics.config import check_health
    check_health()
