import os

import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def db_conn():
    """Live database connection for integration tests."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "football_db"),
        user=os.getenv("DB_USER", "football"),
        password=os.getenv("DB_PASSWORD", "football"),
    )
    yield conn
    conn.close()


@pytest.fixture
def sample_match():
    """Sample match data for testing narratives."""
    return {
        "match_id": 1,
        "match_date": "2024-12-22",
        "matchday": 18,
        "competition_code": "PL",
        "competition_name": "Premier League",
        "home_team_name": "Tottenham Hotspur FC",
        "away_team_name": "Liverpool FC",
        "home_goals": 3,
        "away_goals": 6,
        "home_goals_ht": 1,
        "away_goals_ht": 3,
        "total_goals": 9,
        "result": "away",
        "ht_leader": "away",
        "is_ht_lead_dropped": False,
        "is_high_scoring": True,
    }


@pytest.fixture
def sample_narrative():
    return """Liverpool produced a stunning display at Spurs.
    The visitors dominated from the first whistle, racing into
    a commanding lead. A historic result that sent a title
    statement to all rivals."""
