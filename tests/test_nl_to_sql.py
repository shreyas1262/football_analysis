import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "agent"))

from nl_to_sql import validate_sql

def test_validate_sql_accepts_valid_select():
    valid = "select team_name, points from marts.mart_league_table where competition_code = 'PL' order by points desc limit 10"
    is_valid, error = validate_sql(valid)
    assert is_valid is True
    assert error == ""


def test_validate_sql_rejects_drop_table():
    sql = "drop table marts.mart_league_table"
    is_valid, error = validate_sql(sql)
    assert is_valid is False
    assert "drop" in error.lower()

def test_validate_sql_accepts_column_named_dropped():
    # regression test — is_ht_lead_dropped should not be rejected
    sql = "select count(*) from marts.mart_match_results where is_ht_lead_dropped = true"
    is_valid, error = validate_sql(sql)
    assert is_valid is True
    assert error == ""


def test_validate_sql_accepts_is_ht_lead_dropped_filter():
    # regression test — avg aggregation with is_ht_lead_dropped filter should not be rejected
    sql = "select avg(total_goals) from marts.mart_match_results where is_ht_lead_dropped = true"
    is_valid, error = validate_sql(sql)
    assert is_valid is True, f"Should accept is_ht_lead_dropped column: {error}"

def test_validate_sql_rejects_insert():
    sql = "insert into marts.mart_league_table values (1, 2, 3)"
    is_valid, error = validate_sql(sql)
    assert is_valid is False
    assert "insert" in error.lower()

def test_validate_sql_rejects_empty():
    is_valid, error = validate_sql("")
    assert is_valid is False

def test_validate_sql_rejects_no_marts_table():
    sql = "select * from pg_tables"
    is_valid, error = validate_sql(sql)
    assert is_valid is False

