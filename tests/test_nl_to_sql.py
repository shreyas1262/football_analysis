from football_analytics.agent.nl_to_sql import validate_sql


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
