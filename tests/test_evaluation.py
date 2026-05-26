from football_analytics.agent.evaluation import deterministic_checks, GOLDEN_DATASET


def test_deterministic_checks_nl_to_sql_valid_output():
    output = {
        "sql": "select team_name from marts.mart_league_table where competition_code = 'PL' limit 10",
        "success": True,
        "answer": "Liverpool are top with 38 points.",
        "attempts": 1,
    }
    failures = deterministic_checks(output, "nl_to_sql")
    assert failures == []
