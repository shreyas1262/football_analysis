import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "agent"))

from evaluation import deterministic_checks, GOLDEN_DATASET


def test_deterministic_checks_nl_to_sql_valid_output():
    output = {
        "sql": "select team_name from marts.mart_league_table where competition_code = 'PL' limit 10",
        "success": True,
        "answer": "Liverpool are top with 38 points.",
        "attempts": 1,
    }
    failures = deterministic_checks(output, "nl_to_sql")
    assert failures == []


def test_deterministic_checks_catches_empty_answer():
    output = {
        "sql": "select * from marts.mart_league_table limit 10",
        "success": True,
        "answer": "",
        "attempts": 1,
    }
    failures = deterministic_checks(output, "nl_to_sql")
    assert len(failures) > 0


def test_deterministic_checks_catches_training_memory():
    output = {
        "sql": "select * from marts.mart_league_table limit 10",
        "success": True,
        "response": "Based on my training data, Liverpool won...",
        "attempts": 1,
    }
    failures = deterministic_checks(output, "agent_response")
    assert len(failures) > 0


def test_deterministic_checks_catches_invalid_severity():
    output = {
        "is_anomalous": True,
        "severity": "extreme",  # not in allowed values
        "anomaly_type": "overperforming",
        "explanation": "This team is performing well above average.",
    }
    failures = deterministic_checks(output, "anomaly_detection")
    assert len(failures) > 0


def test_golden_dataset_structure():
    assert len(GOLDEN_DATASET) >= 20
    for case in GOLDEN_DATASET:
        assert "question" in case
        has_check = any(
            k in case
            for k in ["expected_contains", "must_not_contain", "expected_behaviour", "must_include_number"]
        )
        assert has_check, f"No checks defined for: {case['question']}"
