import json
import os
import re as _re
from decimal import Decimal

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from football_analytics.config import DB_CONFIG
from football_analytics.agent.nl_to_sql import validate_sql

load_dotenv()

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def query_db(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [
        {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Layer 1 — deterministic checks
# ---------------------------------------------------------------------------

def deterministic_checks(output: dict, feature: str) -> list[str]:
    """Run hard rule checks on a feature output. Returns a list of failure messages."""
    failures = []

    if feature == "nl_to_sql":
        sql = output.get("sql", "")
        answer = output.get("answer", "")

        if not sql or not sql.strip():
            failures.append("sql is empty")

        if not output.get("success"):
            failures.append("success is False")

        if sql and sql.strip():
            is_valid, validation_error = validate_sql(sql)
            if not is_valid:
                failures.append(f"validate_sql failed: {validation_error}")

        if not answer or not answer.strip():
            failures.append("answer is empty")

        for phrase in ("i don't know", "cannot answer"):
            if phrase in answer.lower():
                failures.append(f"answer contains disallowed phrase: '{phrase}'")

    elif feature == "anomaly_detection":
        required_keys = {"is_anomalous", "severity", "anomaly_type", "explanation"}
        missing = required_keys - output.keys()
        if missing:
            failures.append(f"missing keys: {missing}")

        severity = output.get("severity", "")
        if severity not in ("low", "medium", "high", "none"):
            failures.append(f"invalid severity value: '{severity}'")

        explanation = output.get("explanation", "")
        if len(explanation) <= 20:
            failures.append(f"explanation too short ({len(explanation)} chars, need > 20)")

    elif feature == "agent_response":
        response = output.get("response", "")

        if not response or not response.strip():
            failures.append("response is empty")

        for phrase in ("i don't have access", "my training data"):
            if phrase in response.lower():
                failures.append(f"response contains disallowed phrase: '{phrase}'")

        if len(response) <= 50:
            failures.append(f"response too short ({len(response)} chars, need > 50)")

    return failures


# ---------------------------------------------------------------------------
# Layer 2 — LLM-as-judge
# ---------------------------------------------------------------------------

LLM_JUDGE_SYSTEM = (
    "You are evaluating a football analytics AI agent. "
    "Score the response on three dimensions from 1-5. "
    "Return only valid JSON, no other text."
)


def llm_as_judge(question: str, tool_results: list, answer: str) -> dict | None:
    """Evaluate agent output with a separate Claude call. Returns scores dict or None on parse error."""
    user_prompt = (
        f"Question: {question}\n\n"
        f"Tool results available to agent:\n"
        f"{json.dumps(tool_results, default=str)[:2000]}\n\n"
        f"Agent answer:\n{answer}\n\n"
        "Score on:\n"
        "- grounding: does every claim come from tool results? "
        "(1=no support, 5=fully attributed)\n"
        "- responsiveness: does it answer what was asked? "
        "(1=off topic, 5=directly answers)\n"
        "- accuracy: are numbers consistent with tool results? "
        "(1=contradicts data, 5=perfectly consistent)\n\n"
        "Return JSON:\n"
        "{\n"
        '  "grounding": <1-5>,\n'
        '  "responsiveness": <1-5>,\n'
        '  "accuracy": <1-5>,\n'
        '  "issues": ["list of specific problems"],\n'
        '  "overall_pass": <true if all scores >= 3>\n'
        "}"
    )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=400,
        temperature=0,
        system=LLM_JUDGE_SYSTEM,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw = response.content[0].text.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


# ---------------------------------------------------------------------------
# Golden dataset
# ---------------------------------------------------------------------------

GOLDEN_DATASET = [
    # ------------------------------------------------------------------
    # Factual — correct answers we know from our data
    # ------------------------------------------------------------------
    {
        "question": "Who is top of the Premier League?",
        "expected_contains": ["Liverpool"],
        "expected_tool": "get_league_table",
        "must_include_number": True,
    },
    {
        "question": "What is Southampton's bottler index drop rate?",
        "expected_contains": ["100"],
        "expected_tool": "get_bottler_index",
        "must_include_number": True,
    },
    {
        "question": "How many goals did Bayern concede?",
        "expected_contains": ["16"],
        "expected_tool": "get_league_table",
        "must_include_number": True,
    },
    {
        "question": "What was the highest scoring match?",
        "expected_contains": ["9", "Liverpool", "Tottenham"],
        "expected_tool": "get_high_scoring_matches",
        "must_include_number": True,
    },
    {
        "question": "How many leagues are in the database?",
        "must_include_number": True,
    },
    # ------------------------------------------------------------------
    # Cross-tool — require multiple tool calls
    # ------------------------------------------------------------------
    {
        "question": "Compare Liverpool and Arsenal this season",
        "expected_contains": ["Liverpool", "Arsenal", "points"],
        "must_include_number": True,
    },
    {
        "question": "Which Bundesliga team has the worst bottler rate?",
        "expected_contains": ["Bochum", "57"],
        "expected_tool": "get_bottler_index",
        "must_include_number": True,
    },
    # ------------------------------------------------------------------
    # Regression tests — questions that previously caused issues
    # ------------------------------------------------------------------
    {
        "question": "How many matches had a half-time lead dropped?",
        "expected_contains": ["193"],
        "must_include_number": True,
    },
    {
        "question": "Which teams won more away than home games?",
        "must_include_number": True,
        "must_not_contain": ["Bayern"],
    },
    # ------------------------------------------------------------------
    # Graceful refusals
    # ------------------------------------------------------------------
    {
        "question": "Who will win the Champions League next year?",
        "expected_behaviour": "graceful_refusal",
        "must_not_contain": ["will win", "I predict", "the winner will be"],
    },
    {
        "question": "What is Messi's xG this season?",
        "expected_behaviour": "graceful_refusal",
        "must_not_contain": ["xG is", "expected goals is"],
    },
    # ------------------------------------------------------------------
    # Competition coverage — La Liga (PD)
    # ------------------------------------------------------------------
    {
        "question": "Who is top of La Liga this season?",
        "expected_tool": "get_league_table",
        "must_include_number": True,
    },
    {
        "question": "Which La Liga team collapses most from half-time leads?",
        "expected_tool": "get_bottler_index",
        "must_include_number": True,
    },
    # ------------------------------------------------------------------
    # Competition coverage — Bundesliga (BL1)
    # ------------------------------------------------------------------
    {
        "question": "Who leads the Bundesliga?",
        "expected_contains": ["Bayern"],
        "expected_tool": "get_league_table",
        "must_include_number": True,
    },
    {
        "question": "Give me a Bundesliga season summary",
        "expected_contains": ["goals"],
        "expected_tool": "get_season_summary",
        "must_include_number": True,
    },
    # ------------------------------------------------------------------
    # Competition coverage — Serie A (SA)
    # ------------------------------------------------------------------
    {
        "question": "Show me the top 5 teams in Serie A",
        "expected_tool": "get_league_table",
        "must_include_number": True,
    },
    {
        "question": "What were the most exciting high-scoring matches in the Bundesliga?",
        "expected_contains": ["Frankfurt", "Bochum", "9"],
        "expected_tool": "get_high_scoring_matches",
        "must_include_number": True,
    },
    # ------------------------------------------------------------------
    # Tool coverage — get_team_form
    # ------------------------------------------------------------------
    {
        "question": "What is Liverpool's recent form?",
        "expected_contains": ["Liverpool"],
        "expected_tool": "get_team_form",
        "must_include_number": True,
    },
    {
        "question": "Has Arsenal been winning or losing recently?",
        "expected_contains": ["Arsenal"],
        "expected_tool": "get_team_form",
        "must_include_number": True,
    },
    # ------------------------------------------------------------------
    # Tool coverage — get_head_to_head
    # ------------------------------------------------------------------
    {
        "question": "What is the head-to-head record between Arsenal and Chelsea?",
        "expected_contains": ["Arsenal", "Chelsea"],
        "expected_tool": "get_head_to_head",
        "must_include_number": True,
    },
    # ------------------------------------------------------------------
    # Tool coverage — nl_to_sql (ad-hoc queries)
    # ------------------------------------------------------------------
    {
        "question": "How many Premier League matches ended 0-0 this season?",
        "expected_tool": "nl_to_sql",
        "must_include_number": True,
    },
    {
        "question": "What was the average number of goals per game on matchday 1 across all leagues?",
        "expected_tool": "nl_to_sql",
        "must_include_number": True,
    },
    {
        "question": "Which teams appear in both the top 5 for goals scored and have a bottler drop rate above 30%?",
        "expected_tool": "nl_to_sql",
        "must_include_number": False,
    },
    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------
    {
        "question": "How many points does Southampton have this season?",
        "expected_contains": ["Southampton"],
        "expected_tool": "get_league_table",
        "must_include_number": True,
    },
    {
        "question": "Who won the 2022 World Cup?",
        "expected_behaviour": "graceful_refusal",
        "must_not_contain": ["Argentina", "3-3"],
    },
]

# ---------------------------------------------------------------------------
# Layer 3 — golden dataset runner
# ---------------------------------------------------------------------------

def run_golden_dataset(agent_fn) -> dict:
    """Run every GOLDEN_DATASET case through agent_fn and report pass/fail."""
    failures = []

    for case in GOLDEN_DATASET:
        question = case["question"]
        response = agent_fn(question, verbose=False)
        response_lower = response.lower()
        case_failures = []

        for phrase in case.get("expected_contains", []):
            if phrase.lower() not in response_lower:
                case_failures.append(f"missing expected text: '{phrase}'")

        for phrase in case.get("must_not_contain", []):
            if phrase.lower() in response_lower:
                case_failures.append(f"contains forbidden text: '{phrase}'")

        if case.get("must_include_number") and not _re.search(r"\d", response):
            case_failures.append("no number found in response")

        if case.get("expected_behaviour") == "graceful_refusal":
            refusal_signals = [
                "don't have", "cannot", "not available", "outside",
                "only covers", "no data", "not in", "limited to",
            ]
            if not any(signal in response_lower for signal in refusal_signals):
                case_failures.append("expected graceful refusal but agent answered directly")

        if case_failures:
            failures.append({"question": question, "reasons": case_failures})

    total = len(GOLDEN_DATASET)
    passed = total - len(failures)
    pass_rate = passed / total

    return {
        "total": total,
        "passed": passed,
        "failed": len(failures),
        "pass_rate": round(pass_rate, 3),
        "passed_threshold": pass_rate >= 0.85,
        "failures": failures,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_eval_report(results: dict) -> None:
    """Print a formatted evaluation report with pass/fail summary and recommendations."""
    passed_threshold = results["passed_threshold"]
    status = "PASS" if passed_threshold else "FAIL"
    bar = "=" * 60

    print(bar)
    print(f"  EVALUATION REPORT  [{status}]")
    print(bar)
    print(
        f"  Pass rate : {results['pass_rate'] * 100:.1f}%  "
        f"({results['passed']}/{results['total']} cases)"
        f"  — threshold 85%"
    )
    print()

    failures = results["failures"]
    if not failures:
        print("  All cases passed.")
    else:
        print(f"  Failures ({results['failed']}):")
        for i, f in enumerate(failures, 1):
            print(f"\n  {i}. {f['question']}")
            for reason in f["reasons"]:
                print(f"       • {reason}")

    print()
    print("  Recommendations:")

    if not failures:
        print("  • No prompt changes needed — all cases pass.")
    else:
        reasons_flat = [r for f in failures for r in f["reasons"]]

        if any("missing expected text" in r for r in reasons_flat):
            print("  • Agent is omitting key facts — add 'always cite specific numbers' to SYSTEM_PROMPT.")

        if any("graceful refusal" in r for r in reasons_flat):
            print("  • Agent is answering out-of-scope questions — strengthen refusal instructions in SYSTEM_PROMPT.")

        if any("forbidden text" in r for r in reasons_flat):
            print("  • Agent is leaking forbidden phrases — add explicit must_not rules to SYSTEM_PROMPT.")

        if any("no number" in r for r in reasons_flat):
            print("  • Agent is giving vague answers without statistics — enforce number citation in SYSTEM_PROMPT.")

        missing_tool_cases = [f["question"] for f in failures if any("missing expected text" in r for r in f["reasons"])]
        if missing_tool_cases:
            print(f"  • Review tool routing for: {missing_tool_cases[:3]}")

    print(bar)


if __name__ == "__main__":
    from football_analytics.agent.football_agent import run_agent

    results = run_golden_dataset(run_agent)
    generate_eval_report(results)
