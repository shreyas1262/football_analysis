import json
import os

import anthropic
from dotenv import load_dotenv

from football_analytics.agent.nl_to_sql import nl_to_sql_pipeline
from football_analytics.agent.tool_handlers import ToolHandlers

load_dotenv()

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ---------------------------------------------------------------------------
# Tool schemas
# The agent adds one tool beyond the shared set: nl_to_sql
# ---------------------------------------------------------------------------

TOOLS = ToolHandlers.SCHEMAS + [
    {
        "name": "nl_to_sql",
        "description": (
            "Generates and executes a custom SQL query for any football question "
            "not covered by the other tools. Use this as a fallback when the specific "
            "predefined tools cannot answer the question. Examples: cross-table analysis, "
            "custom aggregations, questions about specific dates or matchdays, "
            "anything requiring a JOIN between tables."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "The natural language question to convert to SQL"},
            },
            "required": ["question"],
        },
    },
]

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a football analytics assistant with access to a database covering the Premier League, La Liga, Serie A, and Bundesliga.

## Data available
- League competitions: Premier League (PL), La Liga (PD), Bundesliga (BL1), Serie A (SA)
- European competitions: Champions League (CL) match results are available for team performance queries
- Data types: match results, league standings, team statistics, bottler index
- NOT available: individual player stats (xG, assists, minutes), transfer data, injury data, future fixtures
- Note: get_league_table, get_bottler_index, and get_season_summary only work for the four domestic leagues. For CL performance use get_team_season_stats or get_team_form with competition_code="CL".

If a question falls outside this scope, say so clearly and suggest what you CAN answer instead.

## Season handling — critical
- ALWAYS call resolve_season before calling any data tool when the user mentions a season (e.g. "this season", "last season", "2 seasons ago", "2024-25")
- Pass the returned season_start_year to the data tool
- The data tools default to the latest available season when no season_start_year is supplied

## Tool selection guide
- Full-season stats or season comparisons for one team in one league → use get_team_season_stats
- Recent form / last N matches (with rolling form points) → use get_team_form (add competition_code to exclude cups/Europe)
- League standings → use get_league_table

## What you must always do
- Use the provided tools to retrieve data before answering any statistical question
- Cite which tool result your answer is based on
- If a question needs both statistics and match context, use both a data tool AND search_match_reports
- Keep answers concise — 3-5 sentences unless asked for more detail
- Always cite specific numbers in your answer — never give a vague answer when the data contains exact figures
- If asked how many leagues, competitions or teams are in the database, use nl_to_sql to count them rather than guessing

## What you must never do
- Answer statistical questions from training memory — always use a tool first
- Invent match results, scores, or player statistics
- Make claims you cannot attribute to a tool result
- Answer questions about future events, predictions, or player stats not in the database (xG, assists, individual stats beyond goals)
- Answer questions about competitions not in the database (Champions League results, cup competitions)"""

# ---------------------------------------------------------------------------
# Tool dispatcher
# ---------------------------------------------------------------------------

def call_tool(name: str, inputs: dict) -> list[dict] | dict | str:
    if name == "resolve_season":
        return ToolHandlers.resolve_season(inputs["reference"])
    elif name == "get_league_table":
        return ToolHandlers.get_league_table(inputs["competition_code"], inputs.get("season_start_year"))
    elif name == "get_bottler_index":
        return ToolHandlers.get_bottler_index(
            inputs["competition_code"],
            inputs.get("min_matches_leading", 3),
            inputs.get("season_start_year"),
        )
    elif name == "get_team_season_stats":
        return ToolHandlers.get_team_season_stats(
            inputs["team_name"],
            inputs["competition_code"],
            inputs.get("season_start_year"),
        )
    elif name == "get_team_form":
        return ToolHandlers.get_team_form(
            inputs["team_name"],
            inputs.get("last_n_games", 5),
            inputs.get("competition_code"),
            inputs.get("season_start_year"),
        )
    elif name == "get_head_to_head":
        return ToolHandlers.get_head_to_head(
            inputs["team_a"], inputs["team_b"], inputs.get("season_start_year")
        )
    elif name == "get_high_scoring_matches":
        return ToolHandlers.get_high_scoring_matches(
            inputs.get("competition_code"),
            inputs.get("min_goals", 5),
            inputs.get("limit", 10),
            inputs.get("season_start_year"),
        )
    elif name == "get_season_summary":
        return ToolHandlers.get_season_summary(
            inputs["competition_code"], inputs.get("season_start_year")
        )
    elif name == "search_match_reports":
        return ToolHandlers.search_match_reports(inputs["query"], inputs.get("limit", 5))
    elif name == "nl_to_sql":
        return nl_to_sql_pipeline(inputs["question"])
    return []

# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

def run_agent(question: str, verbose: bool = True) -> str:
    messages = [{"role": "user", "content": question}]

    for _ in range(10):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            return next(b.text for b in response.content if hasattr(b, "text"))

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                if verbose:
                    print(f"  [tool] {block.name}({json.dumps(block.input, default=str)})")
                result = call_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })

            messages.append({"role": "user", "content": tool_results})

    return "Agent reached iteration limit without a final answer."

# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

DEMO_QUESTIONS = [
    "Who are the top 3 biggest bottlers in the Bundesliga this season?",
    "How did Liverpool perform against the top 6 this season? Were there any surprise results?",
    "Which league had the most goals per game on average this season?",
    "Tell me about the most dramatic matches in the Premier League — any games with big comebacks or late drama?",
    "Compare Arsenal and Liverpool — who had the better season based on the data?",
]


def run_demo() -> None:
    for i, question in enumerate(DEMO_QUESTIONS, 1):
        print(f"\n{'='*70}")
        print(f"  Q{i}: {question}")
        print(f"{'='*70}")
        answer = run_agent(question, verbose=True)
        print(f"\n{answer}\n")


if __name__ == "__main__":
    run_demo()
