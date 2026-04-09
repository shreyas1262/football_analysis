# Football Analytics MCP Server

An MCP (Model Context Protocol) server that exposes the football analytics
data pipeline as callable tools for Claude. The server connects to the
PostgreSQL `football_db` database and queries the dbt mart layer directly.

## What it does

The server runs as a local subprocess. Claude discovers its tools automatically
on startup and calls them when answering football-related questions. All results
are returned as JSON.

## Prerequisites

- Docker running with `postgres` container healthy
- Python virtual environment set up: `python -m venv .venv`
- Dependencies installed: `pip install -r requirements.txt`

## Starting the server

The server is started automatically by Claude Code when configured via
`claude_mcp_config.json`. To start it manually for testing:

```bash
source .venv/bin/activate
python mcp/football_mcp_server.py
```

To register with Claude Code, add the config file path when launching:

```bash
claude --mcp-config claude_mcp_config.json
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `football_db` | Database name |
| `DB_USER` | `football` | Database user |
| `DB_PASSWORD` | `football` | Database password |

## Available tools

### `get_league_table`
Returns current league standings with points, wins, goals and performance metrics.

**Parameters:**
- `competition_code` (required): `PL`, `PD`, `BL1`, or `SA`
- `season` (optional): season start year, default `2024`

**Example questions:**
- "Who is top of the Premier League?"
- "Show me the La Liga table"
- "What is the points gap between 1st and 2nd in the Bundesliga?"

---

### `get_bottler_index`
Returns teams ranked by how often they drop points from winning half-time positions.

**Parameters:**
- `competition_code` (required): `PL`, `PD`, `BL1`, or `SA`
- `min_matches_leading` (optional): minimum HT leads to qualify, default `3`

**Example questions:**
- "Which Premier League teams bottle the most leads?"
- "Who has the worst second-half record in La Liga?"
- "Which teams can't hold on to half-time advantages?"

---

### `get_team_form`
Returns a team's last N matches with results, goals and rolling form points.

**Parameters:**
- `team_name` (required): team name or partial name (case-insensitive)
- `last_n_games` (optional): number of recent games, default `5`

**Example questions:**
- "What is Arsenal's recent form?"
- "Show me Liverpool's last 10 games"
- "How has Barcelona been playing lately?"

---

### `get_head_to_head`
Returns historical results between two specific teams.

**Parameters:**
- `team_a` (required): first team name or partial name
- `team_b` (required): second team name or partial name

**Example questions:**
- "What is the head to head record between Arsenal and Chelsea?"
- "How many times have Real Madrid beaten Barcelona this season?"
- "Show me all Manchester City vs Liverpool results"

---

### `get_high_scoring_matches`
Returns matches with the most total goals.

**Parameters:**
- `competition_code` (optional): filter by league
- `min_goals` (optional): minimum total goals, default `5`
- `limit` (optional): number of matches to return, default `10`

**Example questions:**
- "What were the highest scoring matches this season?"
- "Show me all Premier League games with 5 or more goals"
- "What was the biggest scoreline in the Bundesliga?"

---

### `get_season_summary`
Returns high-level statistics for a competition — total goals, results
distribution, average goals per game.

**Parameters:**
- `competition_code` (required): `PL`, `PD`, `BL1`, or `SA`

**Example questions:**
- "Give me an overview of the Premier League season"
- "How many goals have been scored in La Liga this season?"
- "What percentage of Bundesliga games end in a home win?"
