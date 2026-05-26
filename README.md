# football_analysis

A learn-by-building football analytics project covering the full modern data and AI engineering stack. Ingests match data from football-data.org via Apache Airflow, transforms it through a medallion architecture using dbt and PostgreSQL, then layers an AI tier on top — including a Claude-powered Q&A agent with tool calling, RAG over match reports via pgvector, natural language to SQL, auto-generated match narratives, anomaly detection, and an evaluation framework. Built without frameworks like LangChain — every pattern implemented from first principles using the Anthropic SDK directly.

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Airflow, football-data.org API |
| Storage | PostgreSQL 15 (pgvector/pgvector:pg15) |
| Transformation | dbt (staging → intermediate → marts) |
| AI / LLM | Anthropic Claude (claude-sonnet-4-6) |
| Embeddings | Voyage AI (voyage-3, 1024 dims) |
| Vector search | pgvector |
| MCP server | mcp Python SDK, deployed on Fly.io |
| Testing | pytest |

---

## Project structure

```
football_analysis/
├── src/
│   └── football_analytics/             # Installable Python package
│       ├── config.py                   # Unified DB config (local + Supabase)
│       ├── cli.py                      # CLI entry point (football-analytics ask)
│       ├── agent/
│       │   ├── tool_handlers.py        # Shared tool logic (agent + MCP server)
│       │   ├── football_agent.py       # Agentic loop — 9 tools, Claude tool use
│       │   ├── nl_to_sql.py            # NL → SQL pipeline with validation & retry
│       │   ├── rag_retrieval.py        # Semantic search over match narratives
│       │   ├── generate_and_store_narratives.py  # Generates & embeds match reports
│       │   ├── match_narratives.py     # Single-match narrative generation
│       │   ├── anomaly_detection.py    # Structured anomaly detection via tool use
│       │   └── evaluation.py          # Three-layer evaluation framework
│       └── mcp/
│           └── server.py              # MCP server over SSE
├── airflow/
│   ├── dags/football_ingestion.py     # Airflow DAG — fetches & loads match data
│   └── plugins/football_api_client.py # football-data.org API wrapper
├── dbt/
│   └── models/
│       ├── staging/                   # Raw → typed source models
│       ├── intermediate/              # int_team_form rolling window
│       └── marts/                     # mart_match_results, mart_bottler_index,
│                                      # match_reports, report_embeddings
├── sql/
│   └── init.sql                       # Database schema
├── tests/
├── mcp_entrypoint.py                  # Uvicorn entry point for the MCP server
├── docker-compose.yml                 # PostgreSQL + pgvector
└── pyproject.toml
```

---

## Data

| Code | Competition |
|---|---|
| PL | Premier League |
| PD | La Liga |
| BL1 | Bundesliga |
| SA | Serie A |
| CL | Champions League (team performance queries only) |

Covers the **2024/25 and 2025/26 seasons**.

**Not available:** individual player stats (xG, assists, minutes), transfer data, injury data, future fixtures.

---

## Use the live MCP endpoint

The MCP server is publicly deployed. No local setup required — just point any MCP-compatible client at:

```
https://mcp-touchline.fly.dev/sse
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "football-analytics": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://mcp-touchline.fly.dev/sse"]
    }
  }
}
```

### Claude Code (VS Code extension)

Add to `~/.claude.json` under your project entry:

```json
{
  "mcpServers": {
    "football-analytics": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://mcp-touchline.fly.dev/sse"]
    }
  }
}
```

---

## AI features

### Football agent (`src/football_analytics/agent/football_agent.py`)
Claude agent with a 9-tool agentic loop. Answers natural language questions about match results, standings, form, and narratives.

| Tool | Purpose |
|---|---|
| `resolve_season` | Converts natural language season references to a year |
| `get_league_table` | Live standings computed from match results |
| `get_team_season_stats` | Full-season W/D/L/goals with home & away splits |
| `get_team_form` | Last N matches with rolling form points |
| `get_bottler_index` | Teams ranked by half-time lead drop rate |
| `get_head_to_head` | Historical results between two teams |
| `get_high_scoring_matches` | Matches filtered by total goals |
| `get_season_summary` | Competition-level aggregate stats |
| `search_match_reports` | Semantic RAG search over match narratives |
| `nl_to_sql` | Ad-hoc questions via generated SQL (agent only) |

### NL-to-SQL (`src/football_analytics/agent/nl_to_sql.py`)
Translates natural language to validated, executable PostgreSQL. Pipeline: generate → validate (regex word-boundary checks + sqlparse type check) → execute → interpret. Retries up to 2× on failure with error context fed back to the model.

### RAG pipeline (`src/football_analytics/agent/rag_retrieval.py`, `generate_and_store_narratives.py`)
Match narratives generated by Claude (700 tokens, temperature 0.8), chunked at sentence boundaries, embedded with Voyage AI voyage-3 (1024 dims), and stored in pgvector. Cosine similarity search with a 0.4 threshold.

### Anomaly detection (`src/football_analytics/agent/anomaly_detection.py`)
Structured anomaly detection using Claude tool use with forced `tool_choice`. Returns `is_anomalous`, `severity` (low/medium/high/none), `anomaly_type`, and `explanation` for each team.

### Evaluation framework (`src/football_analytics/agent/evaluation.py`)
Three-layer evaluation:
1. **Deterministic checks** — hard rules (SQL validity, required keys, forbidden phrases, response length)
2. **LLM-as-judge** — separate Claude call scoring grounding, responsiveness, and accuracy (1–5 each)
3. **Golden dataset** — 25 curated test cases covering factual lookups, cross-tool reasoning, regression tests, and graceful refusals. Pass threshold: 85%

### MCP server (`src/football_analytics/mcp/server.py`)
Exposes the nine DB tools as an MCP server over SSE so any MCP-compatible client can query the football database directly. Deployed on Fly.io.

---

## Local setup

### Prerequisites
- Docker
- Python 3.11+
- Anthropic API key
- Voyage AI API key
- football-data.org API key (free tier)

### Start the database

```bash
docker-compose up -d
```

### Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Environment variables

Copy and fill in `.env`:

```bash
cp .env.example .env
```

For local development (docker-compose):
```
ANTHROPIC_API_KEY=...
VOYAGE_API_KEY=...
FOOTBALL_API_KEY=...
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=...
```

For production (Supabase — takes precedence over `DB_*` vars if set):
```
SUPABASE_HOST=...
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=...
```

### Run dbt transformations

```bash
cd dbt && dbt run
```

### Generate match narratives and embeddings

```bash
python -m football_analytics.agent.generate_and_store_narratives
```

---

## Running the agent

```bash
# CLI
football-analytics ask "Who are the biggest bottlers in the Bundesliga?"
football-analytics ask "Compare Liverpool and Arsenal this season" --verbose

# Direct
python -m football_analytics.agent.football_agent
```

## Running evaluation

```bash
python -m football_analytics.agent.evaluation
```

## Running tests

```bash
pytest
```

Unit tests run without a database connection. Integration tests (marked with the `db_conn` fixture) require a live database.
