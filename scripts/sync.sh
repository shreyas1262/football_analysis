#!/bin/bash
# Full data pipeline: ingest → dbt run → dbt test → narratives
#
# Usage:
#   ./scripts/sync.sh                      # incremental (last 15 days)
#   ./scripts/sync.sh --full               # current + previous season
#   ./scripts/sync.sh --seasons 2023,2022  # specific seasons
#
# Run in background:
#   nohup ./scripts/sync.sh --seasons 2023 > logs/sync.log 2>&1 &

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*"; }

log "=== Starting sync pipeline ==="
log "Args: ${*:-none}"

log "--- Ingestion ---"
python "$REPO_ROOT/airflow/dags/football_ingestion.py" "$@"

log "--- dbt run ---"
(cd "$REPO_ROOT/dbt" && dbt run --profiles-dir ./ --profile ci)

log "--- dbt test ---"
(cd "$REPO_ROOT/dbt" && dbt test --profiles-dir ./ --profile ci)

log "--- Narratives & embeddings ---"
python "$REPO_ROOT/src/football_analytics/agent/generate_and_store_narratives.py" --yes

log "=== Sync complete ==="
