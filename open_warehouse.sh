#!/usr/bin/env bash
# Open the DuckDB warehouse with the delta extension loaded and the working dir
# set to dbt/ so the silver views' relative delta_scan('../data/bronze/...') paths
# resolve.
#
#   ./open_warehouse.sh          # interactive CLI shell
#   ./open_warehouse.sh -ui      # browser UI (opens http://localhost:4213)
#
# Then query, e.g.:
#   SELECT * FROM main.stg_fbref_schedule LIMIT 5;
set -euo pipefail
cd "$(dirname "$0")/dbt"
exec duckdb -cmd "LOAD delta;" "$@" ../data/warehouse.duckdb
