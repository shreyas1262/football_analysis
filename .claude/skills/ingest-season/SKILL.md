---
name: ingest-season
description: Ingest one or more FBref seasons through bronze (PySpark/Delta) and silver (dbt/DuckDB), with per-league match-count validation. Use when the user asks to add historical seasons, backfill data, or load more FBref years.
---

# Ingest a season through bronze → silver

Runs the full pipeline for new season(s) and validates the result before declaring success.

## Steps

1. **Confirm scope with the user** if not already given: which season code(s) (e.g. `2223` = 2022-23), and whether to include match-level tables (`--skip-match-level` is much faster and is the default recommendation unless the user needs player_match_stats/lineups/events).

2. **Check FBref data availability for the target season** before running:
   - Advanced stats (xG, progressive passes, pressures, SCA/GCA) only exist from roughly **2017-18** onward. Seasons before that will have those columns present but empty/null in bronze — this is expected, not a bug. Tell the user this up front if they're going back further than ~2017-18.
   - Older seasons (especially pre-2010s) are more likely to have missing referee/attendance data — don't treat nulls in those columns as ingestion failures.

3. **Run the bronze ingest, in the background, and watch for hangs (not just crashes):**
   ```bash
   .venv/bin/python soccerdata/ingest.py --seasons <codes> --skip-match-level
   ```
   Launch with `run_in_background: true` — full ingests regularly exceed the
   10-minute foreground timeout. Multiple seasons: comma-separated, e.g.
   `--seasons 2021,2122,2223`. For more than ~5 seasons at once, suggest
   batching (e.g. 5 at a time) so a mid-run problem doesn't cost all the
   progress.

   **⚠️ READ THIS BEFORE CONCLUDING ANYTHING IS HUNG.** A healthy ingest looks
   almost exactly like a hung one. This cost hours of wasted work once by
   repeatedly killing perfectly healthy runs — don't repeat it.

   A normal, working `team_match_stats` fetch:
   - writes **nothing** to `data/bronze` for **15-20+ minutes** — bronze is only
     written *after* all ~96 team pages for a stat_type are fetched
   - sits at **~0% CPU** — it's blocked on network I/O, which is not a hang signal
   - runs its browser as **`uc_driver`** (undetected-chromedriver), *not*
     `chromedriver` — grepping for `chromedriver` returns 0 and means nothing

   **The only reliable progress signal is the soccerdata HTML cache**, which
   gets a new file per request as work proceeds:
   ```bash
   # newest cached page — if this advances, the scrape is working, full stop
   find ~/soccerdata/data/FBref -type f -newermt "-20 minutes" | wc -l
   ls -lt ~/soccerdata/data/FBref | head -3
   ```
   FBref response time varies enormously — observed at ~10s/request on a good
   day and ~3-5 min/request on a bad one (Cloudflare challenge latency;
   soccerdata itself adds no delay, `rate_limit = 0`). At the slow end a full
   season is 25+ hours. Slow is not hung.

   **Only treat it as genuinely hung if no new cache file has appeared for
   30+ minutes.** Even then, check the captured output for a traceback first.

   `team_match_stats` is now **self-protecting** against this (see Note below):
   every `(league, stat_type)` request runs in its own subprocess with a hard
   5-minute timeout, so it can no longer hang the whole run — a stuck combo is
   killed, logged, and skipped automatically. **The other steps (dimensions,
   schedule, team_season_stats, player_season_stats, match-level) are still
   single long blocking calls with no internal timeout**, so you still need to
   watch those manually. Poll every ~15 minutes, checking the **cache**, not
   bronze, and not CPU:
   ```bash
   # THE progress signal: has any page been fetched recently?
   find ~/soccerdata/data/FBref -type f -newermt "-20 minutes" | wc -l
   ls -lt ~/soccerdata/data/FBref | head -3    # what it's working on now
   ```
   - New cache files appearing → **healthy**, regardless of bronze mtimes, CPU,
     or how long it's been. Leave it alone and check again later.
   - No new cache file for **30+ minutes** → possibly hung. Kill it **surgically** —
     never `pkill -f chromedriver` / `pkill -f "Google Chrome for Testing"`.
     Those match *every* chromedriver/Chrome process system-wide, including
     ones belonging to other still-healthy scrapes (your own concurrent test,
     or another instance of this pipeline), and can rip Chrome out from under
     a working session — which raises `selenium.common.exceptions.InvalidSessionIdException`
     there and looks exactly like a fresh, unrelated hang, when it was actually
     caused by the "cleanup." Kill only the target process's own children:
     ```bash
     # find chromedriver/Chrome processes that are actual descendants of <ingest_pid>
     pgrep -P <ingest_pid>          # direct children
     kill -9 <ingest_pid>           # SIGKILL, not plain kill/SIGTERM — don't wait on a stuck process
     # then kill any chromedriver/Chrome children found above, by PID, not by name
     .venv/bin/python soccerdata/ingest.py --seasons <codes> --skip-match-level   # same command, run_in_background
     ```
     CDC makes this safe to just re-run — tables already written (check bronze
     mtimes / the last `✓` line in the captured output) are redone quickly and
     idempotently; only the table it was stuck on actually re-scrapes.

   **Case study — how this went wrong once (Sep 2026).** Five separate runs
   across two days were each declared "hung" and killed, on different
   leagues/tables each time, and the shifting pattern was read as escalating
   FBref rate-limiting. Every one of those was actually a **healthy, working
   scrape**. The cache directory showed pages still being fetched — the last
   one landed 31 seconds before the final kill. The three signals used
   (`data/bronze` staleness at a 15-min threshold, 0% CPU, and zero
   `chromedriver` processes) are *all* normal for a working run, and the
   `InvalidSessionIdException` in the logs was self-inflicted by the cleanup
   `pkill`, not evidence of a problem. Real cost: hours lost, plus each kill
   discarded the in-flight stat_type's work.

   Lesson: **check `~/soccerdata/data/FBref` for new files before concluding
   anything.** Slow ≠ hung. If pages are still landing, the correct action is
   to wait — even if that means many hours.

4. **Run the silver build:**
   ```bash
   cd dbt && ../.venv/bin/dbt build
   ```
   All 34 tests must pass (16 models + tests). If a test fails, investigate before proceeding — don't ignore failures to "just get the season in."

5. **Validate match counts per league** for each newly-ingested season. Expected match count per league per season:

   | League | Teams | Expected matches/season |
   |---|---|---|
   | ENG-Premier League | 20 | 380 |
   | ESP-La Liga | 20 | 380 |
   | ITA-Serie A | 20 | 380 |
   | FRA-Ligue 1 | 18 (since 2023-24) | **308** |
   | GER-Bundesliga | 18 | **308** |

   Note on the 18-team leagues: the round-robin alone is 306 (18×17), but both
   Bundesliga and Ligue 1 add a **two-legged relegation/promotion playoff**, so
   308 is correct and 306 would indicate the playoff legs are missing. Verified
   against 2024-25 and 2025-26, which both show exactly 308. Those playoff legs
   are also where penalty-shootout scores like `(5) 0–3 (6)` show up in the
   `score` column — `stg_fbref_schedule` strips that notation before casting.

   Run this check from the `dbt/` directory (so the Delta relative paths resolve):
   ```bash
   ../.venv/bin/python -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb'); con.execute('LOAD delta;')
   df = con.execute('''
       select league, season, count(*) as matches
       from main.stg_fbref_schedule
       group by 1, 2 order by 1, 2
   ''').df()
   print(df.to_string())
   "
   ```
   - Flag any league-season that doesn't match the table above (380 for the
     20-team leagues, 308 for Bundesliga and Ligue 1).
   - **Before 2023-24, Ligue 1 had 20 teams** (380 matches) — it dropped to 18
     from 2023-24 onward. So expect 380 for older Ligue 1 seasons and 308 from
     2023-24 (verified: 2022-23 = 380, 2023-24 = 308). Bundesliga has been 18
     teams throughout.
   - **Play-offs add matches and are genuine, not errors.** They vary by season,
     so treat the base round-robin number as a floor, not an exact expectation.
     Verified cases:
     - **Bundesliga: consistently +2** (relegation/promotion play-off) → 308
     - **Ligue 1 2021-22 = 382** — 380 round-robin *plus* a 2-leg play-off that
       it did *not* have in 2022-23 (380). Same team count, different total.
     - **Serie A 2022-23 = 381** — Spezia and Hellas Verona finished level on
       points, forcing a one-off relegation tie-breaker (*spareggio*).

     Play-off rows carry a null `week` and a descriptive `round` (e.g.
     `"French 1/2 Relegation/Promotion play-offs"`, `"Relegation tie-breaker"`).
     Before treating an off-by-one as a scrape problem, check the `round`
     column:
     ```sql
     select round, week, count(*) from <schedule>
     where season = '<code>' and league = '<league>'
     group by 1,2 order by 3
     ```
   - A count that's noticeably *lower* than expected (e.g. mid-300s for a 380-expected league, or a season with under ~300 matches for any league) usually means the scrape was incomplete — check for a partial/crashed run and re-ingest that season.
   - A count of exactly 0 for an expected league-season means it didn't ingest at all — check the bronze ingest output for errors on that league.

6. **Report to the user**: which seasons were added, the match-count table, any leagues/seasons that deviated from expectations and why (genuine anomaly vs. known exception), and whether dbt tests passed.

## Notes

- This skill assumes `ingest.py`'s CDC (dynamic partition overwrite for facts, MERGE for dimensions) — re-running an already-ingested season is safe and just refreshes that partition, it won't duplicate data.
- After ingesting many new seasons, consider regenerating the data catalog (`catalog/generate_catalog.py`) so its row counts stay current.
- **`team_match_stats` isolation mechanism** (added after the 2025-26 hang):
  `ingest_team_match_stats` loops one `(league, stat_type)` combination at a
  time, spawning `ingest.py` itself as a subprocess in worker mode
  (`--team-match-worker-league`, `--team-match-worker-stat-type` — internal
  flags, not meant to be passed by hand) via `start_new_session=True` so the
  whole process group — worker + any chromedriver/Chrome it spawns — can be
  killed together. Each combo gets `TEAM_MATCH_TIMEOUT_SECS` (currently 7200s /
  2 hours) before it's force-killed and skipped; skipped combos are printed at
  the end and are safe to pick up on a later run (CDC only rewrites the
  partitions it actually receives data for).

  Sizing history, so this isn't re-litigated: 5 min killed *every* combo on a
  slow day; 30 min still killed the 20-team leagues; 60 min still killed
  `misc`. 2 hours covers the worst observed case (~110 min for a 20-team league
  at ~5.5 min/page). Timeouts are not catastrophic — soccerdata caches each
  fetched page, so a re-run resumes rather than restarting, and repeated runs
  converge.
- **`--exclude-team-match-leagues`**: comma-separated league names to skip
  entirely from `team_match_stats` for one run, e.g. `--exclude-team-match-leagues "ITA-Serie A"`.
  All other tables still ingest that league normally. Useful as a manual
  override if a league is *known* to be currently broken on FBref's side,
  separate from the automatic per-combo timeout above.
- **All-null-column schema bug** (fixed after the 2025-26 ingest): if a bronze
  column happens to be entirely null in a new season's batch (FBref hasn't
  backfilled that stat yet), naive type inference defaults it to `StringType`,
  which conflicts with an existing Delta table where that column is already
  typed (e.g. `BIGINT`) from prior seasons — `DELTA_FAILED_TO_MERGE_FIELDS`.
  `pandas_to_spark_schema()` now accepts the target Delta table's existing
  schema and reuses established types for known columns, only inferring fresh
  for genuinely new ones. If this error appears for a *new* column never seen
  before, that's a different, legitimate issue — investigate rather than
  assuming this fix covers it.
