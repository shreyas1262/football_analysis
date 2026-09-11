#!/usr/bin/env python3
"""Bronze layer ingestion from FBref via soccerdata into Delta Lake.

Usage:
    python soccerdata/ingest.py                          # default seasons
    python soccerdata/ingest.py --seasons 2324,2425      # specific seasons
    python soccerdata/ingest.py --skip-match-level       # skip slow per-match tables
"""
import argparse
import os
import re
import signal
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import soccerdata as sd
from dotenv import load_dotenv
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv(Path(__file__).parent.parent / ".env")

BRONZE_PATH = Path(__file__).parent.parent / "data" / "bronze"

LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "GER-Bundesliga",
    "ITA-Serie A",
    "FRA-Ligue 1",
]

DEFAULT_SEASONS = ["2324", "2425"]

# Primary keys used for MERGE (CDC) on subsequent runs
PRIMARY_KEYS: dict[str, list[str]] = {
    "fbref_leagues": ["league"],
    "fbref_seasons": ["league", "season"],
    "fbref_schedule": ["game_id"],
    "fbref_team_season_stats_standard": ["league", "season", "team"],
    "fbref_team_season_stats_shooting": ["league", "season", "team"],
    "fbref_team_season_stats_keeper": ["league", "season", "team"],
    "fbref_team_season_stats_playing_time": ["league", "season", "team"],
    "fbref_team_season_stats_misc": ["league", "season", "team"],
    "fbref_team_match_stats_schedule": ["league", "season", "team", "game"],
    "fbref_team_match_stats_shooting": ["league", "season", "team", "game"],
    "fbref_team_match_stats_keeper": ["league", "season", "team", "game"],
    "fbref_team_match_stats_misc": ["league", "season", "team", "game"],
    "fbref_player_season_stats_standard": ["league", "season", "team", "player"],
    "fbref_player_season_stats_shooting": ["league", "season", "team", "player"],
    "fbref_player_season_stats_keeper": ["league", "season", "team", "player"],
    "fbref_player_season_stats_playing_time": ["league", "season", "team", "player"],
    "fbref_player_season_stats_misc": ["league", "season", "team", "player"],
    "fbref_player_match_stats": ["league", "season", "game", "team", "player"],
    "fbref_lineups": ["league", "season", "game", "team", "player"],
    "fbref_events": ["event_id"],  # surrogate key — see flatten_pandas
}


def get_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("football-analytics-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        # Only overwrite partitions present in the incoming data, not the whole table
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def pandas_to_spark_schema(df: pd.DataFrame, existing_schema: StructType | None = None) -> StructType:
    """Build an explicit Spark schema from a pandas DataFrame.

    Defaults all non-numeric/non-datetime columns to StringType so that
    all-None columns don't cause type inference failures.

    If `existing_schema` is given (the target Delta table's current schema),
    reuse its type for any column already present there instead of
    re-inferring — otherwise a column that happens to be entirely null in
    this batch (e.g. a stat FBref hasn't backfilled yet for a new season)
    would infer as StringType and fail DELTA_FAILED_TO_MERGE_FIELDS against
    the table's established type (e.g. BIGINT).
    """
    existing_types = {f.name: f.dataType for f in existing_schema.fields} if existing_schema else {}
    fields = []
    for col in df.columns:
        if col in existing_types:
            spark_type = existing_types[col]
            fields.append(StructField(col, spark_type, nullable=True))
            continue
        sample = next((v for v in df[col] if v is not None and pd.notna(v)), None)
        if pd.api.types.is_datetime64_any_dtype(df[col].dtype):
            spark_type = TimestampType()
        elif isinstance(sample, bool):
            spark_type = BooleanType()
        elif isinstance(sample, int):
            spark_type = LongType()
        elif isinstance(sample, float):
            spark_type = DoubleType()
        else:
            spark_type = StringType()
        fields.append(StructField(col, spark_type, nullable=True))
    return StructType(fields)


def flatten_pandas(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Reset MultiIndex and normalize column names for Spark compatibility."""
    df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        cols = []
        for col in df.columns:
            parts = [str(c) for c in col if c and "Unnamed" not in str(c)]
            cols.append("_".join(parts) if parts else str(col[-1]))
        df.columns = cols

    clean = []
    for c in df.columns:
        c = c.lower()
        c = re.sub(r"[^a-z0-9_]", "_", c)
        c = re.sub(r"_+", "_", c)
        c = c.strip("_")
        if c and c[0].isdigit():
            c = "n_" + c
        clean.append(c)

    # Deduplicate column names
    seen: dict[str, int] = {}
    deduped = []
    for c in clean:
        if c in seen:
            seen[c] += 1
            deduped.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            deduped.append(c)
    df.columns = deduped

    # FBref's `age` column is sometimes "YY-DDD" (years-days, e.g. "28-332")
    # instead of a plain number — observed starting with the 2025-26 scrape,
    # while existing bronze data (2023-24, 2024-25) has it as a clean whole-year
    # float. Extract just the years component so the column stays a consistent
    # numeric type across every season — this also leaves plain-number ages
    # (old format) unchanged, since splitting on '-' with no '-' present just
    # returns the original value.
    if "age" in df.columns:
        df["age"] = df["age"].apply(
            lambda v: float(str(v).split("-")[0]) if pd.notna(v) and str(v).strip() != "" else None
        )

    # Surrogate key for events (no clean natural key)
    if table == "fbref_events":
        import hashlib
        key_cols = ["league", "season", "game", "team", "minute", "event_type"]
        available = [c for c in key_cols if c in df.columns]
        df["event_id"] = df[available].astype(str).agg("_".join, axis=1).apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )

    # Convert pd.NA in nullable extension integer types (Int64, Int32, etc.)
    # to Python int/None — PySpark cannot handle pd.NA as a null value.
    for col in df.columns:
        if pd.api.types.is_extension_array_dtype(df[col]) and pd.api.types.is_integer_dtype(df[col]):
            df[col] = [None if pd.isna(v) else int(v) for v in df[col]]

    # Force-convert all non-numeric/non-datetime columns to native Python str.
    # astype(str) skips the conversion for string-like dtypes in pandas 3;
    # map(lambda) calls str() on every Python object, including lxml's
    # _ElementUnicodeResult subclass that PySpark cannot infer a type for.
    for col in df.columns:
        if not (
            pd.api.types.is_numeric_dtype(df[col])
            or pd.api.types.is_datetime64_any_dtype(df[col])
            or pd.api.types.is_bool_dtype(df[col])
        ):
            df[col] = df[col].map(lambda x: str(x) if pd.notna(x) else None)

    df["ingested_at"] = datetime.now(timezone.utc)
    return df


def coerce_to_schema(df: pd.DataFrame, schema: StructType) -> pd.DataFrame:
    """Coerce column values to match the declared Spark type.

    PySpark's schema verifier is strict and will not auto-widen: a Python `int`
    is rejected by a DoubleType field ("DoubleType() can not accept object 7"),
    even though the value is perfectly representable. This bites whenever we
    reuse an existing Delta table's schema (see pandas_to_spark_schema) and the
    incoming batch happens to produce a different-but-compatible Python type
    than the one the table was originally created with.
    """
    converters = {
        DoubleType: float,
        LongType: int,
        StringType: str,
        BooleanType: bool,
    }
    for field in schema.fields:
        col = field.name
        if col not in df.columns:
            continue
        convert = next(
            (fn for t, fn in converters.items() if isinstance(field.dataType, t)), None
        )
        if convert is None:
            continue
        # dtype=object is essential: a plain list assignment lets pandas re-infer
        # the dtype, which silently undoes the conversion — assigning [180, None]
        # to a column yields float64, turning the int 180 back into 180.0 and
        # re-triggering the very error this function exists to prevent.
        df[col] = pd.Series(
            [None if pd.isna(v) else convert(v) for v in df[col]],
            dtype=object,
            index=df.index,
        )
    return df


def store(pandas_df: pd.DataFrame, table: str, spark) -> None:
    from delta.tables import DeltaTable

    df = flatten_pandas(pandas_df, table)

    path = str(BRONZE_PATH / table)
    existing_schema = None
    if DeltaTable.isDeltaTable(spark, path):
        existing_schema = spark.read.format("delta").load(path).schema

    schema = pandas_to_spark_schema(df, existing_schema)
    df = coerce_to_schema(df, schema)
    spark_df = spark.createDataFrame(df, schema=schema)

    partition_cols = [c for c in ["league", "season"] if c in spark_df.columns]

    if partition_cols:
        # Fact tables: dynamic partition overwrite — only rewrites the partitions
        # present in this batch, leaving all other seasons/leagues untouched.
        (
            spark_df.write.format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .save(path)
        )
        print(f"  ✓ {table} — partitions overwritten")
    else:
        # Dimension tables (leagues, seasons): MERGE so new entries are added
        # without wiping existing rows.
        pks = PRIMARY_KEYS.get(table, [])
        if DeltaTable.isDeltaTable(spark, path) and pks:
            delta_table = DeltaTable.forPath(spark, path)
            merge_condition = " AND ".join(f"target.{k} = source.{k}" for k in pks)
            (
                delta_table.alias("target")
                .merge(spark_df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            print(f"  ✓ {table} — merged")
        else:
            spark_df.write.format("delta").mode("overwrite").save(path)
            print(f"  ✓ {table} — {spark_df.count():,} rows written")


def ingest_dimensions(fbref: sd.FBref, spark) -> None:
    print("--- Dimension tables ---")
    store(fbref.read_leagues(), "fbref_leagues", spark)
    store(fbref.read_seasons(), "fbref_seasons", spark)


def ingest_schedule(fbref: sd.FBref, spark) -> None:
    print("--- Schedule (scores + xG) ---")
    store(fbref.read_schedule(), "fbref_schedule", spark)


def ingest_team_season_stats(fbref: sd.FBref, spark) -> None:
    print("--- Team season stats ---")
    for stat_type in ["standard", "shooting", "keeper", "playing_time", "misc"]:
        print(f"  {stat_type}...")
        store(fbref.read_team_season_stats(stat_type), f"fbref_team_season_stats_{stat_type}", spark)


TEAM_MATCH_STAT_TYPES = ["schedule", "shooting", "keeper", "misc"]
# FBref/soccerdata occasionally hangs indefinitely (bot-detection challenge that
# headless Selenium can never resolve) rather than erroring. There's no reliable
# way to tell in advance which (league, stat_type) will hang, so every request is
# isolated in its own subprocess with a hard wall-clock timeout — a hang costs at
# most this long, not the whole run.
#
# Sizing note: one league fetches ~18-20 team pages sequentially, and FBref page
# latency swings enormously with Cloudflare challenge mode — ~10s/page when it's
# off, ~5.5min/page when it's on (measured Sep 2026). At the slow end a 20-team
# league needs ~110 min, so anything under ~2h kills healthy work on a bad day.
# Sized to cover the worst observed case without needing per-season retuning.
# Note that a timeout is not catastrophic: soccerdata caches every fetched page,
# so a re-run resumes from where it stopped rather than starting over.
TEAM_MATCH_TIMEOUT_SECS = 7200  # 2 hours


def ingest_team_match_stats(
    fbref: sd.FBref, spark, seasons: list[str], exclude_leagues: list[str] | None = None
) -> None:
    print("--- Team match stats (one league at a time, timeout-bounded) ---")
    exclude_leagues = exclude_leagues or []
    included_leagues = [l for l in LEAGUES if l not in exclude_leagues]
    if exclude_leagues:
        print(f"  (skipping {', '.join(exclude_leagues)} — excluded via --exclude-team-match-leagues)")

    skipped: list[tuple[str, str]] = []
    for stat_type in TEAM_MATCH_STAT_TYPES:
        for league in included_leagues:
            label = f"{league} / {stat_type}"
            print(f"  {label} ... ", end="", flush=True)
            proc = subprocess.Popen(
                [
                    sys.executable, str(Path(__file__).resolve()),
                    "--seasons", ",".join(seasons),
                    "--team-match-worker-league", league,
                    "--team-match-worker-stat-type", stat_type,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,  # own process group, so a timeout can kill chromedriver/chrome too
            )
            try:
                output, _ = proc.communicate(timeout=TEAM_MATCH_TIMEOUT_SECS)
                if proc.returncode == 0:
                    print("✓")
                else:
                    print(f"✗ (exit {proc.returncode})")
                    print(output[-2000:])
                    skipped.append((league, stat_type))
            except subprocess.TimeoutExpired:
                print(f"✗ timed out after {TEAM_MATCH_TIMEOUT_SECS}s — killing and skipping")
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait()
                skipped.append((league, stat_type))

    if skipped:
        print(f"\n  Skipped {len(skipped)} (league, stat_type) combo(s):")
        for league, stat_type in skipped:
            print(f"    - {league} / {stat_type}")
        print("  Re-run ingest.py later to retry just these — CDC only rewrites missing partitions.")


@contextmanager
def _season_pinned_urls(fbref: sd.FBref):
    """Force season-explicit URLs while reading player season stats.

    FBref omits the season segment from the URL of whatever it currently
    considers the *latest* season — `/en/comps/9/Premier-League-Stats` rather
    than `/en/comps/9/2025-2026/2025-2026-Premier-League-Stats`. That bare URL
    always resolves to *today's* season.

    soccerdata's `read_team_match_stats` handles this (it has an explicit
    "special case: latest season" branch that rebuilds the URL), but
    `read_player_season_stats` concatenates `season.url` as-is. The result is
    silent and nasty: the most recent season's player stats come back holding
    the *current* season's partial data, labelled with the season you asked
    for. Observed Sep 2026 — "2025-26" player stats contained ~4 matchdays of
    2026-27, including newly promoted clubs that never played in 2025-26.

    This patches `read_seasons` for the duration of the player fetch so the URL
    always names its season. Scoped narrowly, and restored afterwards, so no
    other reader's behaviour changes.
    """
    original = fbref.read_seasons

    def patched(*args, **kwargs):
        df = original(*args, **kwargs).copy()
        urls = []
        for (_lkey, skey), row in df.iterrows():
            parts = row.url.strip("/").split("/")
            # 4 parts => season segment absent (…/comps/<id>/<Name>-Stats)
            # 5 parts => season already present, leave it alone
            if len(parts) == 4 and len(skey) == 4 and skey.isdigit():
                y1 = datetime.strptime(skey[:2], "%y").year
                y2 = datetime.strptime(skey[2:], "%y").year
                season_fmt = f"{y1}-{y2}"
                urls.append(f"/{'/'.join(parts[:-1])}/{season_fmt}/{season_fmt}-{parts[-1]}")
            else:
                urls.append(row.url)
        df["url"] = urls
        return df

    fbref.read_seasons = patched
    try:
        yield fbref
    finally:
        fbref.read_seasons = original


def ingest_player_season_stats(fbref: sd.FBref, spark) -> None:
    print("--- Player season stats ---")
    with _season_pinned_urls(fbref):
        for stat_type in ["standard", "shooting", "keeper", "playing_time", "misc"]:
            print(f"  {stat_type}...")
            store(fbref.read_player_season_stats(stat_type), f"fbref_player_season_stats_{stat_type}", spark)


def ingest_match_level(fbref: sd.FBref, spark) -> None:
    print("--- Match-level tables (slow — one page per match) ---")
    print("  player match stats...")
    store(fbref.read_player_match_stats("summary"), "fbref_player_match_stats", spark)
    print("  lineups...")
    store(fbref.read_lineup(), "fbref_lineups", spark)
    print("  events...")
    store(fbref.read_events(), "fbref_events", spark)


def main():
    parser = argparse.ArgumentParser(description="Ingest FBref data into Delta Lake bronze layer")
    parser.add_argument(
        "--seasons",
        default=",".join(DEFAULT_SEASONS),
        help='Comma-separated season codes e.g. "2324,2425"',
    )
    parser.add_argument(
        "--skip-match-level",
        action="store_true",
        help="Skip per-match tables (player_match_stats, lineups, events) — slow",
    )
    parser.add_argument(
        "--exclude-team-match-leagues",
        default="",
        help='Comma-separated league names to skip for team_match_stats only — a '
             'workaround for a league-specific scrape hang, e.g. "ITA-Serie A". '
             'All other tables still ingest that league normally.',
    )
    # Internal — spawned by ingest_team_match_stats to isolate one (league, stat_type)
    # request in its own process so a hang can be killed on a timeout without losing
    # the rest of the run. Not meant to be passed by hand.
    parser.add_argument("--team-match-worker-league", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--team-match-worker-stat-type", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    seasons = [s.strip() for s in args.seasons.split(",")]

    if args.team_match_worker_league and args.team_match_worker_stat_type:
        # Isolated worker mode: fetch + store exactly one (league, stat_type) slice
        # of team_match_stats, then exit. Runs as its own OS process (see
        # ingest_team_match_stats) so the parent can enforce a hard timeout.
        spark = get_spark()
        worker_fbref = sd.FBref(leagues=[args.team_match_worker_league], seasons=seasons)
        df = worker_fbref.read_team_match_stats(args.team_match_worker_stat_type)
        store(df, f"fbref_team_match_stats_{args.team_match_worker_stat_type}", spark)
        spark.stop()
        return

    exclude_team_match_leagues = [
        l.strip() for l in args.exclude_team_match_leagues.split(",") if l.strip()
    ]
    print(f"Leagues : {', '.join(LEAGUES)}")
    print(f"Seasons : {', '.join(seasons)}")
    if exclude_team_match_leagues:
        print(f"Excluding from team_match_stats: {', '.join(exclude_team_match_leagues)}")
    print(f"Output  : {BRONZE_PATH}\n")

    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    spark = get_spark()
    fbref = sd.FBref(leagues=LEAGUES, seasons=seasons)

    ingest_dimensions(fbref, spark)
    ingest_schedule(fbref, spark)
    ingest_team_season_stats(fbref, spark)
    ingest_team_match_stats(fbref, spark, seasons, exclude_leagues=exclude_team_match_leagues)
    ingest_player_season_stats(fbref, spark)

    if not args.skip_match_level:
        ingest_match_level(fbref, spark)
    else:
        print("--- Skipping match-level tables ---")

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()
