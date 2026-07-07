#!/usr/bin/env python3
"""Bronze layer ingestion from FBref via soccerdata into Delta Lake.

Usage:
    python soccerdata/ingest.py                          # default seasons
    python soccerdata/ingest.py --seasons 2324,2425      # specific seasons
    python soccerdata/ingest.py --skip-match-level       # skip slow per-match tables
"""
import argparse
import re
import sys
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


def pandas_to_spark_schema(df: pd.DataFrame) -> StructType:
    """Build an explicit Spark schema from a pandas DataFrame.

    Defaults all non-numeric/non-datetime columns to StringType so that
    all-None columns don't cause type inference failures.
    """
    fields = []
    for col in df.columns:
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


def store(pandas_df: pd.DataFrame, table: str, spark) -> None:
    from delta.tables import DeltaTable

    df = flatten_pandas(pandas_df, table)
    spark_df = spark.createDataFrame(df, schema=pandas_to_spark_schema(df))

    path = str(BRONZE_PATH / table)
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


def ingest_team_match_stats(fbref: sd.FBref, spark) -> None:
    print("--- Team match stats ---")
    for stat_type in ["schedule", "shooting", "keeper", "misc"]:
        print(f"  {stat_type}...")
        store(fbref.read_team_match_stats(stat_type), f"fbref_team_match_stats_{stat_type}", spark)


def ingest_player_season_stats(fbref: sd.FBref, spark) -> None:
    print("--- Player season stats ---")
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
    args = parser.parse_args()

    seasons = [s.strip() for s in args.seasons.split(",")]
    print(f"Leagues : {', '.join(LEAGUES)}")
    print(f"Seasons : {', '.join(seasons)}")
    print(f"Output  : {BRONZE_PATH}\n")

    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    spark = get_spark()
    fbref = sd.FBref(leagues=LEAGUES, seasons=seasons)

    ingest_dimensions(fbref, spark)
    ingest_schedule(fbref, spark)
    ingest_team_season_stats(fbref, spark)
    ingest_team_match_stats(fbref, spark)
    ingest_player_season_stats(fbref, spark)

    if not args.skip_match_level:
        ingest_match_level(fbref, spark)
    else:
        print("--- Skipping match-level tables ---")

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()
