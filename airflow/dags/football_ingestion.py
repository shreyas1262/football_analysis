import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

from football_api_client import FootballAPIClient

logger = logging.getLogger(__name__)

COMPETITIONS = ["PL", "PD", "BL1", "SA", "FL1", "CL"]
CURRENT_SEASON = 2024

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _write_ingestion_log(conn, dag_id, task_id, entity_type, records_ingested,
                         status, error_message, started_at, finished_at):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.ingestion_log
                (dag_id, task_id, entity_type, records_ingested, status,
                 error_message, started_at, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (dag_id, task_id, entity_type, records_ingested, status,
             error_message, started_at, finished_at),
        )
    conn.commit()


@dag(
    dag_id="football_data_ingestion",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["football", "ingestion"],
    default_args=default_args,
)
def football_data_ingestion():

    @task()
    def ingest_competitions(**context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        started_at = datetime.utcnow()
        count = 0
        error_message = None
        status = "success"

        hook = PostgresHook(postgres_conn_id="football_postgres")
        conn = hook.get_conn()
        client = FootballAPIClient()

        try:
            competitions = client.get_competitions()
            with conn.cursor() as cur:
                for comp in competitions:
                    cur.execute(
                        """
                        INSERT INTO raw.competitions
                            (id, name, code, type, area_name, plan, raw_payload, ingested_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            name        = EXCLUDED.name,
                            code        = EXCLUDED.code,
                            type        = EXCLUDED.type,
                            area_name   = EXCLUDED.area_name,
                            plan        = EXCLUDED.plan,
                            raw_payload = EXCLUDED.raw_payload,
                            ingested_at = EXCLUDED.ingested_at
                        """,
                        (
                            comp["id"],
                            comp.get("name"),
                            comp.get("code"),
                            comp.get("type"),
                            comp.get("area", {}).get("name"),
                            comp.get("plan"),
                            json.dumps(comp),
                        ),
                    )
                    count += 1
            conn.commit()
            logger.info("Upserted %d competitions", count)
        except Exception as exc:
            conn.rollback()
            status = "failure"
            error_message = str(exc)
            logger.exception("ingest_competitions failed")
            raise
        finally:
            _write_ingestion_log(
                conn, dag_id, task_id, "competitions", count,
                status, error_message, started_at, datetime.utcnow(),
            )
            conn.close()

        return count

    @task()
    def ingest_teams(**context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        started_at = datetime.utcnow()
        count = 0
        error_message = None
        status = "success"

        hook = PostgresHook(postgres_conn_id="football_postgres")
        conn = hook.get_conn()
        client = FootballAPIClient()

        try:
            for code in COMPETITIONS:
                try:
                    teams = client.get_teams(code, season=CURRENT_SEASON)
                except Exception as exc:
                    logger.warning("Failed to fetch teams for %s: %s", code, exc)
                    continue

                with conn.cursor() as cur:
                    for team in teams:
                        cur.execute(
                            """
                            INSERT INTO raw.teams
                                (id, name, short_name, tla, competition_id,
                                 area_name, raw_payload, ingested_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (id) DO UPDATE SET
                                name           = EXCLUDED.name,
                                short_name     = EXCLUDED.short_name,
                                tla            = EXCLUDED.tla,
                                competition_id = EXCLUDED.competition_id,
                                area_name      = EXCLUDED.area_name,
                                raw_payload    = EXCLUDED.raw_payload,
                                ingested_at    = EXCLUDED.ingested_at
                            """,
                            (
                                team["id"],
                                team.get("name"),
                                team.get("shortName"),
                                team.get("tla"),
                                team.get("_competition_id"),
                                team.get("area", {}).get("name"),
                                json.dumps(team),
                            ),
                        )
                        count += 1
                conn.commit()
                logger.info("Upserted teams for %s (running total: %d)", code, count)
        except Exception as exc:
            conn.rollback()
            status = "failure"
            error_message = str(exc)
            logger.exception("ingest_teams failed")
            raise
        finally:
            _write_ingestion_log(
                conn, dag_id, task_id, "teams", count,
                status, error_message, started_at, datetime.utcnow(),
            )
            conn.close()

        return count

    @task()
    def ingest_players(**context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        started_at = datetime.utcnow()
        count = 0
        error_message = None
        status = "success"

        hook = PostgresHook(postgres_conn_id="football_postgres")
        conn = hook.get_conn()
        client = FootballAPIClient()

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM raw.teams")
                team_ids = [row[0] for row in cur.fetchall()]

            logger.info("Fetching squads for %d teams", len(team_ids))
            for team_id in team_ids:
                try:
                    squad = client.get_squad(team_id)
                except Exception as exc:
                    logger.warning("Failed to fetch squad for team %d: %s", team_id, exc)
                    continue

                with conn.cursor() as cur:
                    for player in squad:
                        dob_raw = player.get("dateOfBirth")
                        try:
                            date_of_birth = datetime.strptime(dob_raw, "%Y-%m-%d").date() if dob_raw else None
                        except (ValueError, TypeError):
                            date_of_birth = None

                        cur.execute(
                            """
                            INSERT INTO raw.players (
                                id, name, first_name, last_name, date_of_birth,
                                nationality, position, shirt_number, team_id,
                                raw_payload, ingested_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (id) DO UPDATE SET
                                name          = EXCLUDED.name,
                                first_name    = EXCLUDED.first_name,
                                last_name     = EXCLUDED.last_name,
                                date_of_birth = EXCLUDED.date_of_birth,
                                nationality   = EXCLUDED.nationality,
                                position      = EXCLUDED.position,
                                shirt_number  = EXCLUDED.shirt_number,
                                team_id       = EXCLUDED.team_id,
                                raw_payload   = EXCLUDED.raw_payload,
                                ingested_at   = EXCLUDED.ingested_at
                            """,
                            (
                                player["id"],
                                player.get("name"),
                                player.get("firstName"),
                                player.get("lastName"),
                                date_of_birth,
                                player.get("nationality"),
                                player.get("position"),
                                player.get("shirtNumber"),
                                player.get("_team_id"),
                                json.dumps(player),
                            ),
                        )
                        count += 1
                conn.commit()
                logger.info("Upserted squad for team %d (running total: %d)", team_id, count)
        except Exception as exc:
            status = "failure"
            error_message = str(exc)
            logger.exception("ingest_players failed")
            raise
        finally:
            _write_ingestion_log(
                conn, dag_id, task_id, "players", count,
                status, error_message, started_at, datetime.utcnow(),
            )
            conn.close()

        return count

    @task()
    def ingest_matches(**context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        started_at = datetime.utcnow()
        count = 0
        error_message = None
        status = "success"

        hook = PostgresHook(postgres_conn_id="football_postgres")
        conn = hook.get_conn()
        client = FootballAPIClient()

        try:
            for code in COMPETITIONS:
                try:
                    matches = client.get_matches(code, season=CURRENT_SEASON)
                except Exception as exc:
                    logger.warning("Failed to fetch matches for %s: %s", code, exc)
                    continue

                with conn.cursor() as cur:
                    for match in matches:
                        score = match.get("score", {})
                        full_time = score.get("fullTime") or {}
                        half_time = score.get("halfTime") or {}

                        utc_date = match.get("utcDate")
                        if utc_date:
                            utc_date = datetime.fromisoformat(utc_date.replace("Z", "+00:00"))

                        cur.execute(
                            """
                            INSERT INTO raw.matches (
                                id, competition_id, season_id, utc_date, status,
                                matchday, home_team_id, home_team_name,
                                away_team_id, away_team_name,
                                home_score_full_time, away_score_full_time,
                                home_score_half_time, away_score_half_time,
                                winner, raw_payload, ingested_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, NOW()
                            )
                            ON CONFLICT (id) DO UPDATE SET
                                competition_id        = EXCLUDED.competition_id,
                                season_id             = EXCLUDED.season_id,
                                utc_date              = EXCLUDED.utc_date,
                                status                = EXCLUDED.status,
                                matchday              = EXCLUDED.matchday,
                                home_team_id          = EXCLUDED.home_team_id,
                                home_team_name        = EXCLUDED.home_team_name,
                                away_team_id          = EXCLUDED.away_team_id,
                                away_team_name        = EXCLUDED.away_team_name,
                                home_score_full_time  = EXCLUDED.home_score_full_time,
                                away_score_full_time  = EXCLUDED.away_score_full_time,
                                home_score_half_time  = EXCLUDED.home_score_half_time,
                                away_score_half_time  = EXCLUDED.away_score_half_time,
                                winner                = EXCLUDED.winner,
                                raw_payload           = EXCLUDED.raw_payload,
                                ingested_at           = EXCLUDED.ingested_at
                            """,
                            (
                                match["id"],
                                match.get("competition", {}).get("id"),
                                match.get("season", {}).get("id"),
                                utc_date,
                                match.get("status"),
                                match.get("matchday"),
                                match.get("homeTeam", {}).get("id"),
                                match.get("homeTeam", {}).get("name"),
                                match.get("awayTeam", {}).get("id"),
                                match.get("awayTeam", {}).get("name"),
                                full_time.get("home"),
                                full_time.get("away"),
                                half_time.get("home"),
                                half_time.get("away"),
                                score.get("winner"),
                                json.dumps(match),
                            ),
                        )
                        count += 1
                conn.commit()
                logger.info("Upserted matches for %s (running total: %d)", code, count)
        except Exception as exc:
            conn.rollback()
            status = "failure"
            error_message = str(exc)
            logger.exception("ingest_matches failed")
            raise
        finally:
            _write_ingestion_log(
                conn, dag_id, task_id, "matches", count,
                status, error_message, started_at, datetime.utcnow(),
            )
            conn.close()

        return count

    @task()
    def ingest_standings(**context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        started_at = datetime.utcnow()
        count = 0
        error_message = None
        status = "success"

        hook = PostgresHook(postgres_conn_id="football_postgres")
        conn = hook.get_conn()
        client = FootballAPIClient()

        try:
            for code in COMPETITIONS:
                try:
                    standings = client.get_standings(code, season=CURRENT_SEASON)
                except Exception as exc:
                    logger.warning("Failed to fetch standings for %s: %s", code, exc)
                    continue

                with conn.cursor() as cur:
                    for entry in standings:
                        cur.execute(
                            """
                            INSERT INTO raw.standings (
                                competition_id, season_id, team_id, team_name,
                                position, played_games, won, draw, lost, points,
                                goals_for, goals_against, goal_difference,
                                raw_payload, ingested_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, NOW()
                            )
                            ON CONFLICT (competition_id, season_id, team_id) DO UPDATE SET
                                team_name       = EXCLUDED.team_name,
                                position        = EXCLUDED.position,
                                played_games    = EXCLUDED.played_games,
                                won             = EXCLUDED.won,
                                draw            = EXCLUDED.draw,
                                lost            = EXCLUDED.lost,
                                points          = EXCLUDED.points,
                                goals_for       = EXCLUDED.goals_for,
                                goals_against   = EXCLUDED.goals_against,
                                goal_difference = EXCLUDED.goal_difference,
                                raw_payload     = EXCLUDED.raw_payload,
                                ingested_at     = EXCLUDED.ingested_at
                            """,
                            (
                                entry.get("_competition_id"),
                                entry.get("_season_id"),
                                entry.get("team", {}).get("id"),
                                entry.get("team", {}).get("name"),
                                entry.get("position"),
                                entry.get("playedGames"),
                                entry.get("won"),
                                entry.get("draw"),
                                entry.get("lost"),
                                entry.get("points"),
                                entry.get("goalsFor"),
                                entry.get("goalsAgainst"),
                                entry.get("goalDifference"),
                                json.dumps(entry),
                            ),
                        )
                        count += 1
                conn.commit()
                logger.info("Upserted standings for %s (running total: %d)", code, count)
        except Exception as exc:
            conn.rollback()
            status = "failure"
            error_message = str(exc)
            logger.exception("ingest_standings failed")
            raise
        finally:
            _write_ingestion_log(
                conn, dag_id, task_id, "standings", count,
                status, error_message, started_at, datetime.utcnow(),
            )
            conn.close()

        return count

    # --- dependency graph ---
    competitions = ingest_competitions()
    teams = ingest_teams()
    players = ingest_players()
    matches = ingest_matches()
    standings = ingest_standings()

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='docker compose exec -T dbt dbt run',
    )

    competitions >> teams >> players
    competitions >> matches
    competitions >> standings
    [players, matches, standings] >> run_dbt


football_data_ingestion()
