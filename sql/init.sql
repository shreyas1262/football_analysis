-- =============================================================================
-- Bootstrap script for football analytics PostgreSQL instance
-- Runs once on first container start via docker-entrypoint-initdb.d
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Users
-- -----------------------------------------------------------------------------
CREATE USER airflow  WITH PASSWORD 'airflow';
CREATE USER football WITH PASSWORD 'football';

-- -----------------------------------------------------------------------------
-- Databases
-- -----------------------------------------------------------------------------
CREATE DATABASE airflow   OWNER airflow;
CREATE DATABASE football_db OWNER football;

-- -----------------------------------------------------------------------------
-- Privileges
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON DATABASE airflow    TO airflow;
GRANT ALL PRIVILEGES ON DATABASE football_db TO football;

-- =============================================================================
-- football_db setup
-- =============================================================================
\connect football_db

-- -----------------------------------------------------------------------------
-- Schemas
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw          AUTHORIZATION football;
CREATE SCHEMA IF NOT EXISTS staging      AUTHORIZATION football;
CREATE SCHEMA IF NOT EXISTS intermediate AUTHORIZATION football;
CREATE SCHEMA IF NOT EXISTS marts        AUTHORIZATION football;

-- Grant usage on all schemas to football user
GRANT USAGE ON SCHEMA raw, staging, intermediate, marts TO football;

-- Default privileges: any objects football creates are accessible to itself
ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA raw          GRANT ALL ON TABLES    TO football;
ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA staging      GRANT ALL ON TABLES    TO football;
ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA intermediate GRANT ALL ON TABLES    TO football;
ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA marts        GRANT ALL ON TABLES    TO football;

ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA raw          GRANT ALL ON SEQUENCES TO football;
ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA staging      GRANT ALL ON SEQUENCES TO football;
ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA intermediate GRANT ALL ON SEQUENCES TO football;
ALTER DEFAULT PRIVILEGES FOR ROLE football IN SCHEMA marts        GRANT ALL ON SEQUENCES TO football;

-- =============================================================================
-- Raw landing tables
-- =============================================================================
SET ROLE football;

-- -----------------------------------------------------------------------------
-- raw.competitions
-- -----------------------------------------------------------------------------
CREATE TABLE raw.competitions (
    id           INT PRIMARY KEY,
    name         TEXT,
    code         TEXT,
    type         TEXT,
    area_name    TEXT,
    plan         TEXT,
    raw_payload  JSONB,
    ingested_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- raw.teams
-- -----------------------------------------------------------------------------
CREATE TABLE raw.teams (
    id             INT PRIMARY KEY,
    name           TEXT,
    short_name     TEXT,
    tla            TEXT,
    competition_id INT,
    area_name      TEXT,
    raw_payload    JSONB,
    ingested_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- raw.matches
-- -----------------------------------------------------------------------------
CREATE TABLE raw.matches (
    id                      INT PRIMARY KEY,
    competition_id          INT,
    season_id               INT,
    utc_date                TIMESTAMP WITH TIME ZONE,
    status                  TEXT,
    matchday                INT,
    home_team_id            INT,
    home_team_name          TEXT,
    away_team_id            INT,
    away_team_name          TEXT,
    home_score_full_time    INT,
    away_score_full_time    INT,
    home_score_half_time    INT,
    away_score_half_time    INT,
    winner                  TEXT,
    raw_payload             JSONB,
    ingested_at             TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- raw.standings
-- -----------------------------------------------------------------------------
CREATE TABLE raw.standings (
    id               SERIAL PRIMARY KEY,
    competition_id   INT,
    season_id        INT,
    team_id          INT,
    team_name        TEXT,
    position         INT,
    played_games     INT,
    won              INT,
    draw             INT,
    lost             INT,
    points           INT,
    goals_for        INT,
    goals_against    INT,
    goal_difference  INT,
    raw_payload      JSONB,
    ingested_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (competition_id, season_id, team_id)
);

-- -----------------------------------------------------------------------------
-- raw.ingestion_log
-- -----------------------------------------------------------------------------
CREATE TABLE raw.ingestion_log (
    id                SERIAL PRIMARY KEY,
    dag_id            TEXT,
    task_id           TEXT,
    entity_type       TEXT,
    records_ingested  INT,
    status            TEXT,
    error_message     TEXT,
    started_at        TIMESTAMP WITH TIME ZONE,
    finished_at       TIMESTAMP WITH TIME ZONE
);
