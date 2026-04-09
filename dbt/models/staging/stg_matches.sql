SELECT
    id                                      AS match_id,
    competition_id,
    season_id,
    utc_date::date                          AS match_date,
    utc_date                                AS match_datetime,
    status,
    matchday,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score_full_time                    AS home_goals,
    away_score_full_time                    AS away_goals,
    home_score_half_time                    AS home_goals_ht,
    away_score_half_time                    AS away_goals_ht,
    CASE
        WHEN home_score_full_time > away_score_full_time THEN 'home'
        WHEN away_score_full_time > home_score_full_time THEN 'away'
        WHEN home_score_full_time = away_score_full_time
             AND home_score_full_time IS NOT NULL THEN 'draw'
        ELSE NULL
    END                                     AS result,
    winner,
    ingested_at
FROM {{ source('raw', 'matches') }}
WHERE status = 'FINISHED'
