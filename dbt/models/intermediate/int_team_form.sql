WITH all_matches AS (
    SELECT
        match_id,
        match_date,
        competition_id,
        season_id,
        home_team_id    AS team_id,
        home_team_name  AS team_name,
        home_goals      AS goals_scored,
        away_goals      AS goals_conceded,
        result          AS team_result,
        CASE result WHEN 'home' THEN 3
                    WHEN 'draw' THEN 1
                    ELSE 0 END AS points_earned
    FROM {{ ref('stg_matches') }}

    UNION ALL

    SELECT
        match_id,
        match_date,
        competition_id,
        season_id,
        away_team_id    AS team_id,
        away_team_name  AS team_name,
        away_goals      AS goals_scored,
        home_goals      AS goals_conceded,
        CASE result
            WHEN 'away' THEN 'win'
            WHEN 'draw' THEN 'draw'
            ELSE 'loss' END AS team_result,
        CASE result WHEN 'away' THEN 3
                    WHEN 'draw' THEN 1
                    ELSE 0 END AS points_earned
    FROM {{ ref('stg_matches') }}
),

with_form AS (
    SELECT
        *,
        SUM(points_earned) OVER (
            PARTITION BY team_id, competition_id
            ORDER BY match_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS form_points_last5,
        ROW_NUMBER() OVER (
            PARTITION BY team_id, competition_id
            ORDER BY match_date DESC
        ) AS match_recency_rank
    FROM all_matches
)

SELECT * FROM with_form
