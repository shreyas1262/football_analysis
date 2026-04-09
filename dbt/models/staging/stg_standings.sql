SELECT
    competition_id,
    season_id,
    team_id,
    team_name,
    position,
    played_games,
    won,
    draw,
    lost,
    points,
    goals_for,
    goals_against,
    goal_difference,
    ROUND(points::numeric / NULLIF(played_games, 0), 2) AS points_per_game,
    ingested_at
FROM {{ source('raw', 'standings') }}
