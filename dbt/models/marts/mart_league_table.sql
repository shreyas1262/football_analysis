SELECT
    s.competition_id,
    s.season_id,
    c.competition_name,
    c.competition_code,
    s.position,
    s.team_name,
    s.played_games,
    s.won,
    s.draw,
    s.lost,
    s.goals_for,
    s.goals_against,
    s.goal_difference,
    s.points,
    s.points_per_game,
    ROUND(s.won::numeric / NULLIF(s.played_games, 0) * 100, 1)
        AS win_percentage,
    ROUND(s.goals_for::numeric / NULLIF(s.played_games, 0), 2)
        AS goals_per_game,
    ROUND(s.goals_against::numeric / NULLIF(s.played_games, 0), 2)
        AS conceded_per_game
FROM {{ ref('stg_standings') }} s
LEFT JOIN {{ ref('stg_competitions') }} c
    ON s.competition_id = c.competition_id
ORDER BY c.competition_code, s.position
