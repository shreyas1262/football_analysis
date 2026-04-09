SELECT
    m.match_id,
    m.match_date,
    m.matchday,
    m.season_id,
    c.competition_id,
    c.competition_name,
    c.competition_code,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_goals,
    m.away_goals,
    m.home_goals_ht,
    m.away_goals_ht,
    m.home_goals + m.away_goals                    AS total_goals,
    m.home_goals - m.away_goals                    AS goal_diff,
    m.home_goals_ht + m.away_goals_ht              AS total_goals_ht,
    m.result,
    CASE
        WHEN m.home_goals_ht > m.away_goals_ht THEN 'home'
        WHEN m.away_goals_ht > m.home_goals_ht THEN 'away'
        WHEN m.home_goals_ht = m.away_goals_ht THEN 'draw'
        ELSE NULL
    END                                            AS ht_leader,
    CASE
        WHEN (m.home_goals + m.away_goals) >= 4 THEN TRUE
        ELSE FALSE
    END                                            AS is_high_scoring,
    CASE
        WHEN m.home_goals_ht > m.away_goals_ht
             AND m.result != 'home' THEN TRUE
        WHEN m.away_goals_ht > m.home_goals_ht
             AND m.result != 'away' THEN TRUE
        ELSE FALSE
    END                                            AS is_ht_lead_dropped
FROM {{ ref('stg_matches') }} m
LEFT JOIN {{ ref('stg_competitions') }} c
    ON m.competition_id = c.competition_id
