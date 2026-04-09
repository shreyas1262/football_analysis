WITH ht_winning AS (
    SELECT
        competition_id,
        competition_code,
        competition_name,
        season_id,
        home_team_id    AS team_id,
        home_team_name  AS team_name,
        result,
        ht_leader,
        is_ht_lead_dropped
    FROM {{ ref('int_match_results') }}
    WHERE ht_leader = 'home'

    UNION ALL

    SELECT
        competition_id,
        competition_code,
        competition_name,
        season_id,
        away_team_id    AS team_id,
        away_team_name  AS team_name,
        result,
        ht_leader,
        is_ht_lead_dropped
    FROM {{ ref('int_match_results') }}
    WHERE ht_leader = 'away'
),

aggregated AS (
    SELECT
        competition_id,
        competition_code,
        competition_name,
        season_id,
        team_id,
        team_name,
        COUNT(*)                                             AS matches_leading_ht,
        SUM(CASE WHEN is_ht_lead_dropped THEN 1 ELSE 0 END) AS leads_dropped,
        ROUND(
            SUM(CASE WHEN is_ht_lead_dropped THEN 1 ELSE 0 END)::numeric
            / NULLIF(COUNT(*), 0) * 100, 1
        )                                                    AS drop_rate_pct
    FROM ht_winning
    GROUP BY
        competition_id, competition_code, competition_name,
        season_id, team_id, team_name
)

SELECT *
FROM aggregated
WHERE matches_leading_ht >= 3
ORDER BY competition_code, drop_rate_pct DESC
