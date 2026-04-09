SELECT
    id              AS team_id,
    name            AS team_name,
    short_name      AS team_short_name,
    tla             AS team_tla,
    competition_id,
    area_name,
    ingested_at
FROM {{ source('raw', 'teams') }}
