SELECT
    id              AS competition_id,
    name            AS competition_name,
    code            AS competition_code,
    type            AS competition_type,
    area_name,
    plan,
    ingested_at
FROM {{ source('raw', 'competitions') }}
