with source as (
    select * from {{ source('bronze', 'fbref_team_match_stats_keeper') }}
),

renamed as (
    select
        -- per-row primary key: one row per team per match
        {{ dbt_utils.generate_surrogate_key(['match_report', 'team']) }} as team_match_id,
        -- match key: extract FBref's hash from the match URL (matches schedule.game_id)
        regexp_extract(match_report, '/en/matches/([a-f0-9]+)/', 1) as match_id,

        league,
        season,
        team,
        opponent,
        -- date from the game string, not the tz-aware `date` column (avoids off-by-one)
        split_part(game, ' ', 1)::date      as match_date,
        venue,
        result,

        -- keeper stats
        performance_sota::integer           as shots_on_target_against,
        performance_ga::integer             as goals_against,
        performance_saves::integer          as saves,
        performance_cs::integer             as clean_sheets,
        penalty_kicks_pkatt::integer        as penalties_faced,
        penalty_kicks_pka::integer          as penalties_scored_against,
        penalty_kicks_pksv::integer         as penalties_saved

    from source
    -- league matches only: gate on schedule membership, NOT the `league` column
    -- (bronze `league` is unreliably 'nan' even for real league games).
    where regexp_extract(match_report, '/en/matches/([a-f0-9]+)/', 1)
          in (select match_id from {{ ref('stg_fbref_schedule') }})
)

select * from renamed
