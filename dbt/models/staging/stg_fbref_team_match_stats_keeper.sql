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
        date::date                          as match_date,
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
)

select * from renamed
