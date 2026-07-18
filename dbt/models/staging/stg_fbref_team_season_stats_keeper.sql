with source as (
    select * from {{ source('bronze', 'fbref_team_season_stats_keeper') }}
),

renamed as (
    select
        -- composite primary key: one row per team per season
        regexp_extract(url, '/en/squads/([a-f0-9]+)/', 1) as team_id,
        season,

        league,
        team,

        -- playing time
        players_used::integer               as keepers_used,
        playing_time_mp::integer            as matches_played,
        playing_time_starts::integer        as starts,
        playing_time_min::integer           as minutes,
        playing_time_90s::integer           as minutes_90s,

        -- goalkeeping performance
        performance_ga::integer             as goals_against,
        performance_ga90::float             as goals_against_per_90,
        performance_sota::integer           as shots_on_target_against,
        performance_saves::integer          as saves,
        performance_save::float             as save_pct,
        performance_w::integer              as wins,
        performance_d::integer              as draws,
        performance_l::integer              as losses,
        performance_cs::integer             as clean_sheets,
        performance_cs_1::float             as clean_sheet_pct,

        -- penalties
        penalty_kicks_pkatt::integer        as penalties_faced,
        penalty_kicks_pka::integer          as penalties_allowed,
        penalty_kicks_pksv::integer         as penalties_saved,
        penalty_kicks_pkm::integer          as penalties_missed,
        penalty_kicks_save::float           as penalty_save_pct

    from source
)

select * from renamed
