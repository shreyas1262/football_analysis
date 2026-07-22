with source as (
    select * from {{ source('bronze', 'fbref_team_season_stats_shooting') }}
),

renamed as (
    select
        -- composite primary key: one row per team per season
        regexp_extract(url, '/en/squads/([a-f0-9]+)/', 1) as team_id,
        season,

        league,
        team,

        -- squad overview
        players_used::integer       as players_used,
        n_90s::integer              as minutes_90s,

        -- shooting stats
        standard_gls::integer       as goals,
        standard_sh::integer        as shots,
        standard_sot::integer       as shots_on_target,
        standard_sot_1::float       as shots_on_target_pct,
        standard_sh_90::float       as shots_per_90,
        standard_sot_90::float      as shots_on_target_per_90,
        standard_g_sh::float        as goals_per_shot,
        standard_g_sot::float       as goals_per_shot_on_target,
        standard_pk::integer        as penalty_goals,
        standard_pkatt::integer     as penalties_attempted

    from source
)

select * from renamed
