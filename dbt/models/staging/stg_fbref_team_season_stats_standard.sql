with source as (
    select * from {{ source('bronze', 'fbref_team_season_stats_standard') }}
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
        age::float                  as avg_age,
        poss::float                 as possession_pct,

        -- playing time
        playing_time_mp::integer    as matches_played,
        playing_time_starts::integer as starts,
        playing_time_min::integer   as minutes,
        playing_time_90s::integer   as minutes_90s,

        -- goals and assists
        performance_gls::integer    as goals,
        performance_ast::integer    as assists,
        performance_g_pk::integer   as non_penalty_goals,
        performance_pk::integer     as penalty_goals,
        performance_pkatt::integer  as penalties_attempted,
        performance_crdy::integer   as yellow_cards,
        performance_crdr::integer   as red_cards,

        -- per 90 minutes
        per_90_minutes_gls::float     as goals_per_90,
        per_90_minutes_ast::float     as assists_per_90,
        per_90_minutes_g_a::float     as goals_and_assists_per_90,
        per_90_minutes_g_pk::float    as non_penalty_goals_per_90,
        per_90_minutes_g_a_pk::float  as non_penalty_goals_and_assists_per_90

    from source
)

select * from renamed
