with source as (
    select * from {{ source('bronze', 'fbref_team_season_stats_playing_time') }}
),

renamed as (
    select
        -- composite primary key: one row per team per season
        regexp_extract(url, '/en/squads/([a-f0-9]+)/', 1) as team_id,
        season,

        league,
        team,

        -- squad overview
        players_used::integer           as players_used,
        age::float                      as avg_age,

        -- playing time
        playing_time_mp::integer        as matches_played,
        playing_time_min::integer       as minutes,
        playing_time_mn_mp::integer     as minutes_per_match,
        playing_time_min_1::integer     as minutes_pct,
        playing_time_90s::integer       as minutes_90s,

        -- starts
        starts_starts::integer          as starts,
        starts_mn_start::integer        as minutes_per_start,
        starts_compl::integer           as complete_matches,

        -- substitutions
        subs_subs::integer              as sub_appearances,
        subs_mn_sub::integer            as minutes_per_sub,
        subs_unsub::integer             as unused_subs,

        -- team success (on-pitch outcomes)
        team_success_ppm::float         as points_per_match,
        team_success_ong::integer       as goals_for,
        team_success_onga::integer      as goals_against,
        team_success::integer           as goal_difference,
        team_success_90::float          as goal_difference_per_90

    from source
)

select * from renamed
