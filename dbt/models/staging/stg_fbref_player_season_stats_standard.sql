with source as (
    select
        *
    from {{ source('bronze', 'fbref_player_season_stats_standard') }}
), renamed as (
    select
        -- surrogate player ID - stable across seasons
        {{ dbt_utils.generate_surrogate_key(["player", "born", "nation"])}} as player_id,

        league,
        season,
        team,
        player        as name,
        nation,
        pos           as position,
        age::integer  as age,
        born::integer as born,

        -- playing_time
        playing_time_mp::integer     as matches_played,
        playing_time_starts::integer as matches_started,
        playing_time_mins::integer   as minutes_played,
        playing_time_90s::integer    as minutes_played_per_90,

        -- goals and assists
        performance_gls::integer     as goals,
        performance_ast::integer     as assists,
        performance_g_pk::integer    as non_penalty_goals,
        performance_pk::integer      as penalty_goals,
        performance_pkatt::integer   as penalty_attempts,
        performance_crdy::integer    as yellow_cards,
        performance_crdr::integer    as red_cards

        -- per_90_minutes
        per_90_minutes_gls           as goals_per_90,
        per_90_minutes_ast           as assists_per_90,
        per_90_minutes_g_a           as goals_and_assists_per_90,
        per_90_minutes_g_pk          as non_penalty_goals_per_90,
        per_90_minutes_g_a_pk        as non_penalty_goals_and_assists_per_90

    from source
)

select * from renamed