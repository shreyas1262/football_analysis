with source as (
    select
        *
    from {{ source('bronze', 'fbref_player_season_stats_shooting') }}
), renamed as (
    select
        -- surrogate player ID - stable across seasons
        {{ dbt_utils.generate_surrogate_key(["player", "born", "nation"])}} as player_id,

        league,
        season,
        team,
        player as player_name,
        nation,
        pos as position,
        age::integer as age,
        born::integer as born,
        n_90s::double as no_of_90_min_periods_played,
        standard_gls::integer as goals,
        standard_sh::integer as shots,
        standard_sot::integer as shots_on_target,
        standard_sot_1::float as shots_on_target_pct,
        standard_sh_90::float as shots_per_90_min,
        standard_sot_90::float as shots_on_target_per_90_min,
        standard_g_sh::float as goals_per_shot,
        standard_g_sot::float as goals_per_shot_on_target
    from source
)

select * from renamed