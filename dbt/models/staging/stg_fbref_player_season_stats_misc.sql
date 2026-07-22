with source as (
    select
        *
    from {{ source('bronze', 'fbref_player_season_stats_misc') }}
), renamed as (
    select
        -- surrogate player ID - stable across seasons
        {{ dbt_utils.generate_surrogate_key(["player", "born", "nation"])}} as player_id,

        league,
        season,
        team,
        player                      as name,
        nation,
        pos                         as position,
        age::integer                as age,
        born::integer               as born,

        performance_2crdy::integer  as second_yellow_card,
        performance_fls::integer    as fouls_committed,
        performance_fld::integer    as fouls_drawn,
        performance_off::integer    as offsides,
        performance_crs::integer    as crosses_attempted,
        performance_int::integer    as interceptions,
        performance_tklw::integer   as tackles_won,
        performance_pkwon::integer  as penalties_won,
        performance_pkcon::integer  as penalties_conceded,
        performance_og::integer     as own_goals
    from source
    )

select * from renamed