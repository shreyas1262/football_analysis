with source as (
    select
        *
    from {{ source('bronze', 'fbref_player_season_stats_keeper') }}
), renamed as (
    select
        -- surrogate player ID - stable across seasons
        {{ dbt_utils.generate_surrogate_key(["player", "born", "nation"])}} as player_id,

        league,
        season,
        team,
        player             as name,
        nation,
        pos                as position,
        age::integer       as age,
        born::integer      as born,

        performance_ga::integer as goals_against,
        performance_ga90::double as goals_against_per_90,
        performance_sota::integer as shots_on_target_against,
        performance_saves::integer as saves,
        performance_save::double as save_pct,
        performance_w::integer as wins,
        performance_d::integer as draws,
        performance_l::integer as losses,
        performance_cs::integer as clean_sheets,
        performance_cs_1::double as clean_sheet_pct,
        penalty_kicks_pkatt::integer as penalties_faced,
        penalty_kicks_pka::integer as penalties_allowed,
        penalty_kicks_pksv::integer as penalties_saved,
        penalty_kicks_pkm::integer as penalties_missed,
        penalty_kicks_save::double as penalty_save_pct
    from source
)

select * from renamed
