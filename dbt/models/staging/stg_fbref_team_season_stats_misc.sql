with source as (
    select * from {{ source('bronze', 'fbref_team_season_stats_misc') }}
),

renamed as (
    select
        -- composite primary key: one row per team per season
        regexp_extract(url, '/en/squads/([a-f0-9]+)/', 1) as team_id,
        season,

        league,
        team,

        -- playing time
        players_used::integer       as players_used,
        n_90s::integer              as minutes_90s,

        -- misc team stats
        performance_crdy::integer   as yellow_cards,
        performance_crdr::integer   as red_cards,
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
