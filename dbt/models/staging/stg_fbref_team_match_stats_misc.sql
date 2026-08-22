with source as (
    select * from {{ source('bronze', 'fbref_team_match_stats_misc') }}
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
        -- date from the game string, not the tz-aware `date` column (avoids off-by-one)
        split_part(game, ' ', 1)::date      as match_date,
        venue,
        result,

        -- misc team stats
        gf::integer                 as goals_for,
        ga::integer                 as goals_against,
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
    -- league matches only: gate on schedule membership, NOT the `league` column
    -- (bronze `league` is unreliably 'nan' even for real league games).
    where regexp_extract(match_report, '/en/matches/([a-f0-9]+)/', 1)
          in (select match_id from {{ ref('stg_fbref_schedule') }})
)

select * from renamed
