with source as (
    select * from {{ source('bronze', 'fbref_team_match_stats_schedule') }}
),
renamed as (
    select
        -- per-row primary key: one row per team per match
        {{ dbt_utils.generate_surrogate_key(['match_report', 'team']) }} as team_match_id,
        -- match key: extract FBref's hash from the match URL (matches schedule.game_id)
        regexp_extract(match_report, '/en/matches/([a-f0-9]+)/', 1) as match_id,

        -- identity
        league,
        season,
        team,
        opponent,

        -- match context
        split_part(game, ' ', 1)::date  as match_date,   -- see note below
        venue,                                            -- Home / Away
        result,                                           -- W / D / L
        gf::integer                     as goals_for,
        ga::integer                     as goals_against,
        poss::integer                   as possession_pct,

        -- tactical (the unique value of this table)
        formation,
        opp_formation                   as opponent_formation,
        captain,

        -- meta
        attendance::integer             as attendance,
        referee
    from source
    where league != 'nan'  -- league matches only; cup data is incomplete
)

select * from renamed
