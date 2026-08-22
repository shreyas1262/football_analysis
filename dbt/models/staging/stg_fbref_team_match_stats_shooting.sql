with source as (
    select * from {{ source('bronze', 'fbref_team_match_stats_shooting') }}
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

        -- shooting stats
        standard_gls::integer               as goals,
        standard_sh::integer                as shots,
        standard_sot::integer               as shots_on_target,
        standard_sot_1::float               as shots_on_target_pct,
        standard_g_sh::float                as goals_per_shot,
        standard_g_sot::float               as goals_per_shot_on_target,
        standard_pk::integer                as penalty_goals,
        standard_pkatt::integer             as penalties_attempted

    from source
    -- league matches only: gate on schedule membership, NOT the `league` column
    -- (bronze `league` is unreliably 'nan' even for real league games).
    where regexp_extract(match_report, '/en/matches/([a-f0-9]+)/', 1)
          in (select match_id from {{ ref('stg_fbref_schedule') }})
)

select * from renamed
