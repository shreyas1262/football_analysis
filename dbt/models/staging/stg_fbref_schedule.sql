with source as (
    select
        *
    from {{ source('bronze', 'fbref_schedule') }}
), renamed as (
    select
        -- primary key: FBref's native match hash (from the match URL).
        -- team_match_stats extracts the same hash from its match_report column.
        game_id as match_id,

        league,
        season,
        -- date from the game string, not the tz-aware `date` column (avoids off-by-one)
        split_part(game, ' ', 1)::date as match_date,
        cast(week as integer) as match_week,
        home_team,
        away_team,
        score,
        -- Strip penalty-shootout notation before casting. FBref writes those as
        -- "(5) 0–3 (6)", where the parenthesised numbers are the shootout score
        -- (seen on relegation/promotion playoffs). Keep the 90/120-minute score,
        -- which is what a league table counts.
        trim(regexp_replace(split_part(score, '–', 1), '\(\d+\)', ''))::integer as home_score,
        trim(regexp_replace(split_part(score, '–', 2), '\(\d+\)', ''))::integer as away_score,
        score is not null as is_complete,
        attendance::integer as attendance,
        venue,
        referee
    from source
    where game_id != 'None'
)

select * from renamed