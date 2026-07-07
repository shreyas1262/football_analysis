with source as (
    select
        *
    from {{ source('bronze', 'fbref_player_season_stats_playing_time') }}
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

        playing_time_mp::integer as matches_played,
        playing_time_min::integer as minutes_played,
        playing_time_mn_mp::double as minutes_per_match,
        playing_time_min_1::double as minutes_played_pct,
        starts_starts::integer as matches_started,
        starts_mn_start::integer as minutes_played_when_started,
        starts_compl::integer as starts_completed,
        subs_subs::integer as matches_subbed_in,
        subs_mn_sub::integer as minutes_played_when_subbed_in,
        subs_unsub::integer as unused_substitute,
        team_success_ppm::double as team_points_per_match_when_playing,
        team_success_ong::double as team_goals_scored_when_on_pitch,
        team_success_onga::double as team_goals_conceded_when_on_pitch,
        team_success::double as team_goal_difference_when_on_pitch,
        team_success_90s::double as team_goal_difference_per_90_when_on_pitch,
        team_success_on_off::double as team_goal_difference_when_on_pitch_minus_off_pitch
    from source       
)

select * from renamed