--- Calculate the league table
with matches as (
    select * from {{ ref('stg_fbref_schedule') }}
    where is_complete = true
), home as (
    select
        league,
        season,
        home_team as team,
        sum(case
            when home_score > away_score then 3
            when home_score = away_score then 1
            else 0
        end) as points,
        count(*) as played
    from matches
    group by league, season, home_team
), away as (
    select
        league,
        season,
        away_team as team,
        sum(case
            when away_score > home_score then 3
            when away_score = home_score then 1
            else 0
        end) as points,
        count(*) as played
    from matches
    group by league, season, away_team
)

select
    league,
    season,
    team,
    sum(points) as points,
    sum(played) as played
from (
    select * from home
    union all
    select * from away
) as combined
group by league, season, team
order by league, season, points desc
