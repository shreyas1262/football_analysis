with source as (
    select
        *
    from {{ source('bronze', 'fbref_seasons') }}
), renamed as (
    select
        league,
        season
    from source
)

select * from renamed