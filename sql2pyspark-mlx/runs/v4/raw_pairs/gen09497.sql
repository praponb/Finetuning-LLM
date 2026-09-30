with valid as (
    -- Keep recent, valid rows
    select
        *,
        substring(country, 1, 3) as country_prefix,
        ceil(rating) as rating_whole,
        coalesce(city, 'UNKNOWN') as city_clean
    from suppliers
    where supplier_name like '%Tokyo%'
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from valid
    qualify row_number() over (partition by country order by rating_whole, id) = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        avg(rating_whole) over (partition by city, country order by rating, id rows between 3 preceding and current row) as rating_whole_avg_3,
        avg(rating) over (partition by country order by rating_whole, id rows between 3 preceding and current row) as rating_avg_3,
        avg(rating_whole) over (partition by city order by rating, id rows between 5 preceding and current row) as rating_whole_avg_5
    from ranked
)
select
    id,
    country,
    rating_whole_avg_3,
    rating_whole_avg_5
from scored
