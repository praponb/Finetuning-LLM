with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        cast(rating as int) as rating_int
    from suppliers
    where city is not null
)
-- Aggregate with conditional metrics
select
    city,
    case when rating >= 4.2 then 'HIGH' else 'LOW' end as rating_tier,
    approx_percentile(rating_int, array(0.25, 0.5, 0.75)) as rating_int_quartiles,
    count(*) as cnt,
    max(rating_int) filter (where city = 'Singapore') as singapore_max_rating_int,
    grouping(city) as grouping_id
from cleaned
group by rollup (city, case when rating >= 4.2 then 'HIGH' else 'LOW' end);
