with cleaned as (
    select
        id,
        supplier_name,
        country,
        rating,
        length(city) as city_length
    from suppliers
    where city is not distinct from 'New York'
)
select
    case when city_length >= 70 then 'HIGH' else 'LOW' end as city_length_tier,
    case when rating >= 4.0 then 'LARGE' else 'SMALL' end as rating_tier,
    round(avg(city_length), 2) as avg_city_length,
    count(*) as num_rows,
    avg(city_length) as avg_city_length_2,
    count(distinct country) as distinct_countries
from cleaned
group by 1, 2
order by avg_city_length_2 desc
