with valid as (
    select
        *,
        upper(supplier_name) as supplier_name_upper,
        coalesce(city, 'UNKNOWN') as resolved_city
    from suppliers
    where city is not null
)
select
    case when rating >= 4.1 then 'HIGH' else 'LOW' end as rating_tier,
    count(distinct id) as distinct_ids,
    sum(case when country = 'US' then rating else 0 end) as us_rating,
    round(sum(rating) / nullif(count(case when city = 'Berlin' then 1 end), 0), 2) as rating_per_berlin
from valid
group by case when rating >= 4.1 then 'HIGH' else 'LOW' end
order by rating_tier desc;
