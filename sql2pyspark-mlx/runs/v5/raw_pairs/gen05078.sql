with valid as (
    select
        *,
        upper(trim(supplier_name)) as supplier_name_normalized,
        coalesce(city, 'NONE') as resolved_city
    from suppliers
    where country in ('US', 'SG')
)
select
    case when rating >= 4.1 then 'PREMIUM' else 'STANDARD' end as rating_tier,
    size(collect_set(country)) as num_countries,
    count(*) filter (where city = 'Bangkok') as bangkok_rows,
    sum(case when country = 'DE' then rating else 0 end) as de_rating
from valid
group by rating_tier
having num_countries > 1;
