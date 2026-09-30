with ranked as (
    select *, row_number() over (partition by city order by sqft, id) as pos
    from stores
)
select
    id,
    city,
    sqft,
    region
from ranked
where pos <= 2
order by region
