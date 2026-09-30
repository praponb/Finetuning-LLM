select
    id,
    region,
    opened_date,
    sqft,
    sum(sqft) over (partition by region order by opened_date, id) as running_sqft
from stores
where region = 'West'
order by sqft
