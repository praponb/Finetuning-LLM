select
    id,
    city,
    opened_date,
    sqft,
    lag(sqft, 2) over (partition by city order by opened_date, id) as prev_sqft
from stores
where region is null
