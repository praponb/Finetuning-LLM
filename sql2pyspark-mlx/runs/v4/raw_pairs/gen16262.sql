select
    id,
    city,
    sqft,
    region,
    last_value(region) over (partition by city order by sqft desc, id rows between unbounded preceding and unbounded following) as last_region
from stores
where store_name not like 'S%'
