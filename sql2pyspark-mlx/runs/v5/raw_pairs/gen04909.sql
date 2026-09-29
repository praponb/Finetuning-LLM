select
    id,
    region,
    sqft,
    opened_date
from stores
where region is not null and store_name is not null
order by sqft desc;
