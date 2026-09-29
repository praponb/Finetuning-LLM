select
    id,
    opened_date,
    sqft,
    substring(store_name, 1, 4) as store_name_prefix
from stores
where store_name is not null
order by sqft desc;
