select
    id,
    city,
    sqft,
    store_name
from (
    select *, row_number() over (partition by city order by sqft desc, id) as rn
    from stores
    where city = 'New York'
) as t
where rn <= 2
order by 4, sqft
