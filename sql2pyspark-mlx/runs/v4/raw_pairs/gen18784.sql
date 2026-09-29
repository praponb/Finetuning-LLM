with ranked as (
    select
        id,
        city,
        sqft,
        store_name,
        row_number() over (partition by city order by sqft desc, id) as row_num
    from stores
)
select
    id,
    city,
    sqft,
    store_name
from ranked
where row_num = 1
order by city desc;
