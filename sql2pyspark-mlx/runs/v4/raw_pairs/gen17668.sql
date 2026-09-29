with Numbered as (
    select *, row_number() over (partition by city order by sqft, id) as row_num
    from stores
)
select
    id,
    city,
    sqft,
    opened_date
from Numbered
where row_num = 1
order by opened_date
