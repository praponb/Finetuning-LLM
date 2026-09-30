select
    id,
    city,
    sqft,
    region
from (
    select *, row_number() over (partition by city order by sqft desc, id) row_num
    from stores
    where month(opened_date) in (5, 9, 11)
) ranked
where row_num <= 2;
