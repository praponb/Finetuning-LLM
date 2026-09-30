select
    id,
    store_name,
    city,
    opened_date,
    sqft
from (
    select *, row_number() over (partition by region order by opened_date, id) as latest_rank
    from stores
) x
where latest_rank = 1;
