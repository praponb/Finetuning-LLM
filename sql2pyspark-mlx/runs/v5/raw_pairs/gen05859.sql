with latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        supplier_name,
        rating
    from suppliers
    qualify rank() over (partition by city order by rating) = 1
)
select
    id,
    supplier_name,
    rating
from latest
order by id desc
limit 5;
