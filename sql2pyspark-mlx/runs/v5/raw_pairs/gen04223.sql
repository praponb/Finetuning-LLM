with ranked as (
    select *, row_number() over (partition by product_id order by quantity desc, id) row_num
    from inventory
)
select
    id,
    product_id,
    quantity,
    store_id
from ranked
where row_num <= 3
order by quantity
