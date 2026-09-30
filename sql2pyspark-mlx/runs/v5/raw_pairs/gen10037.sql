with ranked_inventory as (
    select *, row_number() over (partition by product_id order by quantity, id) as row_num
    from inventory
)
select
    id,
    product_id,
    quantity,
    store_id
from ranked_inventory
where row_num <= 3
order by id desc;
