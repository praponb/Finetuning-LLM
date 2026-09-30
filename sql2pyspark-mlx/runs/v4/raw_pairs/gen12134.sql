with ranked_order_items as (
    select *, row_number() over (partition by order_id order by quantity desc, id) as pos
    from order_items
)
select
    id,
    order_id,
    quantity,
    unit_price
from ranked_order_items
where pos = 1
