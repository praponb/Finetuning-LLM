select
    id,
    order_id,
    unit_price,
    quantity,
    last_value(quantity) over (partition by order_id order by unit_price, id rows between unbounded preceding and unbounded following) as last_quantity,
    min(quantity) over (partition by order_id) as order_id_min_quantity
from order_items
where product_id is not null
order by quantity desc
