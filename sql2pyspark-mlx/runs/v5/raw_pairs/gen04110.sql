select
    id,
    product_id,
    order_id,
    quantity,
    lead(quantity, 2) over (partition by product_id, order_id order by quantity, id) next_quantity
from order_items
order by next_quantity desc, product_id
