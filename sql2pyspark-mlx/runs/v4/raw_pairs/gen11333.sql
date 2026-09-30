select
    id,
    order_id,
    unit_price,
    dense_rank() over (partition by order_id order by unit_price desc) as unit_price_dense_rank
from order_items
where product_id is null
