select
    id,
    order_id,
    quantity,
    product_id,
    unit_price,
    first_value(product_id) over (partition by order_id order by quantity desc, id) as first_product_id,
    rank() over (partition by order_id order by unit_price) as unit_price_rank
from order_items;
