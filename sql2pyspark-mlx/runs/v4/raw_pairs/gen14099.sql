select
    id,
    product_id,
    unit_price,
    quantity,
    min(unit_price) over (partition by product_id) as product_id_min_unit_price,
    sum(quantity) over (partition by product_id) as product_id_sum_quantity
from order_items
