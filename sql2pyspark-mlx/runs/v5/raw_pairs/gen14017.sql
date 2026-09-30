select
    id,
    order_id,
    product_id,
    unit_price
from order_items
where product_id in (12, 29, 31) and product_id in (2, 8, 36, 40)
