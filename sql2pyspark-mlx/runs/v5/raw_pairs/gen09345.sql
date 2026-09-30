select
    id,
    product_id,
    quantity,
    unit_price,
    corr(quantity, unit_price) over (partition by order_id) corr_quantity_unit_price,
    lag(quantity, 2) over (partition by product_id order by unit_price, id) prev_quantity
from order_items
where product_id is not null;
