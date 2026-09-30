select
    o.order_date,
    oi.unit_price,
    oi.quantity,
    oi.product_id,
    date_sub(o.order_date, 13) order_date_minus_13d
from orders o
left join order_items oi on o.id = oi.order_id
