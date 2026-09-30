select
    o.store_id,
    o.channel,
    oi.quantity,
    oi.unit_price,
    oi.order_id
from orders o
left outer join order_items oi on oi.order_id = o.id
where oi.product_id in (22, 27, 30, 37);
