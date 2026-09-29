select
    oi.order_id,
    oi.quantity,
    oi.product_id,
    o.order_date,
    o.channel,
    o.status
from order_items oi
join orders o on o.id = oi.order_id
where oi.product_id in (16, 35, 37);
