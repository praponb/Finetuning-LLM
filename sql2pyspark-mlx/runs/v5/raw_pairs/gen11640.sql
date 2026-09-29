select
    oi.id,
    oi.product_id,
    oi.order_id,
    o.status,
    o.channel
from order_items oi
inner join orders o on oi.order_id = o.id
