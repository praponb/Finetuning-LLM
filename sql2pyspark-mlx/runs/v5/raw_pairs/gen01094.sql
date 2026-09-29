select
    oi.id,
    oi.unit_price,
    oi.product_id,
    oi.quantity,
    o.customer_id,
    o.channel
from order_items oi
left join orders o on oi.order_id = o.id
