select
    oi.quantity,
    o.amount,
    o.store_id,
    o.discount,
    oi.unit_price + oi.quantity as line_total
from order_items oi
full outer join orders o on oi.order_id = o.id
where oi.product_id in (6, 9, 14, 37)
