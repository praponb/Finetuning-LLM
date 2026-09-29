select
    o.id,
    o.status,
    o.store_id,
    o.customer_id
from orders o
where not exists (select 1 from order_items oi where oi.order_id = o.id and oi.unit_price >= 180)
