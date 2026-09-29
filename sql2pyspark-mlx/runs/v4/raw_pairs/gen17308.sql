select
    s.order_id,
    s.shipped_date,
    o.order_date,
    o.customer_id,
    o.amount
from shipments s
join orders o on s.order_id = o.id
