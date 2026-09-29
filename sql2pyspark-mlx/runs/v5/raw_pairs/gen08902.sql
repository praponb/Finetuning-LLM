select
    s.status,
    s.order_id,
    o.status as order_status,
    o.store_id,
    c.email
from shipments s
left join orders o on s.order_id = o.id
full outer join customers c on o.customer_id = c.id;
