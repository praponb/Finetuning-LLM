select
    o.id,
    o.order_date,
    o.status,
    c.city,
    c.email,
    c.segment
from orders o
left join customers c on c.id = o.customer_id;
