select
    p.method,
    p.status,
    o.discount,
    o.customer_id,
    o.order_date
from payments p
left join orders o on o.id = p.order_id
order by p.method asc, order_date asc
