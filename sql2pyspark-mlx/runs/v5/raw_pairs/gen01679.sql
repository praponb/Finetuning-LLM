select
    s.id,
    s.order_id,
    o.channel,
    o.discount,
    c.is_active,
    c.age
from shipments s
full outer join orders o on s.order_id = o.id
inner join customers c on c.id = o.customer_id
