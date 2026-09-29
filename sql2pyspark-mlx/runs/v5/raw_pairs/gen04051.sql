select
    p.paid_at,
    p.amount,
    o.customer_id,
    c.segment,
    c.country
from payments p
right join orders o on p.order_id = o.id
inner join customers c on c.id = o.customer_id
order by p.paid_at desc
