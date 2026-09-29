select
    o.id,
    o.amount,
    o.shipping_fee,
    o.store_id,
    c.age,
    round(o.amount, 2) as amount_rounded
from orders o
inner join customers c on o.customer_id = c.id
where o.status = 'CANCELLED';
