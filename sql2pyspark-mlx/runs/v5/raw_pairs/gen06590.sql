select
    c.age,
    o.status,
    o.amount,
    o.channel
from customers c
left join orders o on o.customer_id = c.id
where o.store_id = 37 and o.discount is not null
