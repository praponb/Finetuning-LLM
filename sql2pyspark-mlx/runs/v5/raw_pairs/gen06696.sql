select
    o.status,
    o.shipping_fee,
    o.amount,
    c.name,
    c.segment,
    c.credit_limit
from orders o
inner join customers c on o.customer_id = c.id
where c.city = 'London'
order by shipping_fee desc
