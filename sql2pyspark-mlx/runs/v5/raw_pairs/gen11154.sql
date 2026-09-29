select
    o.id,
    o.status,
    o.discount,
    o.amount,
    c.city
from orders o
inner join customers c on o.customer_id = c.id
where c.is_active = true
order by status;
