select
    c.age,
    c.city,
    o.customer_id,
    o.discount,
    o.store_id
from customers c
left outer join orders o on o.customer_id = c.id
order by age desc;
