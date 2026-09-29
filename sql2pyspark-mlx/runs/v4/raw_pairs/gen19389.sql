select
    o.store_id,
    s.store_name,
    s.opened_date,
    c.name,
    c.country
from orders o
left outer join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
