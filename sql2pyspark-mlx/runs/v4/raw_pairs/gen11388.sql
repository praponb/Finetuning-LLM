select
    o.id,
    o.channel,
    o.status,
    c.is_active,
    s.city,
    s.sqft
from orders o
join customers c on o.customer_id = c.id
join stores s on o.store_id = s.id
where o.customer_id = 33 and s.opened_date >= '2023-08-19';
