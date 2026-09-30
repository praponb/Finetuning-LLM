select
    o.id,
    o.store_id,
    s.region,
    c.credit_limit,
    c.segment
from orders o
left outer join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
