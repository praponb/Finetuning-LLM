select
    o.id,
    o.store_id,
    s.region,
    s.city,
    c.segment
from orders o
inner join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
where s.city <> 'Singapore'
order by segment desc, id
limit 50;
