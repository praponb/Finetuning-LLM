select
    s.id,
    s.order_id,
    s.cost,
    o.customer_id,
    o.order_date,
    o.discount
from shipments s
right join orders o on s.order_id = o.id
where o.amount < 940 or o.store_id is null
