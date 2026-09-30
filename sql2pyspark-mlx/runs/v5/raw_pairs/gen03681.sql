select
    oi.id,
    oi.product_id,
    oi.order_id,
    o.status,
    o.discount,
    p.price
from order_items oi
left outer join orders o on oi.order_id = o.id
right outer join products p on oi.product_id = p.id
where o.amount < 570 and o.customer_id is not null
