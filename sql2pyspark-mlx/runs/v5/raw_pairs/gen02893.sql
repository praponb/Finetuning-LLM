select
    oi.id,
    oi.quantity,
    oi.order_id,
    p.price,
    p.launch_date,
    s.country,
    s.city
from order_items oi
inner join products p on oi.product_id = p.id
inner join suppliers s on p.supplier_id = s.id;
