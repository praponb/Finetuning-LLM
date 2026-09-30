select
    oi.id,
    oi.order_id,
    p.category,
    p.product_name,
    s.supplier_name,
    s.rating
from order_items oi
right join products p on p.id = oi.product_id
left join suppliers s on p.supplier_id = s.id
where s.rating between 1.9 and 2.3;
