select
    oi.id,
    oi.quantity,
    p.category,
    p.supplier_id,
    s.country
from order_items oi
inner join products p on oi.product_id = p.id
full outer join suppliers s on p.supplier_id = s.id
