select
    a.id,
    a.category,
    a.brand,
    a.cost,
    a.stock_qty,
    a.supplier_id,
    p.rating,
    p.supplier_name
from products a
left join suppliers p
    on a.supplier_id = p.id;
