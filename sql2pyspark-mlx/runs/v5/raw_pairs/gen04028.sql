select
    p.supplier_id,
    p.product_name,
    s.city,
    s.supplier_name
from products p
full outer join suppliers s on s.id = p.supplier_id;
