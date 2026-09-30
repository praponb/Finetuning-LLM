select
    p.id,
    p.category,
    p.launch_date,
    p.supplier_id
from products p
where exists (select 1 from inventory i where i.product_id = p.id);
