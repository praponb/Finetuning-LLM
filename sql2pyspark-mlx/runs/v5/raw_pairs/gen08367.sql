select
    p.id,
    p.stock_qty,
    p.cost,
    p.supplier_id
from products p
where array_contains(p.tags, 'new') and not exists (select 1 from inventory i where i.product_id = p.id and i.store_id = 1)
