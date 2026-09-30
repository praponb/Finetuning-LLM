select
    i.product_id,
    p.supplier_id,
    p.category,
    upper(trim(p.product_name)) as product_name_normalized
from inventory i
inner join products p on p.id = i.product_id
where i.last_restock between '2023-12-01' and '2025-07-04'
order by product_name_normalized, supplier_id;
