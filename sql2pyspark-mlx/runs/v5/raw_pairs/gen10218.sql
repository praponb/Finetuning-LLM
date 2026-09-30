select
    ss.sale_date,
    ss.product_id,
    s.store_name,
    s.opened_date,
    p.cost
from store_sales ss
inner join stores s on ss.store_id = s.id
right join products p on ss.product_id = p.id
where p.supplier_id is null and p.supplier_id in (2, 22, 27);
