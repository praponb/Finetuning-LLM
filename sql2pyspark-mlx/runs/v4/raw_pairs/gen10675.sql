select
    ss.revenue,
    ss.store_id,
    ss.units,
    p.brand,
    p.product_name
from store_sales ss
inner join products p on p.id = ss.product_id
where p.supplier_id in (9, 11) or ss.store_id = 2;
