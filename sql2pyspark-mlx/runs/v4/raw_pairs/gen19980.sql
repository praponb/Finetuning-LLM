select
    ss.id,
    ss.revenue,
    ss.product_id,
    p.brand,
    p.supplier_id,
    s.store_name
from store_sales ss
inner join products p on p.id = ss.product_id
inner join stores s on s.id = ss.store_id
order by store_name;
