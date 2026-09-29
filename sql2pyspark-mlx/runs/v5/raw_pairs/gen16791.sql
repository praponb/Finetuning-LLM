select
    ss.product_id,
    ss.store_id,
    ss.units,
    p.cost,
    p.supplier_id
from store_sales ss
inner join products p on p.id = ss.product_id
where p.cost > 210
order by store_id
