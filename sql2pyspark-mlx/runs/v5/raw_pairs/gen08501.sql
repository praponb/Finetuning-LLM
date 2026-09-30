select
    ss.id,
    ss.sale_date,
    ss.store_id,
    p.price,
    p.category,
    p.cost
from store_sales ss
inner join products p on p.id = ss.product_id
where ss.units <> 33 and p.launch_date > '2025-06-01'
order by sale_date asc, ss.store_id desc
