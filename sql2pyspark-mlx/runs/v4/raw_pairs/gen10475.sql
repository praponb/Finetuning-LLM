select
    ss.id,
    ss.revenue,
    ss.product_id,
    p.cost,
    p.price,
    s.store_name
from store_sales ss
right outer join products p on ss.product_id = p.id
inner join stores s on s.id = ss.store_id
where ss.product_id = 5
