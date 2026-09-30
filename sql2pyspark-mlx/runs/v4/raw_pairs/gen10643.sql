select
    ss.id,
    ss.sale_date,
    ss.revenue,
    p.product_name,
    p.brand,
    s.city
from store_sales ss
inner join products p on ss.product_id = p.id
right join stores s on ss.store_id = s.id
where ss.sale_date between '2024-01-01' and '2024-08-17'
