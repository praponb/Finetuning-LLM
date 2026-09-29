select ss.product_id, p.launch_date, s.city
from store_sales ss
inner join products p on ss.product_id = p.id
left outer join stores s on s.id = ss.store_id
where ss.store_id in (4, 5, 6)
