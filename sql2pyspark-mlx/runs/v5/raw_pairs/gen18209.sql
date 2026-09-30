select s.city, count(*) as row_count, avg(i.quantity) as avg_quantity
from inventory i
left join products p on i.product_id = p.id
left join stores s on i.store_id = s.id
group by s.city
