select i.id, i.quantity, ss.revenue
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.store_id in (4, 5) and ss.product_id in (2, 3, 4, 10)
order by ss.revenue
