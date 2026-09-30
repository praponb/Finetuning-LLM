select i.store_id, i.quantity, ss.units
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.product_id in (2, 3, 7)
