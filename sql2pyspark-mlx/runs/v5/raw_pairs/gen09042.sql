select ss.id, ss.units, ss.sale_date
from store_sales ss
where ss.product_id not in (select id from products where cost > 540)
