select p.id, p.brand, p.product_name
from products p
where exists (select 1 from store_sales ss where ss.product_id = p.id and ss.revenue between 1500 and 2300)
