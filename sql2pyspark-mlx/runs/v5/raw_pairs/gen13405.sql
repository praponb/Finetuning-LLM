select ss.id, ss.product_id, ss.sale_date
from store_sales ss
where ss.sale_date > '2024-07-01' and ss.store_id in (select id from stores where sqft > 13441);
